/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.LongLongState;
import com.facebook.presto.operator.aggregation.state.MaxOrMinByState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.operator.aggregation.state.TwoStatesMapping;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.sun.org.apache.bcel.internal.generic.MethodObserver;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.method;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodType.methodType;

public abstract class AbstractMinMaxBy
        extends SqlAggregationFunction
{
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AbstractMinMaxBy.class, "output", MethodHandle.class, MethodHandle.class, AccumulatorState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(AbstractMinMaxBy.class, "input", MethodHandle.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, AccumulatorState.class, Block.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AbstractMinMaxBy.class, "combine", MethodHandle.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, AccumulatorState.class, AccumulatorState.class);

    private final boolean min;

    protected AbstractMinMaxBy(boolean min)
    {
        super((min ? "min" : "max") + "_by",
                ImmutableList.of(orderableTypeParameter("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("V"),
                ImmutableList.of(parseTypeSignature("V"), parseTypeSignature("K")));
        this.min = min;
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        return generateAggregation(valueType, keyType, functionRegistry);
    }

    private InternalAggregationFunction generateAggregation(Type valueType, Type keyType, FunctionRegistry functionRegistry)
    {
        Class<?> stateClazz = TwoStatesMapping.getStateClass(keyType.getJavaType(), valueType.getJavaType());
        StateCompiler.StateClass stateClass = new StateCompiler.StateClass(stateClazz, Optional.of(ImmutableMap.of("FirstState", keyType, "SecondState", valueType)));
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(stateClass, classLoader);
        AccumulatorStateSerializer<?> stateSerializer = StateCompiler.generateStateSerializer(stateClass, classLoader);
        Type intermediateType = stateSerializer.getSerializedType();

        OperatorType operator = min ? LESS_THAN : GREATER_THAN;
        MethodHandle compare = functionRegistry.getScalarFunctionImplementation(internalOperator(operator.name(), BOOLEAN.getTypeSignature(), ImmutableList.of(keyType.getTypeSignature(), keyType.getTypeSignature()))).getMethodHandle();
        MethodHandle getKey = methodHandle(Type.class, "getLong", Block.class, int.class).bindTo(keyType);
        MethodHandle getValue = methodHandle(Type.class, "getLong", Block.class, int.class).bindTo(valueType);
        MethodHandle stateGetKey = methodHandle(stateClazz, "getFirstState");
        MethodHandle stateGetValue = methodHandle(stateClazz, "getSecondState");
        MethodHandle stateSetKey = methodHandle(stateClazz, "setFirstState", keyType.getJavaType());
        MethodHandle stateSetValue = methodHandle(stateClazz, "setSecondState", valueType.getJavaType());
        MethodHandle writeValue = methodHandle(Type.class, "writeLong", BlockBuilder.class, long.class).bindTo(valueType);

        List<Type> inputTypes = ImmutableList.of(valueType, keyType);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getName(), valueType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(valueType, keyType),
                INPUT_FUNCTION.bindTo(getKey).bindTo(getValue).bindTo(compare).bindTo(stateSetKey).bindTo(stateSetValue).bindTo(stateGetKey),
                COMBINE_FUNCTION.bindTo(compare).bindTo(stateGetKey).bindTo(stateGetValue).bindTo(stateSetKey).bindTo(stateSetValue),
                OUTPUT_FUNCTION.bindTo(stateGetValue).bindTo(writeValue),
                AccumulatorState.class,
                stateSerializer,
                stateFactory,
                valueType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getName(), inputTypes, intermediateType, valueType, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value, Type key)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INPUT_CHANNEL, key), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(MethodHandle getKey, MethodHandle getValue, MethodHandle compare, MethodHandle stateSetKey, MethodHandle stateSetValue, MethodHandle stateGetKey, AccumulatorState state, Block value, Block key, int position)
    {
        MethodHandle compareHandle = MethodHandles.dropArguments(compare, 1, Block.class, int.class);
        MethodHandle foldedCompare = MethodHandles.foldArguments(compareHandle, getKey);
        List<Class<?>> foldedCompareParameters = new ArrayList<>(foldedCompare.type().parameterList());
        Class<?> lastParam = foldedCompareParameters.remove(foldedCompareParameters.size() - 1);
        foldedCompareParameters.add(0, lastParam);
        foldedCompare = MethodHandles.permuteArguments(foldedCompare, methodType(foldedCompare.type().returnType(), foldedCompareParameters), 1, 2, 0);
        stateGetKey = stateGetKey.bindTo(state);
        foldedCompare = MethodHandles.foldArguments(foldedCompare, stateGetKey);
        MethodHandle setKeyHandle = MethodHandles.dropArguments(stateSetKey, 2, Block.class, int.class);
        MethodHandle foldedSetKey = MethodHandles.foldArguments(setKeyHandle.bindTo(state), getKey);
        MethodHandle setValueHandle = MethodHandles.dropArguments(stateSetValue, 2, Block.class, int.class);
        MethodHandle foldedSetValue = MethodHandles.foldArguments(setValueHandle.bindTo(state), getValue);
        try {
            if (!((LongLongState)state).isFirstStateNotNull() || (boolean)foldedCompare.invokeExact(key, position)) {
                foldedSetKey.invokeExact(key, position);
                foldedSetValue.invokeExact(value, position);
                ((LongLongState)state).setFirstStateNotNull(true);
                ((LongLongState)state).setSecondStateNotNull(true);
            }
        } catch (Throwable e) {
        }
//        if ((state.getKey() == null) || state.getKey().isNull(0) ||
//                compare(keyType.compareTo(key, position, state.getKey(), 0), min)) {
//            state.setKey(key.getSingleValueBlock(position));
//            state.setValue(value.getSingleValueBlock(position));
//        }
    }

    public static void combine(MethodHandle compare, MethodHandle getKey, MethodHandle getValue, MethodHandle setKey, MethodHandle setValue, AccumulatorState state, AccumulatorState otherState)
    {
//        MethodHandle compareHandle = MethodHandles.dropArguments(compare, 1, long.class);
        MethodHandle foldedOneStateCompare = MethodHandles.foldArguments(compare, getKey.bindTo(state));
        MethodHandle foldedCompare = MethodHandles.foldArguments(foldedOneStateCompare, getKey.bindTo(otherState));

        MethodHandle foldedSetKey = MethodHandles.foldArguments(setKey.bindTo(state), getKey.bindTo(otherState));
        MethodHandle foldedSetValue = MethodHandles.foldArguments(setValue.bindTo(state), getValue.bindTo(otherState));

        try {
            if (!((LongLongState) state).isFirstStateNotNull() || ((LongLongState) otherState).isFirstStateNotNull() && (boolean)foldedCompare.invokeExact()) {
                foldedSetKey.invokeExact();
                foldedSetValue.invokeExact();
                ((LongLongState)state).setFirstStateNotNull(((LongLongState)otherState).isFirstStateNotNull());
                ((LongLongState)state).setSecondStateNotNull(((LongLongState)otherState).isSecondStateNotNull());
            }
        } catch (Throwable e) {

        }
//        Block key = state.getKey();
//        Block otherKey = otherState.getKey();
//        if ((key == null) || ((otherKey != null) && compare(keyType.compareTo(otherKey, 0, key, 0), min))) {
//            state.setKey(otherKey);
//            state.setValue(otherState.getValue());
//        }
    }

    public static void output(MethodHandle getValue, MethodHandle writeValue, AccumulatorState state, BlockBuilder out)
    {
        if (!((LongLongState)state).isSecondStateNotNull()) {
            out.appendNull();
        }
        else {
            List<Class<?>> writeValueParameters = new ArrayList<>(writeValue.type().parameterList());
            Class<?> lastParam = writeValueParameters.remove(writeValueParameters.size() - 1);
            writeValueParameters.add(0, lastParam);
            writeValue = MethodHandles.permuteArguments(writeValue, methodType(writeValue.type().returnType(), writeValueParameters), 1, 0);
            MethodHandle foldedWriteValue = MethodHandles.foldArguments(writeValue, getValue.bindTo(state));
            try {
                foldedWriteValue.invokeExact(out);
            } catch (Throwable e) {

            }
        }
//        if (state.getValue() == null) {
//            out.appendNull();
//        }
//        else {
//            valueType.appendTo(state.getValue(), 0, out);
//        }
    }

    private static boolean compare(int value, boolean lessThan)
    {
        return lessThan ? (value < 0) : (value > 0);
    }
}
