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
package com.facebook.presto.operator;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class RemoteProjectOperator
        implements Operator
{
    private static final Logger log = Logger.get(RemoteProjectOperator.class);

    private final OperatorContext operatorContext;
    private final FunctionManager functionManager;
    private final List<RowExpression> projections;

    private final CompletableFuture<Block>[] result;

    private boolean finishing;

    private RemoteProjectOperator(OperatorContext operatorContext, FunctionManager functionManager, List<RowExpression> projections)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        this.result = new CompletableFuture[projections.size()];
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && !processingPage();
    }

    @Override
    public void addInput(Page page)
    {
        log.info(
                "Remote project addInput for taskId %s: row count: %d, size in bytes: %d",
                operatorContext.getDriverContext().getTaskId(),
                page.getPositionCount(),
                page.getSizeInBytes());
        checkState(!finishing, "Operator is already finishing");
        checkState(!processingPage(), "Still processing previous input");
        requireNonNull(page, "page is null");
        for (int channel = 0; channel < projections.size(); channel++) {
            RowExpression projection = projections.get(channel);
            if (projection instanceof InputReferenceExpression) {
                result[channel] = completedFuture(page.getBlock(((InputReferenceExpression) projection).getField()));
            }
            else if (projection instanceof CallExpression) {
                CallExpression remoteCall = (CallExpression) projection;
                result[channel] = functionManager.executeFunction(
                        remoteCall.getFunctionHandle(),
                        page,
                        remoteCall.getArguments().stream()
                                .map(InputReferenceExpression.class::cast)
                                .map(InputReferenceExpression::getField)
                                .collect(toImmutableList()));
            }
            else {
                checkState(projection instanceof ConstantExpression, format("Does not expect expression type %s", projection.getClass()));
            }
        }
    }

    @Override
    public Page getOutput()
    {
        if (resultReady()) {
            Block[] blocks = new Block[result.length];
            Page output;
            try {
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = result[i].get();
                }
                output = new Page(blocks);
                log.info(
                        "Remote project getOutput for taskId %s: row count: %d, size in bytes: %d",
                        operatorContext.getDriverContext().getTaskId(),
                        output.getPositionCount(),
                        output.getSizeInBytes());
                Arrays.fill(result, null);
                return output;
            }
            catch (InterruptedException ie) {
                log.error("InterruptedException: %s", ie.getMessage());
                currentThread().interrupt();
                throw new RuntimeException(ie);
            }
            catch (ExecutionException e) {
                log.error("ExecutionException: %s", e.getMessage());
                Throwable cause = e.getCause();
                if (cause != null) {
                    throwIfUnchecked(cause);
                    throw new RuntimeException(cause);
                }
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && !processingPage();
    }

    private boolean processingPage()
    {
        // Array result will be filled with nulls when getOutput() produce output.
        // If result has non-null values that means input page is in processing.
        for (int i = 0; i < result.length; i++) {
            if (result[i] != null) {
                return true;
            }
        }
        return false;
    }

    private boolean resultReady()
    {
        for (int i = 0; i < result.length; i++) {
            if (result[i] == null || !result[i].isDone()) {
                return false;
            }
        }
        return true;
    }

    public static class RemoteProjectOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final FunctionManager functionManager;
        private final List<RowExpression> projections;
        private boolean closed;

        public RemoteProjectOperatorFactory(int operatorId, PlanNodeId planNodeId, FunctionManager functionManager, List<RowExpression> projections)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.functionManager = requireNonNull(functionManager, "functionManager is null");
            this.projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        }
        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, RemoteProjectOperator.class.getSimpleName());
            return new RemoteProjectOperator(operatorContext, functionManager, projections);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new RemoteProjectOperatorFactory(operatorId, planNodeId, functionManager, projections);
        }
    }
}
