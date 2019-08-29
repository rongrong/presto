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
package com.facebook.presto.sql.relational;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionImplementation;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.RelationId;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.function.FunctionLanguage.SQL;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;

public final class SqlFunctionUtils
{
    private SqlFunctionUtils() {}

    public static Expression getSqlFunctionExpression(FunctionMetadata metadata, FunctionImplementation implementation, Session session)
    {
        checkArgument(metadata.getLanguage().equals(SQL), format("Expect SQL function, get %s", implementation.getLanguage()));
        checkArgument(implementation.getLanguage().equals(SQL), format("Expect SQL function, get %s", implementation.getLanguage()));
        return new SqlParser().createExpression((String) implementation.getImplementation(), createParsingOptions(session));
    }

    public static Map<NodeRef<Expression>, Type> getSqlFunctionExpressionTypes(
            Session session,
            Metadata metadata,
            Expression expression,
            List<String> argumentNames,
            List<Type> argumentTypes,
            WarningCollector warningCollector)
    {
        ExpressionAnalyzer analyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionManager(),
                metadata.getTypeManager(),
                session,
                emptyList(),
                NOT_SUPPORTED,
                "SQL function does not support subquery",
                warningCollector,
                false);
        Map<String, Type> types = getArgumentTypeMap(argumentNames, argumentTypes);
        ImmutableSet.Builder<NodeRef<Identifier>> inputBuilder = ImmutableSet.builder();
        new IdentifierExtractor().process(expression, inputBuilder);
        inputBuilder.build().forEach(input -> analyzer.setExpressionType(input.getNode(), types.get(input.getNode().getValue())));

        analyzer.analyze(expression, Scope.builder().withRelationType(RelationId.anonymous(), new RelationType()).build());
        return analyzer.getExpressionTypes();
    }

    private static Map<String, Type> getArgumentTypeMap(List<String> argumentNames, List<Type> argumentTypes)
    {
        ImmutableMap.Builder<String, Type> typeBuilder = ImmutableMap.builder();
        for (int i = 0; i < argumentNames.size(); i++) {
            typeBuilder.put(argumentNames.get(i), argumentTypes.get(i));
        }
        return typeBuilder.build();
    }

    public static RowExpression getSqlFunctionRowExpression(FunctionMetadata functionMetadata, FunctionImplementation implementation, Metadata metadata, Session session)
    {
        Expression expression = getSqlFunctionExpression(functionMetadata, implementation, session);

        List<String> argumentNames = functionMetadata.getArgumentNames().get();
        List<Type> argumentTypes = functionMetadata.getArgumentTypes().stream().map(metadata::getType).collect(toImmutableList());
        checkState(argumentNames.size() == argumentTypes.size(), format("Expect argumentNames (size %d) and argumentTypes (size %d) to be of the same size", argumentNames.size(), argumentTypes.size()));
        return SqlToRowExpressionTranslator.translate(
                expression,
                getSqlFunctionExpressionTypes(session, metadata, expression, argumentNames, argumentTypes, WarningCollector.NOOP),
                ImmutableMap.of(),
                metadata.getFunctionManager(),
                metadata.getTypeManager(),
                session,
                true);
    }

    private static class IdentifierExtractor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<NodeRef<Identifier>>>
    {
        @Override
        protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<NodeRef<Identifier>> builder)
        {
            builder.add(NodeRef.of(node));
            return null;
        }
    }
}
