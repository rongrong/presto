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

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionRewriter;
import com.facebook.presto.spi.relation.RowExpressionTreeRewriter;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SqlFunctionArgumentBinder
{
    private SqlFunctionArgumentBinder() {}

    public static Expression bindFunctionArguments(Expression function, List<String> argumentNames, List<Expression> argumentValues)
    {
        checkArgument(argumentNames.size() == argumentValues.size(), format("Expect same size for argumentNames (%d) and argumentValues (%d)", argumentNames.size(), argumentValues.size()));
        ImmutableMap.Builder<String, Expression> argumentBindings = ImmutableMap.builder();
        for (int i = 0; i < argumentNames.size(); i++) {
            argumentBindings.put(argumentNames.get(i), argumentValues.get(i));
        }
        return ExpressionTreeRewriter.rewriteWith(new ExpressionFunctionVisitor(argumentBindings.build()), function);
    }

    public static RowExpression bindFunctionArguments(RowExpression function, List<String> argumentNames, List<RowExpression> argumentValues)
    {
        checkArgument(argumentNames.size() == argumentValues.size(), format("Expect same size for argumentNames (%d) and argumentValues (%d)", argumentNames.size(), argumentValues.size()));
        ImmutableMap.Builder<String, RowExpression> argumentBindings = ImmutableMap.builder();
        for (int i = 0; i < argumentNames.size(); i++) {
            argumentBindings.put(argumentNames.get(i), argumentValues.get(i));
        }
        return RowExpressionTreeRewriter.rewriteWith(new RowExpressionRewriter<Map<String, RowExpression>>()
        {
            @Override
            public RowExpression rewriteVariableReference(VariableReferenceExpression variable, Map<String, RowExpression> context, RowExpressionTreeRewriter<Map<String, RowExpression>> treeRewriter)
            {
                if (context.containsKey(variable.getName())) {
                    return context.get(variable.getName());
                }
                return variable;
            }
        }, function, argumentBindings.build());
    }

    private static class ExpressionFunctionVisitor
            extends ExpressionRewriter<Void>
    {
        private final Map<String, Expression> argumentBindings;

        public ExpressionFunctionVisitor(Map<String, Expression> argumentBindings)
        {
            this.argumentBindings = requireNonNull(argumentBindings, "argumentBindings is null");
        }

        @Override
        public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (argumentBindings.containsKey(node.getValue())) {
                return argumentBindings.get(node.getValue());
            }
            return node;
        }
    }
}
