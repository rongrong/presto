package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.assertions.ExpressionMatcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.PlanRemotePojections.ProjectionContext;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sqlfunction.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.sqlfunction.testing.SqlInvokedFunctionTestUtils;
import com.facebook.presto.testing.InMemoryFunctionNamespaceManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sqlfunction.testing.SqlInvokedFunctionTestUtils.FUNCTION_REMOTE_FOO_0;
import static com.facebook.presto.sqlfunction.testing.SqlInvokedFunctionTestUtils.FUNCTION_REMOTE_FOO_1;
import static com.facebook.presto.sqlfunction.testing.SqlInvokedFunctionTestUtils.FUNCTION_REMOTE_FOO_2;
import static org.testng.Assert.assertEquals;

public class TestPlanRemoteProjections
        extends BaseRuleTest
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    public TestPlanRemoteProjections()
    {
        FunctionManager functionManager = METADATA.getFunctionManager();
        functionManager.addTestFunctionNamespace(new InMemoryFunctionNamespaceManager(new SqlInvokedFunctionNamespaceManagerConfig()), "unittest.memory");
        functionManager.createFunction(FUNCTION_REMOTE_FOO_0, true);
        functionManager.createFunction(FUNCTION_REMOTE_FOO_1, true);
        functionManager.createFunction(FUNCTION_REMOTE_FOO_2, true);
        functionManager.createFunction(SqlInvokedFunctionTestUtils.FUNCTION_REMOTE_FOO_3, true);
    }

    @Test
    void testLocalOnly()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        planBuilder.variable("x", INTEGER);
        planBuilder.variable("y", INTEGER);

        PlanRemotePojections rule = new PlanRemotePojections(METADATA.getFunctionManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("abs(x) + abs(y)"))
                .put(planBuilder.variable("b", BOOLEAN), planBuilder.rowExpression("x is null and y is null"))
                .build(), new PlanVariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 1);
        assertEquals(rewritten.get(0).getProjections().size(), 2);
    }

    @Test
    void testRemoteOnly()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);

        PlanRemotePojections rule = new PlanRemotePojections(METADATA.getFunctionManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("unittest.memory.remote_foo()"))
                .put(planBuilder.variable("b"), planBuilder.rowExpression("unittest.memory.remote_foo(unittest.memory.remote_foo())"))
                .build(), new PlanVariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 2);
        assertEquals(rewritten.get(1).getProjections().size(), 2);
    }

    @Test
    void testRemoteAndLocal()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        planBuilder.variable("x", INTEGER);
        planBuilder.variable("y", INTEGER);

        PlanRemotePojections rule = new PlanRemotePojections(METADATA.getFunctionManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("unittest.memory.remote_foo(x, y + unittest.memory.remote_foo(x))"))
                .put(planBuilder.variable("b"), planBuilder.rowExpression("abs(x)"))
                .put(planBuilder.variable("c"), planBuilder.rowExpression("abs(unittest.memory.remote_foo())"))
                .put(planBuilder.variable("d"), planBuilder.rowExpression("unittest.memory.remote_foo(x + y, abs(x))"))
                .build(), new PlanVariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 4);
        assertEquals(rewritten.get(3).getProjections().size(), 4);
    }

    @Test
    void testSpecialForm()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        planBuilder.variable("x", INTEGER);
        planBuilder.variable("y", INTEGER);

        PlanRemotePojections rule = new PlanRemotePojections(METADATA.getFunctionManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("unittest.memory.remote_foo(x, y + unittest.memory.remote_foo(x))"))
                .put(planBuilder.variable("b"), planBuilder.rowExpression("x IS NULL OR y IS NULL"))
                .put(planBuilder.variable("c"), planBuilder.rowExpression("IF(abs(unittest.memory.remote_foo()) > 0, x, y)"))
                .put(planBuilder.variable("d"), planBuilder.rowExpression("unittest.memory.remote_foo(x + y, abs(x))"))
                .build(), new PlanVariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 4);
        assertEquals(rewritten.get(3).getProjections().size(), 4);
    }

    @Test
    void testPlanRewrite()
    {
        FunctionManager functionManager = getFunctionManager();
        functionManager.addTestFunctionNamespace(new InMemoryFunctionNamespaceManager(new SqlInvokedFunctionNamespaceManagerConfig()), "unittest.memory");
        functionManager.createFunction(FUNCTION_REMOTE_FOO_0, true);
        functionManager.createFunction(FUNCTION_REMOTE_FOO_1, true);
        functionManager.createFunction(FUNCTION_REMOTE_FOO_2, true);
        functionManager.createFunction(SqlInvokedFunctionTestUtils.FUNCTION_REMOTE_FOO_3, true);

        tester().assertThat(new PlanRemotePojections(functionManager))
                .on(p -> {
                    p.variable("x", INTEGER);
                    p.variable("y", INTEGER);
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a"), p.rowExpression("unittest.memory.remote_foo(x, y + unittest.memory.remote_foo(x))")) // identity
                                    .put(p.variable("b"), p.rowExpression("x IS NULL OR y IS NULL")) // complex expression referenced multiple times
                                    .put(p.variable("c"), p.rowExpression("abs(unittest.memory.remote_foo()) > 0")) // complex expression referenced multiple times
                                    .put(p.variable("d"), p.rowExpression("unittest.memory.remote_foo(x + y, abs(x))")) // literal referenced multiple times
                                    .build(),
                            p.values(p.variable("x", INTEGER), p.variable("y", INTEGER)));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("unittest.memory.remote_foo(x, add)"),
                                        "b", PlanMatchPattern.expression("b"),
                                        "c", PlanMatchPattern.expression("c"),
                                        "d", PlanMatchPattern.expression("d")),
                                project(
                                        ImmutableMap.of(
                                                "x", PlanMatchPattern.expression("x"),
                                                "add", PlanMatchPattern.expression("y + unittest_memory_remote_foo"),
                                                "b", PlanMatchPattern.expression("b"),
                                                "c", PlanMatchPattern.expression("abs(unittest_memory_remote_foo_7) > expr_8"),
                                                "d", PlanMatchPattern.expression("d")),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("x", PlanMatchPattern.expression("x"))
                                                        .put("y", PlanMatchPattern.expression("y"))
                                                        .put("unittest_memory_remote_foo", PlanMatchPattern.expression("unittest.memory.remote_foo(x)"))
                                                        .put("b", PlanMatchPattern.expression("b"))
                                                        .put("unittest_memory_remote_foo_7", PlanMatchPattern.expression("unittest.memory.remote_foo()"))
                                                        .put("expr_8", PlanMatchPattern.expression("expr_8"))
                                                        .put("d", PlanMatchPattern.expression("unittest.memory.remote_foo(add_14, abs_16)"))
                                                        .build(),
                                                project(
                                                        ImmutableMap.<String, ExpressionMatcher>builder()
                                                                .put("x", PlanMatchPattern.expression("x"))
                                                                .put("y", PlanMatchPattern.expression("y"))
                                                                .put("b", PlanMatchPattern.expression("x IS NULL OR y is NULL"))
                                                                .put("expr_8", PlanMatchPattern.expression("0"))
                                                                .put("add_14", PlanMatchPattern.expression("x + y"))
                                                                .put("abs_16", PlanMatchPattern.expression("abs(x)"))
                                                                .build(),
                                                        values(ImmutableMap.of("x", 0, "y", 1)))))));
    }
}
