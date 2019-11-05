package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.iterative.rule.PlanRemotePojections.ProjectionContext;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sqlfunction.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.sqlfunction.testing.SqlInvokedFunctionTestUtils;
import com.facebook.presto.testing.InMemoryFunctionNamespaceManager;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.sqlfunction.testing.SqlInvokedFunctionTestUtils.FUNCTION_REMOTE_FOO_0;
import static com.facebook.presto.sqlfunction.testing.SqlInvokedFunctionTestUtils.FUNCTION_REMOTE_FOO_1;
import static com.facebook.presto.sqlfunction.testing.SqlInvokedFunctionTestUtils.FUNCTION_REMOTE_FOO_2;
import static org.testng.Assert.assertEquals;

public class TestPlanRemoteProjections
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
        assertEquals(rewritten.get(0).getProjections().size(), 1);
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
}
