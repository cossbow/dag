package org.cossbow.dag;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DAGGraphTest {

    @Test
    public void testLegal() {
        var nodes = List.of(1, 2, 3, 4);
        var edges = List.of(
                Map.entry(1, 2),
                Map.entry(1, 3),
                Map.entry(2, 4),
                Map.entry(3, 4)
        );
        new DAGGraph<>(nodes, edges);

        var re = DAGUtil.topologicalSort(Set.copyOf(nodes), edges);
        Assert.assertTrue(re.getKey());
        Assert.assertEquals(nodes, re.getValue());
    }

    @Test
    public void testIllegal1() {
        var nodes = List.of(1, 2, 3, 4);
        var edges = List.of(
                Map.entry(1, 2),
                Map.entry(2, 3),
                Map.entry(3, 4),
                Map.entry(4, 1)
        );
        try {
            new DAGGraph<>(nodes, edges);
            Assert.fail("The graph is illegal");
        } catch (IllegalArgumentException e) {
        }

        var re = DAGUtil.topologicalSort(Set.copyOf(nodes), edges);
        Assert.assertFalse(re.getKey());
    }

    @Test
    public void testIllegal2() {
        var nodes = List.of(1, 2, 3, 4, 5);
        var edges = List.of(
                Map.entry(1, 2),
                Map.entry(2, 3),
                Map.entry(3, 4),
                Map.entry(4, 5),
                Map.entry(5, 1)
        );
        try {
            new DAGGraph<>(nodes, edges);
            Assert.fail("The graph is illegal");
        } catch (IllegalArgumentException e) {
        }

        var re = DAGUtil.topologicalSort(Set.copyOf(nodes), edges);
        Assert.assertFalse(re.getKey());
    }

    @Test
    public void testIllegal3() {
        var nodes = List.of(1, 2, 3, 4, 5, 6);
        var edges = List.of(
                Map.entry(1, 2),
                Map.entry(2, 3),
                Map.entry(3, 4),
                Map.entry(4, 5),
                Map.entry(5, 6),
                Map.entry(6, 1)
        );
        try {
            new DAGGraph<>(nodes, edges);
            Assert.fail("It's a cyclic graph");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testKeys() {
        var nodes = Set.of(1, 2, 3, 4, 5, 6);
        var edges = List.of(
                Map.entry(1, 2),
                Map.entry(1, 3),
                Map.entry(2, 4),
                Map.entry(3, 4),
                Map.entry(1, 5),
                Map.entry(3, 5),
                Map.entry(6, 5)
        );
        var graph = new DAGGraph<>(nodes, edges);

        Assert.assertEquals(Set.of(1, 6), graph.heads());
        Assert.assertEquals(Set.of(4, 5), graph.tails());
    }

    @Test
    public void testDependency() {
        var nodes = List.of(1, 2, 3, 4);
        var edges = List.of(
                Map.entry(1, 2),
                Map.entry(1, 3),
                Map.entry(2, 4),
                Map.entry(3, 4)
        );
        var r = new DAGGraph<>(nodes, edges);
        Assert.assertEquals(Set.copyOf(nodes), r.allNodes());

        Assert.assertEquals(Set.of(1), r.heads());
        Assert.assertEquals(Set.of(2, 3), r.next(1));
        Assert.assertEquals(Set.of(4), r.next(2));
        Assert.assertEquals(Set.of(4), r.next(3));
        Assert.assertTrue(r.next(4).isEmpty());

        Assert.assertEquals(Set.of(4), r.tails());
        Assert.assertEquals(Set.of(2, 3), r.prev(4));
        Assert.assertEquals(Set.of(1), r.prev(2));
        Assert.assertEquals(Set.of(1), r.prev(3));
        Assert.assertTrue(r.prev(1).isEmpty());
    }

    @Test
    public void bfs() {
        var nodes = Set.of(1, 2, 3, 4,5,6);
        var edges = List.of(
                Map.entry(1, 2),
                Map.entry(1, 3),
                Map.entry(2, 4),
                Map.entry(2, 5),
                Map.entry(3, 4),
                Map.entry(5, 6)
        );
        DAGUtil.bfs(nodes, edges, i -> {
            System.out.println(i);
        });
    }

}
