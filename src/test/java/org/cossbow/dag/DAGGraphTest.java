package org.cossbow.dag;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DAGGraphTest {

    @Test
    public void testCheckAcyclic() {
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

}
