package org.cossbow.dag;

import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DAGTaskTest {


    static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(16);


    final DAGGraph<String> graph;

    {
        var nodes = new HashSet<String>();
        for (int i = 1; i <= 6; i++) {
            var key = "N-" + i;
            nodes.add(key);
        }
        var edges = List.of(
                Map.entry("N-1", "N-2"),
                Map.entry("N-1", "N-3"),
                Map.entry("N-2", "N-4"),
                Map.entry("N-3", "N-4"),
                Map.entry("N-3", "N-5"),
                Map.entry("N-5", "N-6"),
                Map.entry("N-1", "N-6")
        );

        graph = new DAGGraph<>(nodes, edges);
    }

    @Test
    public void testCalc() {
        final var executor = CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS,
                EXECUTOR);
        var task = new DAGTask<>(graph, (k, i) -> {
            return CompletableFuture.supplyAsync(() -> {
                System.out.println(k + " return " + (i + 1));
                return new DAGResult<>(i + 1);
            }, executor);
        }, (k, t, results) -> {
            System.out.println(k + " input " + results);
            if (results.isEmpty()) return t;
            int sum = 0;
            for (var v : results.values()) {
                if (v.isSuccess()) sum += v.getOutput();
            }
            return sum;
        }, 1);
        var re = task.call().join();

        System.out.println(re);

        EXECUTOR.shutdown();
    }


}
