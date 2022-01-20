package org.cossbow.dag;

import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DAGTaskTest {


    static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(16);

    final HashSet<String> nodes;
    final List<Map.Entry<String, String>> edges;
    final DAGGraph<String> graph;

    {
        nodes = new HashSet<>();
        for (int i = 1; i <= 6; i++) {
            var key = "N-" + i;
            nodes.add(key);
        }

        edges = List.of(
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
    public void testTask() {
        var task = new DAGTask<String, Integer>(graph, (node, results) -> {
            System.out.println("Node-" + node + " input: " + results);
            var input = results.isEmpty() ? 1 :
                    results.values().stream().mapToInt(Integer::intValue).sum();
            var output = input + 1;
            System.out.println("Node-" + node + " return: " + (output));
            return CompletableFuture.completedFuture(output);
        });
        EXECUTOR.execute(task);
        var re = task.join();
        var sum = re.values().stream().mapToInt(Integer::intValue).sum();
        System.out.println(sum);


        EXECUTOR.shutdown();
    }

    @Test
    public void testHandler() {
        final var executor = CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS,
                EXECUTOR);
        final var seq = new AtomicInteger();

        var handler = new DAGNodeHandler<Integer, String, Integer>(
                seq::incrementAndGet, (id, node, results) -> {
            System.out.println("Node-" + node + "/Subtask-" + id + " input " + results);
            var args = results.isEmpty() ? 1 : results.values().stream().mapToInt(DAGResult::getData).sum();
            return DAGResult.success(args);
        }, (id, node, input) -> CompletableFuture.supplyAsync(() -> {
            System.out.println("Node-" + node + "/Subtask-" + id + " return " + (input + 1));
            return DAGResult.success(input + 1);
        }, executor));
        var task = new DAGTask<>(graph, handler);
        EXECUTOR.execute(task);
        EXECUTOR.execute(task);
        var re = task.join();
        var sum = re.values().stream().mapToInt(DAGResult::getData).sum();
        System.out.println(sum);


        EXECUTOR.shutdown();
    }



}
