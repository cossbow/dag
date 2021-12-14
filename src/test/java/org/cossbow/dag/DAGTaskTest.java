package org.cossbow.dag;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class DAGTaskTest {


    enum NodeType {
        CALL, DECISION, COMPUTE,
    }

    @Data
    static class Node {
        private final String key;
        private final NodeType type;


    }

    @Data
    static class Edge {
        private final String from;
        private final String to;
    }


    static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(16);
    static final Executor DeferExecutor = CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS,
            EXECUTOR);
    static final Map<NodeType, NodeHandler<Node, Integer>> HandlerMap = Map.of(
            NodeType.CALL, new CallNodeHandler(DeferExecutor)
    );

    public static void main(String[] args) {
        //
        var nodes = new HashMap<String, Node>();
        for (int i = 1; i <= 6; i++) {
            var key = "N-" + i;
            nodes.put(key, new Node(key, NodeType.CALL));
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
        var graph = new DAGGraph<>(nodes.keySet(), edges);

        var task = DAGTask.<String, Integer>builder(graph).taskHandler((k, i) -> {
            var node = nodes.get(k);
            return HandlerMap.get(node.getType()).call(node, i);
        }).inputHandler((k, t, results) -> {
            if (results.isEmpty()) return t;
            int sum = 0;
            for (var v : results.values()) {
                if (v.isSuccess()) sum += v.getOutput();
            }
            return sum;
        }).listener(NodeListener.of((key, result) -> {
            System.out.println(key + " complete: " + result);
        }, (key, e) -> {
            System.err.println(key + " complete: " + e.getMessage());
        })).resultHandler((i, results) -> {
            int sum = 0;
            for (var v : results.values()) {
                if (v.isSuccess()) sum += v.getOutput();
            }
            return new DAGResult<>(true, sum, null);
        }).input(1).build();
        var re = task.call().join();
        System.out.println(re);

        EXECUTOR.shutdown();
    }


    public static class CallNodeHandler
            implements NodeHandler<Node, Integer> {
        final AtomicLong SEQ = new AtomicLong(0);
        final Executor executor;

        public CallNodeHandler(Executor executor) {
            this.executor = executor;
        }

        @Override
        public CompletableFuture<DAGResult<Integer>> call(Node node, Integer input) {
            System.out.println("request: " + node.getKey());
            return CompletableFuture.supplyAsync(() -> {
                var seq = SEQ.incrementAndGet();
                System.out.println("response: " + node.getKey() + ", seq=" + seq);
                return success(input + 1);
            }, executor);
        }
    }

}
