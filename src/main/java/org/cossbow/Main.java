package org.cossbow;

import lombok.Data;
import org.cossbow.dag.DAGGraph;
import org.cossbow.dag.DAGTask;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

public class Main {


    enum NodeType {
        CALL, DECISION, COMPUTE,
    }

    @Data
    static class Node<K> {
        private final K key;
        private final NodeType type;


    }


    @Data
    static class Edge<K> {
        private final K from;
        private final K to;
    }


    static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(16);
    static final Executor DeferExecutor = CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS,
            EXECUTOR);
    static final Map<NodeType, BiFunction<Node<String>, Object, CompletableFuture<Object>>> HandlerMap = Map.of(
            NodeType.CALL, new CallNodeHandler(DeferExecutor)
    );

    public static void main(String[] args) {
        //
        var nodes = new HashMap<String, Node<String>>();
        for (int i = 1; i <= 6; i++) {
            var key = "N-" + i;
            nodes.put(key, new Node<>(key, NodeType.CALL));
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

        var task = new DAGTask<>(graph, nodes::get, n -> HandlerMap.get(n.getType()));
        task.call().join();

        EXECUTOR.shutdown();
    }

    public static class CallNodeHandler
            implements BiFunction<Node<String>, Object, CompletableFuture<Object>> {
        final AtomicLong SEQ = new AtomicLong(0);
        final Executor executor;

        public CallNodeHandler(Executor executor) {
            this.executor = executor;
        }

        @Override
        public CompletableFuture<Object> apply(Node<String> node, Object input) {
            System.out.println("request: " + node.getKey());
            return CompletableFuture.supplyAsync(() -> {
                var seq = SEQ.incrementAndGet();
                System.out.println("response: " + node.getKey() + ", seq=" + seq);
                return Map.of(
                        "key", node.getKey(),
                        "input", Objects.requireNonNullElse(input, Map.of()),
                        "sequence", seq
                );
            }, executor);
        }
    }

}
