package org.cossbow;

import lombok.Data;
import org.cossbow.dag.DAGGraph;
import org.cossbow.dag.DAGTaskHandler;
import org.cossbow.dag.DAGTask;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

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
    static final Map<NodeType, DAGTaskHandler<Node<String>, Object>> handlerMap = Map.of(
            NodeType.CALL, new CallNodeHandler(DeferExecutor)
    );

    public static void main(String[] args) {
        //
        var nodes = new ArrayList<Map.Entry<String, Node<String>>>();
        for (int i = 1; i <= 6; i++) {
            var key = "N-" + i;
            nodes.add(Map.entry(key, new Node<>(key, NodeType.CALL)));
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
        var graph = new DAGGraph<>(nodes, edges);

        var task = new DAGTask<>(graph, n -> handlerMap.get(n.getType()));
        task.call().join();

        EXECUTOR.shutdown();
    }

    public static class CallNodeHandler implements DAGTaskHandler<Node<String>, Object> {
        final AtomicLong SEQ = new AtomicLong(0);
        final Executor executor;

        public CallNodeHandler(Executor executor) {
            this.executor = executor;
        }

        @Override
        public CompletableFuture<Object> exec(Node<String> node, Object input) {
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