package org.cossbow.dag;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DAGTask<K, N, R> {
    private final DAGGraph<K, N> graph;
    private final Function<N, DAGTaskHandler<N, R>> handlerManager;

    private final Map<K, R> results = new ConcurrentHashMap<>();
    private final Map<K, CompletableFuture<Void>> futures = new ConcurrentHashMap<>();

    public DAGTask(DAGGraph<K, N> graph, Function<N, DAGTaskHandler<N, R>> handlerSupplier) {
        this.graph = Objects.requireNonNull(graph);
        this.handlerManager = Objects.requireNonNull(handlerSupplier);
    }

    private CompletableFuture<Void> execHandler(K key) {
        var node = graph.getNode(key);
        var handler = handlerManager.apply(node);
        if (null == handler) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node " + key + " not exists."));
        }
        return handler.exec(node, null).thenAccept(r -> results.put(key, r));
    }

    private CompletableFuture<Void> execOne(K key) {
        return futures.computeIfAbsent(key, this::execHandler);
    }

    private CompletableFuture<Void> execBatch(Collection<K> keys) {
        var futures = keys.stream().map(this::execDependencies)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    private CompletableFuture<Void> execDependencies(K key) {
        var dependencies = graph.prev(key);
        if (dependencies.isEmpty()) {
            return execOne(key);
        } else {
            return execBatch(dependencies).thenCompose(v -> execOne(key));
        }
    }

    public CompletableFuture<Map<K, R>> call() {
        return execBatch(graph.tails()).thenApply(v -> Map.copyOf(results));
    }

}
