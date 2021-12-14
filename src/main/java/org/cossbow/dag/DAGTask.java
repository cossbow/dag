package org.cossbow.dag;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DAGTask<K, N, R> {
    private final DAGGraph<K> graph;
    private final Function<K, N> nodeManager;
    private final Function<N, BiFunction<N, R, CompletableFuture<R>>> handlerManager;

    private final Map<K, R> results = new HashMap<>();
    private final Map<K, String> errors = new HashMap<>();
    private final Map<K, CompletableFuture<Void>> futures = new ConcurrentHashMap<>();

    public DAGTask(DAGGraph<K> graph,
                   Function<K, N> nodeManager,
                   Function<N, BiFunction<N, R, CompletableFuture<R>>> handlerManager) {
        this.graph = Objects.requireNonNull(graph);
        this.nodeManager = Objects.requireNonNull(nodeManager);
        this.handlerManager = Objects.requireNonNull(handlerManager);
    }

    private CompletableFuture<Void> execHandler(K key) {
        var node = nodeManager.apply(key);
        var handler = handlerManager.apply(node);
        if (null == handler) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node " + key + " not exists."));
        }
        return handler.apply(node, null).whenComplete((r, e) -> {
            if (null != e) {
                synchronized (errors) {
                    errors.put(key, e.getMessage());
                }
            }
        }).thenAccept(r -> {
            synchronized (results) {
                results.put(key, r);
            }
        });
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
