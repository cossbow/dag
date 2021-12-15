package org.cossbow.dag;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

final
public class DAGTask<Key, Data> {

    private final DAGGraph<Key> graph;
    private final BiFunction<Key, Data, CompletableFuture<DAGResult<Data>>> taskHandler;
    private final ParamHandler<Key, Data> paramHandler;
    private final Data input;


    // 运行状态Future
    private final Map<Key, CompletableFuture<?>> futures =
            new ConcurrentHashMap<>();
    // 执行结果集
    private final Map<Key, DAGResult<Data>> results =
            new ConcurrentHashMap<>();
    // 只读结果集
    private final Map<Key, DAGResult<Data>> immutableResult =
            Collections.unmodifiableMap(results);
    // Task最终结果future
    private volatile CompletableFuture<Map<Key, DAGResult<Data>>> future;

    public DAGTask(DAGGraph<Key> graph,
                   BiFunction<Key, Data, CompletableFuture<DAGResult<Data>>> taskHandler,
                   ParamHandler<Key, Data> paramHandler,
                   Data input) {
        this.graph = Objects.requireNonNull(graph);
        this.taskHandler = Objects.requireNonNull(taskHandler);
        this.paramHandler = Objects.requireNonNull(paramHandler);
        this.input = Objects.requireNonNull(input);
    }

    private Data dependentResults(Key key) {
        var prev = graph.prev(key);
        if (prev.isEmpty()) {
            return paramHandler.form(key, input, Map.of());
        }

        var resultMap = new HashMap<Key, DAGResult<Data>>(prev.size());
        for (Key k : prev) {
            var v = results.get(k);
            if (null != v) {
                resultMap.put(k, v);
            }
        }
        return paramHandler.form(key, input, resultMap);
    }

    private CompletableFuture<?> execHandler(Key key) {
        var args = dependentResults(key);
        return taskHandler.apply(key, args)
                .thenAccept(r -> results.put(key, r));
    }

    private CompletableFuture<?> execOne(Key key) {
        return futures.computeIfAbsent(key, this::execHandler);
    }

    private CompletableFuture<?> execBatch(Collection<Key> keys) {
        var futures = keys.stream().map(this::execDependencies)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    private CompletableFuture<?> execDependencies(Key key) {
        var dependencies = graph.prev(key);
        if (dependencies.isEmpty()) {
            return execOne(key);
        } else {
            return execBatch(dependencies)
                    .thenCompose(v -> execOne(key));
        }
    }

    private CompletableFuture<Map<Key, DAGResult<Data>>> doCall() {
        return execBatch(graph.tails()).thenApply(v -> immutableResult);
    }

    public CompletableFuture<Map<Key, DAGResult<Data>>> call() {
        var f = future;
        if (null == f) {
            synchronized (this) {
                f = future;
                if (null == f) {
                    f = future = doCall();
                }
            }
        }
        return f;
    }

}
