package org.cossbow.dag;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

final
public class DAGTask<Key, Result>
        extends CompletableFuture<Map<Key, Result>>
        implements Runnable {

    private static final VarHandle STARTED;

    static {
        MethodHandles.Lookup l = MethodHandles.lookup();
        try {
            STARTED = l.findVarHandle(DAGTask.class, "started", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }


    //

    private final DAGGraph<Key> graph;
    private final BiFunction<Key, Map<Key, Result>,
            CompletableFuture<Result>> handler;


    // 运行状态Future
    private final Map<Key, CompletableFuture<?>> futures =
            new ConcurrentHashMap<>();
    // 执行结果集
    private final Map<Key, Result> results =
            new ConcurrentHashMap<>();
    // 是否已经启动
    private volatile boolean started = false;


    public DAGTask(DAGGraph<Key> graph,
                   BiFunction<Key, Map<Key, Result>,
                           CompletableFuture<Result>> handler) {
        this.graph = Objects.requireNonNull(graph);
        this.handler = Objects.requireNonNull(handler);
    }

    private Map<Key, Result> dependentResults(Key key) {
        var prev = graph.prev(key);
        if (prev.isEmpty()) {
            return Map.of();
        }

        var resultMap = new HashMap<Key, Result>(prev.size());
        for (Key k : prev) {
            var v = results.get(k);
            if (null != v) {
                resultMap.put(k, v);
            }
        }
        return resultMap;
    }

    private CompletableFuture<?> execHandler(Key key) {
        return handler.apply(key, dependentResults(key))
                .thenAccept(r -> this.results.put(key, r));
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

    private void execute() {
        try {
            execBatch(graph.tails()).whenComplete((v, e) -> {
                if (null == e) {
                    complete(results);
                } else {
                    completeExceptionally(e);
                }
            });
        } catch (Throwable e) {
            completeExceptionally(e);
        }
    }


    public void run() {
        if (!STARTED.compareAndSet(this, false, true)) {
            return;
        }

        execute();
    }

    public boolean isStarted() {
        return started;
    }

}
