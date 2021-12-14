package org.cossbow.dag;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;


public class DAGTask<Key, Data> {

    private final DAGGraph<Key> graph;
    private final BiFunction<Key, Data, CompletableFuture<DAGResult<Data>>> taskHandler;
    private final NodeParamHandler<Key, Data> paramHandler;
    private final BiFunction<Data, Map<Key, DAGResult<Data>>, DAGResult<Data>> resultHandler;
    private final NodeListener<Key, Data> listener;
    private final Data input;


    // 运行状态Future
    private final Map<Key, CompletableFuture<?>> futures =
            new ConcurrentHashMap<>();
    // 执行结果集
    private final Map<Key, DAGResult<Data>> results =
            new ConcurrentHashMap<>();

    DAGTask(DAGGraph<Key> graph,
            BiFunction<Key, Data, CompletableFuture<DAGResult<Data>>> taskHandler,
            NodeParamHandler<Key, Data> paramHandler,
            NodeListener<Key, Data> listener,
            Data input,
            BiFunction<Data, Map<Key, DAGResult<Data>>, DAGResult<Data>> resultHandler) {
        this.graph = Objects.requireNonNull(graph);
        this.taskHandler = Objects.requireNonNull(taskHandler);
        this.paramHandler = Objects.requireNonNull(paramHandler);
        this.resultHandler = Objects.requireNonNull(resultHandler);
        this.listener = listener;
        this.input = Objects.requireNonNull(input);
    }

    private CompletableFuture<?> execHandler(Key key) {
        var prev = graph.prev(key);
        var resultMap = new HashMap<Key, DAGResult<Data>>(prev.size());
        for (Key k : prev) {
            var v = results.get(k);
            if (null != v) {
                resultMap.put(k, v);
            }
        }
        var args = paramHandler.form(key, input, resultMap);
        return taskHandler.apply(key, args).whenComplete((r, e) -> {
            if (null == e) {
                results.put(key, r);
                if (null != listener) {
                    listener.onSuccess(key, r);
                }
            } else if (null != listener) {
                listener.onError(key, e);
            }
        });
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

    public CompletableFuture<DAGResult<Data>> call() {
        return execBatch(graph.tails())
                .thenApply(v -> resultHandler.apply(input, results));
    }

    public boolean started(Key key) {
        return futures.containsKey(key);
    }

    public boolean complete(Key key) {
        return results.containsKey(key);
    }


    //

    public static <K, D> Builder<K, D> builder(DAGGraph<K> graph) {
        return new Builder<K, D>().graph(graph);
    }

    public static class Builder<K, D> {
        private DAGGraph<K> graph;
        private BiFunction<K, D, CompletableFuture<DAGResult<D>>> taskHandler;
        private NodeParamHandler<K, D> paramHandler;
        private NodeListener<K, D> listener;
        private D input;
        private BiFunction<D, Map<K, DAGResult<D>>, DAGResult<D>> resultHandler;

        public Builder<K, D> graph(DAGGraph<K> graph) {
            this.graph = graph;
            return this;
        }

        public Builder<K, D> taskHandler(BiFunction<K, D, CompletableFuture<DAGResult<D>>> taskHandler) {
            this.taskHandler = taskHandler;
            return this;
        }

        public Builder<K, D> inputHandler(NodeParamHandler<K, D> paramHandler) {
            this.paramHandler = paramHandler;
            return this;
        }

        public Builder<K, D> listener(NodeListener<K, D> listener) {
            this.listener = listener;
            return this;
        }

        public Builder<K, D> input(D input) {
            this.input = input;
            return this;
        }

        public Builder<K, D> resultHandler(
                BiFunction<D, Map<K, DAGResult<D>>, DAGResult<D>> resultHandler) {
            this.resultHandler = resultHandler;
            return this;
        }

        public DAGTask<K, D> build() {
            return new DAGTask<>(graph,
                    taskHandler,
                    paramHandler,
                    listener,
                    input,
                    resultHandler);
        }
    }

}
