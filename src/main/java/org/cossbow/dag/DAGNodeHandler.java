package org.cossbow.dag;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;


public class DAGNodeHandler<ID, K, D>
        implements BiFunction<K, Map<K, DAGResult<D>>,
        CompletableFuture<DAGResult<D>>> {

    final Supplier<ID> IDGenerator;
    final TriFunction<ID, K, D, CompletableFuture<DAGResult<D>>> executor;
    final TriFunction<ID, K, Map<K, DAGResult<D>>, DAGResult<D>> paramMaker;

    public DAGNodeHandler(Supplier<ID> IDGenerator,
                          TriFunction<ID, K, Map<K, DAGResult<D>>, DAGResult<D>> paramMaker,
                          TriFunction<ID, K, D, CompletableFuture<DAGResult<D>>> executor) {

        this.IDGenerator = IDGenerator;
        this.executor = executor;
        this.paramMaker = paramMaker;
    }

    @Override
    public CompletableFuture<DAGResult<D>> apply(K nodeKey, Map<K, DAGResult<D>> dependentResults) {
        var subtaskId = IDGenerator.get();
        DAGResult<D> input;
        try {
            input = paramMaker.apply(subtaskId, nodeKey, dependentResults);
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
        if (!input.isSuccess()) {
            return CompletableFuture.completedFuture(input);
        }

        return executor.apply(subtaskId, nodeKey, input.getData());
    }

}
