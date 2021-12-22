package org.cossbow.dag;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;


public class DAGNodeHandler<ID, K, D>
        implements TriFunction<K, D, Map<K, DAGResult<D>>,
        CompletableFuture<DAGResult<D>>> {

    final Supplier<ID> IDGenerator;
    final TriFunction<ID, K, D, CompletableFuture<DAGResult<D>>> executor;
    final DAGParamMaker<ID, K, D, DAGResult<D>> paramMaker;

    public DAGNodeHandler(Supplier<ID> IDGenerator,
                          DAGParamMaker<ID, K, D, DAGResult<D>> paramMaker,
                          TriFunction<ID, K, D, CompletableFuture<DAGResult<D>>> executor) {
        this.IDGenerator = IDGenerator;
        this.executor = executor;
        this.paramMaker = paramMaker;
    }


    @Override
    public CompletableFuture<DAGResult<D>> apply(K nodeKey, D input, Map<K, DAGResult<D>> dependentResults) {
        var subtaskId = IDGenerator.get();
        DAGResult<D> form;
        try {
            form = paramMaker.checkAndForm(subtaskId, nodeKey, input, dependentResults);
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
        if (!form.isSuccess()) {
            return CompletableFuture.completedFuture(form);
        }

        return executor.apply(subtaskId, nodeKey, form.getData());
    }

}
