package org.cossbow.dag;

import java.util.concurrent.CompletableFuture;

public interface NodeHandler<N, D> {

    CompletableFuture<DAGResult<D>> call(N node, D input);

    default DAGResult<D> success(D result) {
        return new DAGResult<>(true, result, null);
    }

    default DAGResult<D> error(String error) {
        return new DAGResult<>(true, null, error);
    }

}
