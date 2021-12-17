package org.cossbow.dag;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface DAGHandler<Key, Data> {

    CompletableFuture<DAGResult<Data>> call(
            Key nodeKey, Data input,
            Map<Key, DAGResult<Data>> dependentResults);

}
