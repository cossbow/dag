package org.cossbow.dag;

import java.util.Map;

@FunctionalInterface
public interface NodeParamHandler<K, D> {

    D form(K currentKey, D input, Map<K, DAGResult<D>> dependentResults);

}
