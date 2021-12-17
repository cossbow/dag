package org.cossbow.dag;

import java.util.Map;

@FunctionalInterface
public interface DAGParamMaker<ID, K, D> {

    /**
     * @param subtaskID        子任务ID
     * @param currentKey       当前节点Key
     * @param input            任务输入
     * @param dependentResults 上游节点的结果集，Map的Key是上游节点的Key
     * @return 返回成功，包含输入给节点的参数；返回失败，包含错误信息
     */
    DAGResult<D> checkAndForm(
            ID subtaskID, K currentKey, D input,
            Map<K, DAGResult<D>> dependentResults);

}
