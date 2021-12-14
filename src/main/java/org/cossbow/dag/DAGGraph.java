package org.cossbow.dag;

import java.util.*;

import static org.cossbow.dag.DAGUtil.hashSet;
import static org.cossbow.dag.DAGUtil.toImmutable;


/**
 * <h3>有向无环图</h3>
 * <div>设计为不可变类型，创建时构建好图并检查错误</div>
 * <div>2021-12-14</div>
 *
 * @author jiangjianjun5
 */
public class DAGGraph<Key> {
    private final Set<Key> allNodes;

    private final Set<Key> heads, tails;

    private final Map<Key, Set<Key>> edgesMap;
    private final Map<Key, Set<Key>> reverseEdgesMap;

    public DAGGraph(Collection<Key> allNodes,
                    Collection<Map.Entry<Key, Key>> edges) {
        if (null == allNodes || allNodes.isEmpty()) {
            throw new IllegalArgumentException("keys empty");
        }
        this.allNodes = Set.copyOf(allNodes);
        if (this.allNodes.size() != allNodes.size()) {
            throw new IllegalArgumentException("Has duplicate Key");
        }

        var edgesMap = new HashMap<Key, Set<Key>>();
        var reverseEdgesMap = new HashMap<Key, Set<Key>>();
        var queue = new LinkedList<Key>();
        for (var edge : edges) {
            Key from = edge.getKey(), to = edge.getValue();

            if (!this.allNodes.contains(from)) {
                throw new IllegalArgumentException("Key not exists: " + from);
            }
            if (!this.allNodes.contains(to)) {
                throw new IllegalArgumentException("Key not exists: " + to);
            }

            if (!isLegalAddEdge(allNodes.size(), queue, edgesMap, from, to)) {
                throw new IllegalArgumentException(
                        "Serious error: edge(" + from + " -> " + to + ") is invalid, cause cycle！");
            }
            queue.clear();

            edgesMap.computeIfAbsent(from, hashSet()).add(to);
            reverseEdgesMap.computeIfAbsent(to, hashSet()).add(from);
        }

        this.edgesMap = toImmutable(edgesMap);
        this.reverseEdgesMap = toImmutable(reverseEdgesMap);
        this.tails = DAGUtil.subtract(this.allNodes, this.edgesMap.keySet());
        this.heads = DAGUtil.subtract(this.allNodes, this.reverseEdgesMap.keySet());
    }

    private boolean isLegalAddEdge(
            int verticesCount,
            LinkedList<Key> queue,
            Map<Key, Set<Key>> edgesMap,
            Key fromNode, Key toNode) {
        if (Objects.equals(fromNode, toNode)) {
            return false;
        }

        queue.add(toNode);

        while (!queue.isEmpty() && (--verticesCount > 0)) {
            Key key = queue.poll();
            var subsequentNodes = edgesMap.get(key);
            if (null != subsequentNodes) {
                for (Key subsequentNode : subsequentNodes) {
                    if (subsequentNode.equals(fromNode)) {
                        return false;
                    }

                    queue.add(subsequentNode);
                }
            }
        }

        return true;
    }

    private void addEdge(Key from, Key to, Map<Key, Set<Key>> map) {
        map.computeIfAbsent(from, k -> new HashSet<>()).add(to);
    }


    //

    public Set<Key> allNodes() {
        return allNodes;
    }

    public Set<Key> heads() {
        return heads;
    }

    public Set<Key> tails() {
        return tails;
    }

    public Set<Key> prev(Key key) {
        return reverseEdgesMap.getOrDefault(key, Set.of());
    }

    public Set<Key> next(Key key) {
        return edgesMap.getOrDefault(key, Set.of());
    }

}
