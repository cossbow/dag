package org.cossbow.dag;

import java.util.*;
import java.util.function.Function;

/**
 * 轻量级的Immutable的Graph类
 *
 * @param <Key>
 */
public class ImmutableGraph<Key> {

    /**
     * 顶点集合， Node为key， NodeInfo为value
     */
    private final Set<Key> allNodes;

    /**
     * 有向边集合，起点Node为key， 终点和指向终点的边组合的边组合的MAP为value
     */
    private final Map<Key, Set<Key>> edgesMap;

    /**
     * 反向边集合，终点Node为key，起点和背向起点的边组合为value
     */
    private final Map<Key, Set<Key>> reverseEdgesMap;

    public ImmutableGraph(Collection<Key> allNodes,
                          Collection<Map.Entry<Key, Key>> edges) {
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
    }

    public ImmutableGraph(Set<Key> allNodes,
                          Map<Key, Set<Key>> edgesMap,
                          Map<Key, Set<Key>> reverseEdgesMap) {
        this.allNodes = Set.copyOf(allNodes);
        this.edgesMap = toImmutable(edgesMap);
        this.reverseEdgesMap = toImmutable(reverseEdgesMap);
    }

    private Map<Key, Set<Key>> toImmutable(Map<Key, Set<Key>> src) {
        var dst = new HashMap<>(src);
        for (var entry : dst.entrySet()) {
            entry.setValue(Set.copyOf(entry.getValue()));
        }
        return Map.copyOf(dst);
    }


    private Function<Key, Set<Key>> hashSet() {
        return k -> new HashSet<>();
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


    //

    public Set<Key> getEndNodes() {
        return DAGUtil.subtract(allNodes, edgesMap.keySet());
    }

    public Set<Key> getPreviousNodes(Key node) {
        return reverseEdgesMap.getOrDefault(node, Set.of());
    }

}
