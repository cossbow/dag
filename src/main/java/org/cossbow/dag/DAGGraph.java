package org.cossbow.dag;

import java.util.*;

public class DAGGraph<Key, Node> {
    private final Map<Key, Node> nodeMap;

    private final Map<Key, Set<Key>> edgesMap;
    private final Map<Key, Set<Key>> reverseEdgesMap;

    public DAGGraph(List<Map.Entry<Key, Node>> nodes,
                    List<Map.Entry<Key, Key>> edges) {
        var nodeMap = new HashMap<Key, Node>(nodes.size());
        for (var e : nodes) {
            nodeMap.put(e.getKey(), e.getValue());
        }
        if (nodeMap.size() != nodes.size()) {
            throw new IllegalArgumentException("Exists duplicated Key");
        }

        var edgesMap = new HashMap<Key, Set<Key>>();
        var reverseEdgesMap = new HashMap<Key, Set<Key>>();
        for (var e : edges) {
            if (nodeMap.containsKey(e.getKey())) {
                throw new IllegalArgumentException(
                        "Key " + e.getKey() + " not exists in nodes");
            }
            if (nodeMap.containsKey(e.getValue())) {
                throw new IllegalArgumentException(
                        "Key " + e.getValue() + " not exists in nodes");
            }
            addEdge(e.getKey(), e.getValue(), edgesMap);
            addEdge(e.getValue(), e.getKey(), reverseEdgesMap);
        }
        for (var e : edgesMap.entrySet()) {
            e.setValue(Set.copyOf(e.getValue()));
        }
        for (var e : reverseEdgesMap.entrySet()) {
            e.setValue(Set.copyOf(e.getValue()));
        }
        //
        this.edgesMap = Map.copyOf(edgesMap);
        this.nodeMap = Map.copyOf(nodeMap);
        this.reverseEdgesMap = Map.copyOf(reverseEdgesMap);
    }

    private void addEdge(Key from, Key to, Map<Key, Set<Key>> map) {
        map.computeIfAbsent(from, k -> new HashSet<>()).add(to);
    }

    public Node getNode(Key key) {
        return nodeMap.get(key);
    }

    public Set<Key> tails() {
        var set = new HashSet<>(nodeMap.keySet());
        set.removeAll(edgesMap.keySet());
        return set;
    }

    public Set<Key> prev(Key key) {
        return reverseEdgesMap.getOrDefault(key, Set.of());
    }

}
