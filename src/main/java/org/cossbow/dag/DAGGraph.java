package org.cossbow.dag;

import java.util.*;

public class DAGGraph<Key> {
    private final Set<Key> keys;

    private final Map<Key, Set<Key>> edgesMap;
    private final Map<Key, Set<Key>> reverseEdgesMap;

    public DAGGraph(Set<Key> keys,
                    List<Map.Entry<Key, Key>> edges) {
        if (null == keys || keys.isEmpty()) {
            throw new IllegalArgumentException("keys empty");
        }
        var edgesMap = new HashMap<Key, Set<Key>>();
        var reverseEdgesMap = new HashMap<Key, Set<Key>>();
        for (var e : edges) {
            if (!keys.contains(e.getKey())) {
                throw new IllegalArgumentException(
                        "Key " + e.getKey() + " not exists in nodes");
            }
            if (!keys.contains(e.getValue())) {
                throw new IllegalArgumentException(
                        "Key " + e.getValue() + " not exists in nodes");
            }
            addEdge(e.getKey(), e.getValue(), edgesMap);
            addEdge(e.getValue(), e.getKey(), reverseEdgesMap);
        }
        // TODO check
        for (var e : edgesMap.entrySet()) {
            e.setValue(Set.copyOf(e.getValue()));
        }
        for (var e : reverseEdgesMap.entrySet()) {
            e.setValue(Set.copyOf(e.getValue()));
        }
        //
        this.keys = Set.copyOf(keys);
        this.edgesMap = Map.copyOf(edgesMap);
        this.reverseEdgesMap = Map.copyOf(reverseEdgesMap);
    }

    private void topologicalSort(Set<Key> keys,
                                 Map<Key, Set<Key>> edgesMap) {

        var prev = new ArrayList<>(edgesMap.keySet());
        var queue = new ArrayDeque<>(prev);
        var next = new ArrayList<Key>();
        while (queue.size() >= keys.size()) {
            for (Key key : prev) {
                next.addAll(edgesMap.get(key));
            }
            queue.addAll(next);
            prev.clear();
            prev.addAll(next);
            next.clear();
        }

    }


    private boolean check(int keyCount, Map<Key, Set<Key>> edgesMap) {
        var queue = new ArrayDeque<Key>();
        var prev = new ArrayList<>(edgesMap.keySet());
        var next = new ArrayList<Key>();
        while (keyCount <= queue.size()) {
            for (Key key : prev) {
                var n = edgesMap.get(key);
                if (null == n) continue;
                next.addAll(n);
                queue.addAll(prev);
                prev.clear();
                prev.addAll(next);
                next.clear();
            }
        }
        return queue.size() > keyCount;
    }

    private boolean tarjan() {
        return false;
    }

    private void addEdge(Key from, Key to, Map<Key, Set<Key>> map) {
        map.computeIfAbsent(from, k -> new HashSet<>()).add(to);
    }

    public Set<Key> tails() {
        var set = new HashSet<>(keys);
        set.removeAll(edgesMap.keySet());
        return set;
    }

    public Set<Key> prev(Key key) {
        return reverseEdgesMap.getOrDefault(key, Set.of());
    }

}
