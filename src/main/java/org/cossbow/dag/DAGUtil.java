package org.cossbow.dag;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

final
public class DAGUtil {
    private DAGUtil() {
    }


    public static <Key> boolean check(List<Map.Entry<Key, Key>> edges) {
        var queue = new LinkedList<Key>();
        for (var e : edges) {
            queue.push(e.getKey());

        }
        return true;
    }

}
