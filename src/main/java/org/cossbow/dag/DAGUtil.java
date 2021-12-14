
package org.cossbow.dag;

import java.util.*;

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

    public static <E, C extends Collection<E>, S extends Collection<E>>
    Set<E> subtract(C minuend, S subtraction) {
        var result = new HashSet<>(minuend);
        result.removeAll(subtraction);
        return result;
    }

}
