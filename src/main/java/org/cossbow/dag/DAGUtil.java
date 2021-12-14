
package org.cossbow.dag;

import java.util.*;
import java.util.function.Function;

final
public class DAGUtil {
    private DAGUtil() {
    }

    public static <E, C extends Collection<E>, S extends Collection<E>>
    Set<E> subtract(C minuend, S subtraction) {
        var result = new HashSet<>(minuend);
        result.removeAll(subtraction);
        return result;
    }

    public static <Key> Function<Key, Set<Key>> hashSet() {
        return k -> new HashSet<>();
    }


    public static <Key> Map<Key, Set<Key>> toImmutable(Map<Key, Set<Key>> src) {
        var dst = new HashMap<>(src);
        for (var entry : dst.entrySet()) {
            entry.setValue(Set.copyOf(entry.getValue()));
        }
        return Map.copyOf(dst);
    }


}
