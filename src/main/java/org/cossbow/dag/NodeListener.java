package org.cossbow.dag;

import java.util.function.BiConsumer;

public interface NodeListener<K, D> {

    void onSuccess(K key, DAGResult<D> r);

    void onError(K key, Throwable e);


    //

    static <K, R> NodeListener<K, R> of(BiConsumer<K, DAGResult<R>> onSuccess,
                                        BiConsumer<K, Throwable> onError) {
        return new NodeListener<>() {
            @Override
            public void onSuccess(K k, DAGResult<R> r) {
                onSuccess.accept(k, r);
            }

            @Override
            public void onError(K k, Throwable e) {
                onError.accept(k, e);
            }
        };
    }

}
