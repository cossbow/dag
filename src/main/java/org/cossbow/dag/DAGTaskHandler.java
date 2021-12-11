package org.cossbow.dag;

import java.util.concurrent.CompletableFuture;

public interface DAGTaskHandler<N, R> {
    CompletableFuture<R> exec(N node, R input);
}
