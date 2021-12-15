package org.cossbow.dag;

import lombok.Data;

@Data
public class DAGResult<D> {
    private boolean success;
    private D output;
    private String error;

    public DAGResult(boolean success, D result, String error) {
        this.success = success;
        this.output = result;
        this.error = error;
    }

    public DAGResult(D output) {
        this(true, output, null);
    }

    public DAGResult(String error) {
        this(false, null, error);
    }
}
