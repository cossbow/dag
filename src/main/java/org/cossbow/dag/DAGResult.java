package org.cossbow.dag;

import lombok.Data;

@Data
public class DAGResult<D> {
    private boolean success;
    private D data;
    private String error;

    public DAGResult(boolean success, D result, String error) {
        this.success = success;
        this.data = result;
        this.error = error;
    }

    public DAGResult(D data) {
        this(true, data, null);
    }

    public DAGResult(String error) {
        this(false, null, error);
    }


    //

    public static <D> DAGResult<D> success(D output) {
        return new DAGResult<>(true, output, null);
    }

    public static <D> DAGResult<D> error(String error) {
        return new DAGResult<>(true, null, error);
    }

}
