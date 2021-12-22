package org.cossbow.dag;

public class DAGResult<D> {
    private boolean success;
    private D data;
    private String error;

    public DAGResult() {
        this(true, null, null);
    }

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

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public D getData() {
        return data;
    }

    public void setData(D data) {
        this.data = data;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }


    //

    public static <D> DAGResult<D> success(D output) {
        return new DAGResult<>(true, output, null);
    }

    public static <D> DAGResult<D> error(String error) {
        return new DAGResult<>(true, null, error);
    }

}
