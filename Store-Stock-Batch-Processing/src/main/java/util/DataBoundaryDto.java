package util;

public class DataBoundaryDto {
    private int minBound ;
    private int maxBound ;

    public DataBoundaryDto(int minBound, int maxBound) {
        this.minBound = minBound;
        this.maxBound = maxBound;
    }

    public int getMinBound() {
        return minBound;
    }

    public void setMinBound(int minBound) {
        this.minBound = minBound;
    }

    public int getMaxBound() {
        return maxBound;
    }

    public void setMaxBound(int maxBound) {
        this.maxBound = maxBound;
    }
}
