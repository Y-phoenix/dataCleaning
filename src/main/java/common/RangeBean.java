package common;

/**
 * Created by rchirinos on 4/21/16.
 */
public class RangeBean {
    private int ini;
    private int end;

    public RangeBean(int ini, int end){
        this.ini = ini;
        this.end = end;
    }
    public int getIni() {
        return ini;
    }

    public void setIni(int ini) {
        this.ini = ini;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }
}
