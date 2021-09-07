package dxw.jbolt.tx;

public class TxStats {
    public int pageCount;
    public int pageAlloc;

    public int cursorCount;

    public int nodeCount;
    public int nodeDeref;

    public int rebalance;
    public long rebalanceTime;

    public int split;
    public int spill;
    public int spillTime;

    public int write;
    public long writeTime;
}
