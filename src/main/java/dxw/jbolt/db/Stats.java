package dxw.jbolt.db;

import dxw.jbolt.tx.TxStats;

public class Stats {
    // Freelist stats
    public int FreePageN;     // total number of free pages on the freelist
    public int PendingPageN;  // total number of pending pages on the freelist
    public int FreeAlloc;     // total bytes allocated in free pages
    public int FreelistInuse; // total bytes used by the freelist

    // Transaction stats
    public int TxN;     // total number of started read transactions
    public int OpenTxN; // number of currently open read transactions

    public TxStats TxStats; // global, ongoing stats.
}
