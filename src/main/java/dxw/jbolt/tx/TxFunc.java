package dxw.jbolt.tx;

@FunctionalInterface
public interface TxFunc {
    void run(Tx tx) throws Exception;
}
