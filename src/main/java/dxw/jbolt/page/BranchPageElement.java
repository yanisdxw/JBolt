package dxw.jbolt.page;

/**
 * 和 leafnode 的区别是：branchnode 的 value 是子节点的 page id，
 * 存放在 branchPageElement 里，而key的存储相同都是通过pos得到：
 */
public class BranchPageElement {
    public int pos;
    public int ksize;
    public long pgid;

    private byte[] buf;
    private int index;

    // key returns a byte slice of the node key.
    public byte[] key(){
        return new byte[]{};
    }


}
