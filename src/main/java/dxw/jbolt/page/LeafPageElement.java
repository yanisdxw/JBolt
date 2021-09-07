package dxw.jbolt.page;


import static dxw.jbolt.util.Consts.leafPageElementSize;

/**
 * 通过 pos、ksize 和 vsize 获取对应的 key/value 的地址：
 * &leafPageElement + pos == &key。
 * &leafPageElement + pos + ksize == &val
 */
public class LeafPageElement {
    public int pos; // key 距离 leafPageElement 的位移
    public int ksize;
    public int vsize;
    public int flags; // 通过 flags 区分 subbucket 和普通 value

    public byte[] buf;
    public int index;

    public LeafPageElement(){

    }

    public LeafPageElement(int flags, int pos, int ksize, int vsize){
        this.flags = flags;
        this.pos = pos;
        this.ksize = ksize;
        this.vsize = vsize;
    }

    // leafPageElement represents a node on a leaf page.
    public byte[] key(){
        final byte[] result = new byte[ksize];
        System.arraycopy(buf, leafPageElementSize*index + pos, result,0,ksize);
        return result;
    }

    // value returns a byte slice of the node value.
    public byte[] value(){
        final byte[] result = new byte[vsize];
        System.arraycopy(buf,leafPageElementSize*index + pos + ksize,result,0,vsize);
        return result;
    }
}
