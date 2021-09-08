package dxw.jbolt.page;

import static dxw.jbolt.util.Consts.leafPageElementSize;
import static dxw.jbolt.util.Consts.pageHeaderSize;

/**
 * 通过 pos、ksize 和 vsize 获取对应的 key/value 的地址：
 * &leafPageElement + pos == &key。
 * &leafPageElement + pos + ksize == &val
 */
public class LeafPageElement implements BytesData {

    public int flags; // 通过 flags 区分 subbucket 和普通 value
    public int pos; // key 距离 leafPageElement 的位移
    public int ksize;
    public int vsize;

    private byte[] buf;
    private int index;

    public void setBufData(byte[] data){
        this.buf = data;
    }

    public void setIndex(int index){
        this.index = index;
    }

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
        System.arraycopy(buf, pageHeaderSize+leafPageElementSize*index + pos, result,0,ksize);
        return result;
    }

    // value returns a byte slice of the node value.
    public byte[] value(){
        final byte[] result = new byte[vsize];
        System.arraycopy(buf,pageHeaderSize+leafPageElementSize*index + pos + ksize,result,0,vsize);
        return result;
    }

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }
}
