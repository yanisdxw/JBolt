package dxw.jbolt.page;

import dxw.jbolt.db.Meta;

import java.util.ArrayList;
import java.util.List;

import static dxw.jbolt.util.Consts.*;

public class Page {

    //pageHeader;
    public long id; // pageid
    public short flags; // 这块内容标识：可以为元数据、空闲列表、树枝、叶子 这四种中的一种
    public short count; // 存储数据的数量
    public int overflow; // 溢出的数量(一页放不下，需要多页)

    //pageData;
    public byte[] data; //数据块,存放node

    //ptr
    public int offset; // 真正存储数据的指向（仅内存中的标识，并不落盘）

    public Page(){

    }

    public Page(byte[] buf){
        this.data = buf;
        this.offset = 0;
    }

    public Page(byte[] buf, int offset){
        this.data = buf;
        this.offset = offset;
    }

    public void setFlags(int flags){
        this.flags = (short) flags;
        writeShort(data,8,flags);
    }

    public void setCount(int count){
        this.count = (short) count;
        this.offset = leafPageElementSize*count;
        writeShort(data,10,count);
    }

    public void setId(long id){
        this.id = id;
        writeLong(data,0,id);
    }
    // typ returns a human readable page type string used for debugging.
    public String type(){
        return "";
    }

    // leafPageElement retrieves the leaf node by index
    public LeafPageElement getLeafPageElement(int index){
        byte[] element = readBytes(leafPageElementSize*index,leafPageElementSize);
        LeafPageElement leafPageElement = new LeafPageElement();
        leafPageElement.flags = readInt(element,0);
        leafPageElement.pos = readInt(element,4);
        leafPageElement.ksize = readInt(element,8);
        leafPageElement.vsize = readInt(element,12);
        leafPageElement.buf = data;
        leafPageElement.index = index;
        return leafPageElement;
    }

    // write leafPageElement to page
    public void setLeafPageElement(int index, LeafPageElement leafPageElement){
        writeInt(data, index*leafPageElementSize, leafPageElement.flags);
        writeInt(data, index*leafPageElementSize+4,leafPageElement.pos);
        writeInt(data, index*leafPageElementSize+8,leafPageElement.ksize);
        writeInt(data, index*leafPageElementSize+12,leafPageElement.vsize);
    }

    // leafPageElements retrieves a list of leaf nodes.
    public List<LeafPageElement> getLeafPageElements(){
        List<LeafPageElement> rst = new ArrayList<>();
        if(count==0) return rst;
        for (int i = 0; i < count; i++) {
            LeafPageElement leafPageElement = getLeafPageElement(i);
            rst.add(leafPageElement);
        }
        return rst;
    }

    // branchPageElement retrieves the branch node by index
    public BranchPageElement getBranchPageElement(int index){
        byte[] element = readBytes(branchPageElementSize*index,branchPageElementSize);
        BranchPageElement leafPageElement = new BranchPageElement();
        leafPageElement.pos = readInt(element,0);
        leafPageElement.ksize = readInt(element,4);
        leafPageElement.pgid = readLong(element,8);
        return leafPageElement;
    }

    //
    public void setBranchPageElement(int index, BranchPageElement branchPageElement){
        writeInt(data, index*leafPageElementSize+4,branchPageElement.pos);
        writeInt(data, index*leafPageElementSize+8,branchPageElement.ksize);
        writeLong(data, index*leafPageElementSize+12,branchPageElement.pgid);
    }

    // branchPageElements retrieves a list of branch nodes.
    public List<BranchPageElement> getBranchPageElements(){
        List<BranchPageElement> rst = new ArrayList<>();
        if(count==0) return rst;
        for (int i = 0; i < count; i++) {
            BranchPageElement branchPageElement = getBranchPageElement(i);
            rst.add(branchPageElement);
        }
        return rst;
    }

    //
    public void setFreeList(int index, long pgid){
        writeLong(data, 16+index*freelistPageElementSize, pgid);
    }

    public long[] getPgids(int from, int to){
        long[] ids = new long[to-from];
        for (int i = from; i < to; i++) {
            long id = readLong(data,16+freelistPageElementSize*i);
            ids[i] = id;
        }
        return ids;
    }

    public Meta getMeta(int index){
        return new Meta(index);
    }

    public void setMeta(Meta meta){

    }

    //// dump writes n bytes of the page to STDERR as hex output.
    public void hexdump(int n){

    }

    // 读取指定位置指定长度的length
    public byte[] readBytes(int position, int length) {
        final byte[] b = this.data;
        byte[] result = new byte[length];
        // 拷贝原数据到新的byte中
        System.arraycopy(b, position, result, 0, length);
        return result;
    }

    private void writeShort(byte[] b, int offset, int i){
        b[offset++] = (byte) (i &0xff);
        b[offset++] = (byte) (i>>>8);
    }

    private short readShort(byte[] b, int offset){
        short i = (short) (b[offset++] & 0xff);
        i |= (b[offset++] & 0xff)<<8;
        return i;
    }

    private void writeInt(byte[] buffer, int offset, int i) {
        buffer[offset++] = (byte) (i & 0xff);
        buffer[offset++] = (byte) (i >>> 8);
        buffer[offset++] = (byte) (i >>> 16);
        buffer[offset++] = (byte) (i >>> 24);
    }

    private int readInt(byte[] b, int offset) {
        int i = b[offset++] & 0xff;
        i |= (b[offset++] & 0xff) << 8;
        i |= (b[offset++] & 0xff) << 16;
        i |= (b[offset++] & 0xff) << 24;
        return i;
    }

    private void writeLong(byte[] buffer, int offset, long i) {
        buffer[offset++] = (byte) (i & 0xff);
        buffer[offset++] = (byte) (i >>> 8);
        buffer[offset++] = (byte) (i >>> 16);
        buffer[offset++] = (byte) (i >>> 24);
        buffer[offset++] = (byte) (i >>> 32);
        buffer[offset++] = (byte) (i >>> 40);
        buffer[offset++] = (byte) (i >>> 48);
        buffer[offset++] = (byte) (i >>> 56);
    }

    public long readLong(byte[] b, int offset) {
        long l = (b[offset++] & 0xff);
        l |= (long) (b[offset++] & 0xff) << 8;
        l |= (long) (b[offset++] & 0xff) << 16;
        l |= (long) (b[offset++] & 0xff) << 24;
        l |= (long) (b[offset++] & 0xff) << 32;
        l |= (long) (b[offset++] & 0xff) << 40;
        l |= (long) (b[offset++] & 0xff) << 48;
        l |= (long) (b[offset++] & 0xff) << 56;
        return l;
    }
}
