package dxw.jbolt.page;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static dxw.jbolt.util.Consts.*;
import static dxw.jbolt.util.IOUtils.*;

public class Page implements BytesData {

    //pageHeader;
    public long id; // pageid
    public short flags; // 这块内容标识：可以为元数据、空闲列表、树枝、叶子 这四种中的一种
    public short count; // 存储数据的数量
    public int overflow; // 溢出的数量(一页放不下，需要多页)

    //pageData;
    public byte[] data; //数据块,存放node
    public int offset; // 真正存储数据的指向（仅内存中的标识，并不落盘）
    //ptr
    public int ptr; //在buf中的起始位置

    public Page(){

    }

    public Page(byte[] buf){
        this.data = buf;
    }

    public Page(byte[] buf, int ptr){
        this.data = buf;
        this.offset = ptr;
    }

    public static Page getPage(byte[] buf){
        Page page = new Page(buf);
        page.id = readLong(buf,0);
        page.flags = readShort(buf,8);
        page.count = readShort(buf,10);
        page.overflow = readInt(buf,12);
        page.data = buf;
        page.offset = pageHeaderSize;
        return page;
    }

    public void setFlags(int flags){
        this.flags = (short) flags;
        writeShort(data,ptr+8,flags);
    }

    public void setCount(int count){
        this.count = (short) count;
        this.offset = leafPageElementSize*count + pageHeaderSize;
        writeShort(data,ptr+10,count);
    }

    public void setId(long id){
        this.id = id;
        writeLong(data, ptr, id);
    }
    // typ returns a human readable page type string used for debugging.
    public String type(){
        return "";
    }

    // leafPageElement retrieves the leaf node by index
    public LeafPageElement getLeafPageElement(int index){
        byte[] element = readBytes(ptr+pageHeaderSize+leafPageElementSize*index,leafPageElementSize);
        LeafPageElement leafPageElement = new LeafPageElement();
        leafPageElement.flags = readInt(element,0);
        leafPageElement.pos = readInt(element,4);
        leafPageElement.ksize = readInt(element,8);
        leafPageElement.vsize = readInt(element,12);
        leafPageElement.setBufData(data);
        leafPageElement.setIndex(index);
        return leafPageElement;
    }

    // write leafPageElement to page
    public void setLeafPageElement(int index, LeafPageElement leafPageElement){
        writeInt(data, ptr+pageHeaderSize+index*leafPageElementSize, leafPageElement.flags);
        writeInt(data, ptr+pageHeaderSize+index*leafPageElementSize+4,leafPageElement.pos);
        writeInt(data, ptr+pageHeaderSize+index*leafPageElementSize+8,leafPageElement.ksize);
        writeInt(data, ptr+pageHeaderSize+index*leafPageElementSize+12,leafPageElement.vsize);
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
        byte[] element = readBytes(ptr+branchPageElementSize*index,branchPageElementSize);
        BranchPageElement leafPageElement = new BranchPageElement();
        leafPageElement.pos = readInt(element,0);
        leafPageElement.ksize = readInt(element,4);
        leafPageElement.pgid = readLong(element,8);
        return leafPageElement;
    }

    //
    public void setBranchPageElement(int index, BranchPageElement branchPageElement){
        writeInt(data, ptr+pageHeaderSize+index*leafPageElementSize+4,branchPageElement.pos);
        writeInt(data, ptr+pageHeaderSize+index*leafPageElementSize+8,branchPageElement.ksize);
        writeLong(data, ptr+pageHeaderSize+index*leafPageElementSize+12,branchPageElement.pgid);
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
        writeLong(data, ptr+pageHeaderSize+index*freelistPageElementSize, pgid);
    }

    public List<Long> getPgids(int from, int to){
        long[] ids = new long[to-from];
        for (int i = from; i < to; i++) {
            long id = readLong(data,ptr+pageHeaderSize+freelistPageElementSize*i);
            ids[i] = id;
        }
        return Arrays.stream(ids).boxed().collect(Collectors.toList());
    }

    //从字节数组中读取meta数据
    public Meta getMeta(){
        byte[] data = readBytes(ptr+pageHeaderSize,pageSize-pageHeaderSize);
        Meta meta = new Meta();
        meta.magic = readInt(data,0);
        meta.version = readInt(data,4);
        meta.pageSize = readInt(data,8);
        meta.flag = readInt(data,12);
        meta.root = readLong(data, 16);
        meta.sequence = readLong(data, 24);
        meta.freelist = readLong(data,32);
        meta.pgid = readLong(data,40);
        meta.txid = readLong(data,48);
        meta.checksum = readInt(data,56);
        return meta;
    }

    //将meta数据写入数组中
    public void setMeta(Meta meta){
        writeInt(data, ptr+pageHeaderSize, meta.magic);
        writeInt(data, ptr+pageHeaderSize+4,meta.version);
        writeInt(data,ptr+pageHeaderSize+8,meta.pageSize);
        writeInt(data,ptr+pageHeaderSize+12,meta.flag);
        writeLong(data,ptr+pageHeaderSize+16,meta.root);
        writeLong(data,ptr+pageHeaderSize+24,meta.sequence);
        writeLong(data,ptr+pageHeaderSize+32,meta.freelist);
        writeLong(data,ptr+pageHeaderSize+40,meta.pgid);
        writeLong(data,ptr+pageHeaderSize+48,meta.txid);
        writeLong(data,ptr+pageHeaderSize+56,meta.checksum);
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

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }
}
