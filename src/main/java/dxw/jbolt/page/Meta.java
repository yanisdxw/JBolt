package dxw.jbolt.page;

import dxw.jbolt.bucket.Bucket;
import dxw.jbolt.exception.ErrChecksum;
import dxw.jbolt.exception.ErrInvalid;
import dxw.jbolt.exception.ErrVersionMismatch;
import dxw.jbolt.util.Consts;
import dxw.jbolt.util.IOUtils;

import java.util.Objects;

import static dxw.jbolt.util.IOUtils.*;
import static dxw.jbolt.util.IOUtils.writeLong;

public class Meta implements BytesData {
    public int magic; // 标识db文件为boltdb产生的
    public int version; // 版本号
    public int pageSize; // 页大小，根据系统获得，一般为4k
    public int flag; // 表示为metadata
    public long root; // 内含根节点的pageid，起始时从3开始
    public long sequence; // monotonically incrementing, used by NextSequence()
    public long pgid; // 空闲列表pageid，起始时从2开始
    public long freelist; // 下一个要分配的pageid
    public long txid; // 下一个要分配的事务id
    public int checksum; // 检查meta完整性时使用

    public byte[] buf; //

    public Meta(){

    }

    public Meta(long pgid){
        this.pgid = pgid;
    }

    public int sum64(){
        checksum = Objects.hash(root,freelist,pgid,txid);
        return checksum;
    }

    public void validate() throws Exception {
        if(this.magic!= Consts.magic){
            throw new ErrInvalid();
        }else if(version!=Consts.version){
            throw new ErrVersionMismatch();
        }else if(checksum!=0 && checksum!=sum64()){
            throw new ErrChecksum();
        }
    }

    @Override
    public byte[] toBytes() {
        byte[] data = new byte[64];
        writeInt(data,0,magic);
        writeInt(data,4,version);
        writeInt(data,8,pageSize);
        writeInt(data,12,flag);
        writeLong(data,16,root);
        writeLong(data,24,sequence);
        writeLong(data,32,freelist);
        writeLong(data,40,pgid);
        writeLong(data,48,txid);
        writeInt(data,56,checksum);
        return data;
    }

    public static Meta getMeta(byte[] data){
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


}
