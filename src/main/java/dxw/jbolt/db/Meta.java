package dxw.jbolt.db;

import dxw.jbolt.bucket.Bucket;

public class Meta {
    public int magic; // 标识db文件为boltdb产生的
    public int version; // 版本号
    public int pageSize; // 页大小，根据系统获得，一般为4k
    public int flag; // 表示为metadata
    public Bucket root; // 内含根节点的pageid，起始时从3开始
    public long pgid; // 空闲列表pageid，起始时从2开始
    public long freelist; // 下一个要分配的pageid
    public long txid; // 下一个要分配的事务id
    public long checksum; // 检查meta完整性时使用

    public Meta(long pgid){
        pgid = pgid;
    }

    public long sum64(){
        return 0;
    }
}
