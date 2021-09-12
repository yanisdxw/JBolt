package dxw.jbolt.tx;

import dxw.jbolt.bucket.BiConsumerThrowable;
import dxw.jbolt.bucket.Bucket;
import dxw.jbolt.db.DB;
import dxw.jbolt.exception.ErrTxClose;
import dxw.jbolt.exception.ErrTxNotWritable;
import dxw.jbolt.page.BranchPageElement;
import dxw.jbolt.page.Meta;
import dxw.jbolt.page.Page;

import java.util.*;

import static dxw.jbolt.node.Node.branchPageFlag;

public class Tx {
    public boolean writable;
    public boolean managed;
    public DB db;
    public Meta meta;
    public Bucket root;
    public Map<Long, Page> pages;
    public TxStats stats;

    public int writeFlag;

    public Tx(){

    }

    public Tx(boolean writable){
        this.writable = writable;
    }

    public void init(DB db) {
        this.db = db;
        pages = null;
        meta = db.meta();
        root = new Bucket();
        root.root = meta.pgid;
        if(writable){
            pages = new HashMap<>();
            meta.txid += 1;
        }
    }

    public void commit() throws Exception {
        if(!managed){
            throw new Exception("managed tx commit not allowed");
        }
        if(db==null){
            throw new ErrTxClose();
        }else if(!writable){
            throw new ErrTxNotWritable();
        }
        long startTime = new Date().getTime();
        root.reblance();
        if(stats.rebalance>0){
            stats.rebalanceTime += new Date().getTime()-startTime;
        }
        startTime = new Date().getTime();
        try {
            root.spill();
        }catch (Exception e){
            rollback();
        }
        stats.spillTime += new Date().getTime()-startTime;
        meta.root = root.root;
        long opgid = meta.pgid;
        db.freeList.free(meta.txid, db.page(meta.freelist));
        Page p = null;
        try {
            p = allocate(db.freeList.size()/db.pageSize+1);
        }catch (Exception e){
            rollback();
            return;
        }
        try {
            db.freeList.write(p);
        }catch (Exception e){
            rollback();
            return;
        }
        meta.freelist = p.id;
        if(meta.pgid>opgid){
            try {
                db.grow((int)(meta.pgid+1)*db.pageSize);
            }catch (Exception e){
                rollback();
                return;
            }
        }

        // Write dirty pages to disk.
        startTime = new Date().getTime();
        try {

        }catch (Exception e){
            rollback();
        }
    }

    public void rollback(){

    }

    public void close(){

    }

    public Page page(long id){
        if(pages!=null){
            Page page = pages.get(id);
            if(page!=null){
                return page;
            }
        }
        return db.page(id);
    }

    public void forEachPage(long pgid, int depth, BiConsumerThrowable<Page, Integer> fn) throws Exception {
        Page p = page(pgid);
        fn.accept(p,depth);
        if((p.flags & branchPageFlag)!=0){
            for (int i = 0; i < p.count; i++) {
                BranchPageElement element = p.getBranchPageElement(i);
                forEachPage(element.pgid, depth+1, fn);
            }
        }
    }

    public Bucket createBUcket(byte[] name) throws Exception {
        return this.root.createBUcket(name);
    }

    public Bucket Bucket(byte[] name) throws Exception {
        return root.Bucket(name);
    }

    public Page allocate(int count) throws Exception {
        Page p = db.allocate(count);
        pages.put(p.id,p);
        stats.pageCount++;
        stats.pageAlloc += count*db.pageSize;
        return p;
    }

    public void write(){
        List<Page> pages = new ArrayList<>();
        for(Map.Entry<Long,Page> entry:this.pages.entrySet()){
            pages.add(entry.getValue());
        }
        this.pages = new HashMap<>();
        Collections.sort(pages, new Comparator<Page>() {
            @Override
            public int compare(Page o1, Page o2) {
                return (int)(o1.id - o2.id);
            }
        });
        for(Page p:pages){
            int size = (p.overflow+1)*db.pageSize;
            int offset = (int) p.id * db.pageSize;

        }
    }
}
