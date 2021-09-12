package dxw.jbolt.tx;

import dxw.jbolt.bucket.BiConsumerThrowable;
import dxw.jbolt.bucket.Bucket;
import dxw.jbolt.db.DB;
import dxw.jbolt.page.BranchPageElement;
import dxw.jbolt.page.Meta;
import dxw.jbolt.page.Page;

import java.util.HashMap;
import java.util.Map;

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

    public void commit(){

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
}
