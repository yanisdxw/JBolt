package dxw.jbolt.tx;

import dxw.jbolt.bucket.Bucket;
import dxw.jbolt.db.DB;
import dxw.jbolt.db.Meta;
import dxw.jbolt.page.Page;

import java.util.Map;

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

    public void init(DB db){

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
}
