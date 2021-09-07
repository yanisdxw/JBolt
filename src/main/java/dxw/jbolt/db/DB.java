package dxw.jbolt.db;

import dxw.jbolt.bucket.Bucket;
import dxw.jbolt.exception.ErrDatabaseNotOpen;
import dxw.jbolt.exception.ErrDatabaseReadOnly;
import dxw.jbolt.page.FreeList;
import dxw.jbolt.page.Page;
import dxw.jbolt.tx.Tx;
import dxw.jbolt.tx.TxFunc;
import dxw.jbolt.util.Consts;
import dxw.jbolt.util.IOUtils;
import dxw.jbolt.util.Utils;
import lombok.SneakyThrows;

import java.io.File;
import java.io.RandomAccessFile;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static dxw.jbolt.node.Node.*;

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
public class DB {

    public boolean strictMode;

    public boolean noSync;

    public boolean noGrowSync;

    public int mmapFlags;

    public int maxBatchSize;

    public int maxBatchDelay;

    public int allocSize;

    public String path;
    public File file;
    public File lockFile;
    public byte[] dataref;
    public byte[] data;
    public int datasz;
    public int filesz;
    public Meta meta00;
    public Meta meta01;
    public int pageSize;
    public boolean opened;
    public Tx rwtx;
    public List<Tx> txs;
    public FreeList freeList;
    public Stats stats;

    public Lock batchMu;
    public Batch batch;
//
    public ReentrantLock rwlock;     // Allows only one writer at a time.
    public ReentrantLock metalock;   // Protects meta page access.
    public ReentrantReadWriteLock mmaplock; // Protects mmap access during remapping.
    public ReentrantReadWriteLock statlock; // Protects stats access.

    public boolean readOnly;


    public static int DefaultMaxBatchSize  = 1000;
    public static int DefaultMaxBatchDelay = 10;
    public static int DefaultAllocSize     = 16 * 1024 * 1024;

    public String path(){
        return path;
    }

    @Override
    public String toString(){
        return String.format("DB{path:%s}",path);
    }

    public DB Open(String path, String fileMode, Option option) throws Exception {
        DB db = new DB();
        db.opened = true;
        // Set default options if no options are provided.
        if(option==null){
            option = new Option(0,false);
        }
        db.noGrowSync = option.noGrowSync;
        db.mmapFlags = option.mmapFlags;
        // Set default values for later DB operations.
        db.maxBatchSize = DefaultMaxBatchSize;
        db.maxBatchDelay = DefaultMaxBatchDelay;
        db.allocSize = DefaultAllocSize;

        String flag = Consts.O_RDWR;
        if(option.readOnly){
            flag = Consts.O_RDONLY;
            db.readOnly = true;
        }
        // Open data file and separate sync handler for metadata writes.
        db.path = path;
        db.file = new File(path);
        // Initialize the database if it doesn't exist.
        if(db.file==null){

        }

        return db;
    }

    // init creates a new database file and initializes its meta pages.
    public void init(DB db){
        // Set the page size to the OS page size.
        db.pageSize = Utils.getOsPageSize();
        // Create two meta pages on a buffer.
        byte[] buf = new byte[db.pageSize*4];
        Page p;
        for (int i = 0; i < 2; i++) {
            p = pageInBuffer(buf,i);
            p.setId(i);
            p.setFlags(metaPageFlag);
            // Initialize the meta page.
            Meta m = new Meta(i);
            m.magic = Consts.magic;
            m.version = Consts.version;
            m.pageSize = db.pageSize;
            m.freelist = 2;
            m.root = new Bucket();
            m.pgid = 4;
            m.txid = i;
            //m.checksum;
        }
        // Write an empty freelist at page 3.
        p = db.pageInBuffer(buf,2);
        p.setId(2);
        p.setFlags(freelistPageFlag);
        p.setCount(0);
        // Write an empty leaf page at page 4.
        p = db.pageInBuffer(buf,3);
        p.setId(3);
        p.setFlags(leafPageFlag);
        p.setCount(0);
        // Write the buffer to our data file.
        writeAt(buf);
    }

    // pageInBuffer retrieves a page reference from a given byte array based on the current page size.
    private Page pageInBuffer(byte[] buf, long id){
        return new Page(buf);
    }

    private void writeAt(byte[] b){
        IOUtils.write(file, b);
    }

    private void readAt(){
        byte[] b = IOUtils.read(file);

    }

    public void update(TxFunc txFunc) throws Exception {
        Tx t = begin(true);
        t.managed = true;
        try {
            txFunc.run(t);
        } catch (Exception e) {
            t.rollback();
        } finally {
            t.commit();
            t.managed = false;
        }
    }

    public void view(TxFunc txFunc) throws Exception {
        Tx t = begin(false);
        t.managed = true;
        try {
            txFunc.run(t);
        }catch (Exception e){
            t.rollback();
        }finally {
            t.managed = false;
        }
    }

    public Tx begin(boolean writable) throws Exception {
        if(writable){
            return beginRWTx();
        }
        return beginTx();
    }

    private Tx beginTx(){
        metalock.lock();
        mmaplock.readLock().lock();
        if(opened){
            mmaplock.readLock().unlock();;
            metalock.unlock();
        }
        Tx t = new Tx();
        t.init(this);
        txs.add(t);
        metalock.unlock();
        statlock.readLock();
        stats.OpenTxN = txs.size();
        return t;
    }

    public Tx beginRWTx() throws Exception {
        if(readOnly){
            throw new ErrDatabaseReadOnly();
        }
        rwlock.lock();
        metalock.lock();
        if(!opened){
            rwlock.unlock();
            throw new ErrDatabaseNotOpen();
        }
        Tx t = new Tx(true);
        t.init(this);
        rwtx = t;
        long minid = Long.MAX_VALUE;
        for(Tx tx:txs){
            if(tx.meta.txid<minid){
                minid = t.meta.txid;
            }
        }
        if(minid>0){
            freeList.release(minid-1);
        }
        metalock.unlock();
        return t;
    }

    public Page page(long id){
        return new Page();
    }

}
