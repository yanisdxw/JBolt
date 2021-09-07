package dxw.jbolt.bucket;

import dxw.jbolt.cursor.Cursor;
import dxw.jbolt.exception.*;
import dxw.jbolt.node.Node;
import dxw.jbolt.page.Page;
import dxw.jbolt.tx.Tx;
import dxw.jbolt.util.Pair;
import dxw.jbolt.util.Triple;
import dxw.jbolt.util.Utils;
import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

// Bucket represents a collection of key/value pairs inside the database.
@Data
public class Bucket {
    public Tx tx;                             // the associated transaction
    public Map<String,Bucket> buckets;        // subbucket cache
    public Page page;                         // inline page reference
    public Node rootnode;                     // materialized node for the root page.
    public Map<Long,Node> nodes;              // node cache
    // This is non-persisted across transactions so it must be set in every Tx.
    public double fillPercent;
    public long root;
    public long sequence;

    public static final double DefaultFillPercent = 0.5;
    public static final byte bucketLeafFlag = 0x01;
    public static final int bucketHeaderSize = 0;

    public Bucket(){

    }
    public Bucket(Tx tx){
        Bucket bucket = new Bucket();
        bucket.tx = tx;
        bucket.fillPercent = DefaultFillPercent;
        if(tx.writable){
            bucket.buckets = new HashMap<>();
            bucket.nodes = new HashMap<>();
        }
    }

    public long root(){
        return root;
    }

    public boolean writable(){
        return tx.writable;
    }

    // Cursor creates a cursor associated with the bucket.
    // The cursor is only valid as long as the transaction is open.
    // Do not use a cursor after the transaction is closed.
    public Cursor cursor(){
        // Update transaction statistics.
        tx.stats.cursorCount++;

        Cursor cursor = new Cursor();
        cursor.bucket = this;
        cursor.stack = new ArrayList<>();
        return cursor;
    }

    // Bucket retrieves a nested bucket by name.
    // Returns nil if the bucket does not exist.
    // The bucket instance is only valid for the lifetime of the transaction.
    public Bucket bucket(byte[] name) throws Exception {
        if(buckets!=null){
            Bucket child = buckets.get(new String(name));
            if(child!=null){
                return child;
            }
        }
        Cursor c = cursor();
        Triple<byte[],byte[],Integer> triple = c.seek(name);
        if(Utils.compareBytes(name,triple.left)==0||(triple.right&bucketLeafFlag)==0){
            return null;
        }
        Bucket child = openBucket(triple.left);
        if(buckets!=null){
            buckets.put(new String(name),child);
        }
        return child;
    }

    public Bucket openBucket(byte[] key){
        Bucket child = new Bucket(tx);
        // If unaligned load/stores are broken on this arch and value is
        // unaligned simply clone to an aligned byte array.
        return child;
    }

    public Bucket createBucket(byte[] key) throws Exception {
        if(tx.db==null){
            throw new ErrTxClose();
        }else if(!tx.writable){
            throw new ErrTxNotWritable();
        }else if(key.length==0){
            throw new ErrBucketNameRequired();
        }
        Cursor c = cursor();
        Triple<byte[],byte[],Integer> triple = c.seek(key);
        if(Arrays.equals(triple.left,key)){
            if((triple.right&bucketLeafFlag)!=0){
                throw new ErrBucketExists();
            }
            throw new ErrIncompatibleValue();
        }
        Bucket bucket = new Bucket();
        bucket.rootnode = new Node(true);
        bucket.fillPercent = DefaultFillPercent;
        byte[] value = bucket.write();
        c.node().put(key,key,value,0,bucketLeafFlag);
        page = null;
        return bucket(key);
    }

    public Pair<Page,Node> pageNode(long id) throws Exception {
        if(root==0){
            if(id!=0){
                throw new Exception(String.format("inline bucket non-zero page access(2): %d != 0", id));
            }
            if(rootnode!=null){
                return Pair.of(null,rootnode);
            }
            return Pair.of(page,null);
        }
        if(nodes!=null){
            Node node = nodes.get(id);
            if(node!=null){
                return Pair.of(null,node);
            }
        }
        return Pair.of(tx.page(id),null);
    }

    public Node node(long pgid, Node parent) throws Exception {
        if(nodes==null){
            throw new Exception(String.format("nodes map expected"));
        }
        Node n = nodes.get(pgid);
        if(n!=null){
            return n;
        }
        n = new Node();
        n.bucket = this;
        n.parent = parent;
        if(parent==null){
            rootnode = n;
        }else {
            parent.children.add(n);
        }
        Page p = page;
        if(p==null){
            p = tx.page(pgid);
        }
        n.read(p);
        nodes.put(pgid,n);
        tx.stats.nodeCount++;
        return n;
    }

    // write allocates and writes a bucket to a byte slice.
    public byte[] write(){
        Node n = rootnode;
        byte[] value = new byte[bucketHeaderSize+n.size()];
        //todo: Write a bucket header.

        //todo: Convert byte slice to a fake page and write the root node.

        //n.write(p);
        return value;
    }

}
