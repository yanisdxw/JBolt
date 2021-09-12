package dxw.jbolt.bucket;

import dxw.jbolt.cursor.Cursor;
import dxw.jbolt.exception.*;
import dxw.jbolt.node.Node;
import dxw.jbolt.page.BranchPageElement;
import dxw.jbolt.page.Page;
import dxw.jbolt.tx.Tx;
import dxw.jbolt.util.IOUtils;
import dxw.jbolt.util.Pair;
import dxw.jbolt.util.Triple;
import dxw.jbolt.util.Utils;
import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static dxw.jbolt.node.Node.branchPageFlag;
import static dxw.jbolt.util.Consts.leafPageElementSize;
import static dxw.jbolt.util.Consts.pageHeaderSize;

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
    // bucket represents the on-file representation of a bucket.
    public long root;
    public long sequence;

    public static final double DefaultFillPercent = 0.5;
    public static final byte bucketLeafFlag = 0x01;
    public static final int bucketHeaderSize = 0;
    public static final int MaxKeySize = 32768;
    public static final int MaxValueSize = (1 << 31) - 2;

    public Bucket(){

    }

    public Bucket Bucket(byte[] name) throws Exception {
        if(buckets!=null){
            Bucket child = buckets.get(new String(name));
            if(child!=null){
                return child;
            }
        }
        Cursor c = cursor();
        Triple<byte[],byte[],Integer> triple = c.seek(name);
        if(!Arrays.equals(name,triple.left)||(triple.right&bucketLeafFlag)==0){
            return null;
        }
        Bucket child = openBucket(triple.middle);
        if(!buckets.isEmpty()){
            buckets.put(new String(name),child);
        }
        return child;
    }

    public Bucket createBUcket(byte[] key) throws Exception {
        if(tx.db==null){
            throw new ErrTxClose();
        }else if(!tx.writable){
            throw new ErrTxNotWritable();
        }else if(key.length==0){
            throw  new ErrBucketNameRequired();
        }
        Cursor c = cursor();
        Triple<byte[],byte[],Integer> triple = c.seek(key);
        if(Arrays.equals(key,triple.left)){
            if((triple.right & bucketLeafFlag)!=0){
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

    public Bucket createBucketIfNotExists(byte[] key) throws Exception {
        Bucket child;
        try {
            child = createBucket(key);
        } catch (ErrBucketExists e) {
            return Bucket(key);
        } catch (Exception e){
            throw e;
        }
        return child;
    }

    public void deleteBucket(byte[] key) throws Exception {
        if(tx.db==null){
            throw new ErrTxClose();
        }else if(!writable()){
            throw new ErrTxNotWritable();
        }
        Cursor c = cursor();
        Triple<byte[],byte[],Integer> triple = c.seek(key);
        if(!Arrays.equals(key,triple.left)){
            throw new ErrBucketNotFound();
        } else if((triple.right & bucketLeafFlag)==0){
            throw new ErrIncompatibleValue();
        }
        Bucket child = bucket(key);
        child.forEach((k,v)->{
            if(v==null){
                try {
                    child.deleteBucket((byte[]) k);
                } catch (Exception e) {
                    throw new Exception(String.format("delete bucket: %s", e.getMessage()));
                }
            }
        });
        buckets.remove(new String(key));
        child.nodes = null;
        child.rootnode = null;
        //child.
    }

    public byte[] get(byte[] key) throws Exception {
        Triple<byte[],byte[],Integer> triple = cursor().seek(key);
        if((triple.right & bucketLeafFlag)!=0){
            return null;
        }
        if(Arrays.equals(key,triple.left)){
            return null;
        }
        return triple.middle;
    }

    public void put(byte[] key, byte[] value) throws Exception {
        if(tx.db==null){
            throw new ErrTxClose();
        }else if(!writable()){
            throw new ErrTxNotWritable();
        }else if(key.length==0){
            throw new ErrKeyRequired();
        }else if(key.length>MaxKeySize){
            throw new ErrKeyTooLarge();
        }else if(value.length>MaxValueSize){
            throw new ErrValueTooLarge();
        }
        Cursor c = cursor();
        Triple<byte[],byte[],Integer> triple = c.seek(key);
        if(Arrays.equals(key,triple.left) && (triple.right&bucketLeafFlag)!=0){
            throw new ErrIncompatibleValue();
        }
        c.node().put(key,key,value,0,0);
    }

    public void delete(byte[] key) throws Exception {
        if(tx.db==null){
            throw new ErrTxClose();
        }else if(!writable()){
            throw new ErrTxNotWritable();
        }
        Cursor c = cursor();
        Triple<byte[],byte[],Integer> triple = c.seek(key);
        if((triple.right&bucketLeafFlag)!=0){
            throw new ErrIncompatibleValue();
        }
        c.node().del(key);
    }

    public void setSequnce(long v) throws Exception {
        if(tx.db==null){
            throw new ErrTxClose();
        }else if(!writable()){
            throw new ErrTxNotWritable();
        }
        // Materialize the root node if it hasn't been already so that the
        // bucket will be saved during commit.
        if(rootnode==null){
            node(root,null);
        }
        sequence = v;
    }

    public long nextSequence() throws Exception {
        if(tx.db==null){
            throw new ErrTxClose();
        }else if(!writable()){
            throw new ErrTxNotWritable();
        }
        if(rootnode==null){
            node(root,null);
        }
        return sequence++;
    }

    public void forEach(BiConsumerThrowable fn) throws Exception {
        if(tx.db==null){
            throw new ErrTxClose();
        }
        Cursor c = cursor();
        Pair<byte[],byte[]> pair = c.First();
        while (pair.left!=null){
            fn.accept(pair.left,pair.right);
            pair = c.Next();
        }
        return;
    }

    public void forEachPage(BiConsumerThrowable<Page,Integer> fn) throws Exception {
        if(page!=null){
            fn.accept(page,0);
        }
        tx.forEachPage(root,0,fn);
    }

    public void forEachPageNode(TriConsumerThrowable<Page,Node,Integer> fn) throws Exception {
        if(page!=null){
            fn.accept(page,null,0);
        }
        _forEachPageNode(root,0,fn);
    }

    public void _forEachPageNode(long pgid, int depth, TriConsumerThrowable<Page, Node, Integer> fn) throws Exception {
        Pair<Page,Node> pair = pageNode(pgid);
        Page p = pair.left;
        Node n = pair.right;
        fn.accept(p,n,depth);
        if(pair.left!=null){
            if((p.flags & branchPageFlag)!=0){
                for (int i = 0; i < p.count; i++) {
                    BranchPageElement element = p.getBranchPageElement(i);
                    _forEachPageNode(element.pgid,depth+1,fn);
                }
            }
        }else {
            if(!n.isLeaf){
                for(Node.iNode inode:n.inodes){
                    _forEachPageNode(inode.pgid,depth+1,fn);
                }
            }
        }
    }

    // spill writes all the nodes for this bucket to dirty pages.
    public void spill() throws Exception {
        for(Map.Entry<String,Bucket> entry:buckets.entrySet()){
            String name = entry.getKey();
            Bucket child = entry.getValue();
            byte[] value;
            if(child.inlineable()){
                child.free();
                value = child.write();
            }else {
                // Update the child bucket header in this bucket.
                child.spill();
                value = new byte[16];
                IOUtils.writeLong(value,0,child.root);
                IOUtils.writeLong(value,8,child.sequence);
            }
            if(child.rootnode==null){
                continue;
            }
            Cursor c = cursor();
            Triple<byte[],byte[],Integer> triple = c.seek(name.getBytes());
            if(Arrays.equals(name.getBytes(),triple.left)){
                throw new Exception(String.format("misplaced bucket header: %x -> %x", name.getBytes(), triple.left));
            }
            if((triple.right&bucketLeafFlag)==0){
                throw new Exception(String.format("unexpected bucket header flag: %x",triple.right));
            }
            c.node().put(name.getBytes(),name.getBytes(),value,0,bucketLeafFlag);
        }
        if(rootnode==null){
            return;
        }
        rootnode.spill();
        rootnode = rootnode.root();
        if(rootnode.pgid>=tx.meta.pgid){
            throw new Exception(String.format("pgid (%d) above high water mark (%d)",rootnode.pgid,tx.meta.pgid));
        }
        root = rootnode.pgid;
    }

    private boolean inlineable(){
        Node n = rootnode;
        if(n==null || !n.isLeaf){
            return false;
        }
        int size = pageHeaderSize;
        for(Node.iNode inode:n.inodes){
            size += leafPageElementSize + inode.key.length + inode.value.length;
            if((inode.flags & bucketLeafFlag)!=0){
                return false;
            }else if(size>maxInlineBucketSize()){
                return false;
            }
        }
        return true;
    }

    private int maxInlineBucketSize(){
        return tx.db.pageSize/4;
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
    public byte[] write() throws Exception {
        Node n = rootnode;
        byte[] value = new byte[bucketHeaderSize+n.size()];
        //todo: Write a bucket header.
        //IOUtils.write();
        //todo: Convert byte slice to a fake page and write the root node.
        Page p = new Page();
        //IOUtils.read();
        n.write(p);
        return value;
    }

    public void reblance() throws Exception {
        for(Map.Entry<Long,Node> entry:nodes.entrySet()){
            Node n = entry.getValue();
            n.rebalance();
        }
        for(Map.Entry<String,Bucket> entry:buckets.entrySet()){
            Bucket child = entry.getValue();
            child.reblance();
        }
    }

    public void free(){
        if(root==0){
            return;
        }

    }

    public void dereference(){

    }

}
