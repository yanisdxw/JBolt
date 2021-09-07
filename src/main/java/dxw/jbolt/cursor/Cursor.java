package dxw.jbolt.cursor;

import dxw.jbolt.bucket.Bucket;
import dxw.jbolt.exception.ErrIncompatibleValue;
import dxw.jbolt.exception.ErrTxClose;
import dxw.jbolt.exception.ErrTxNotWritable;
import dxw.jbolt.node.Node;
import dxw.jbolt.page.BranchPageElement;
import dxw.jbolt.page.LeafPageElement;
import dxw.jbolt.page.Page;
import dxw.jbolt.util.Pair;
import dxw.jbolt.util.Triple;
import dxw.jbolt.util.Utils;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static dxw.jbolt.bucket.Bucket.bucketLeafFlag;
import static dxw.jbolt.node.Node.*;

public class Cursor {

    public Bucket bucket;
    public List<ElementRef> stack;

    public Pair<byte[],byte[]> First() throws Exception {
        if(bucket.tx.db==null){
            throw new Exception("tx closed");
        }
        stack = stack.subList(0,1);
        Pair<Page, Node> pair = bucket.pageNode(bucket.root);
        stack.add(new ElementRef(pair.left,pair.right,0));
        if(stack.get(stack.size()-1).count()==0){
            next();
        }
        Triple<byte[],byte[],Integer> triple = keyValue();
        if((triple.right & bucketLeafFlag)!=0){
            return Pair.of(triple.left,null);
        }
        return Pair.of(triple.left, triple.middle);
    }

    private void first() throws Exception {
        while (true){
            ElementRef ref = stack.get(stack.size()-1);
            if(ref.isLeaf()){
                break;
            }
            long pgid;
            if(ref.node!=null){
                pgid = ref.node.inodes.get(ref.index).pgid;
            }else {
                pgid = ref.page.getBranchPageElement(ref.index).pgid;
            }
            Pair<Page,Node> pair = bucket.pageNode(pgid);
            stack.add(new ElementRef(pair.left,pair.right,0));
        }
    }

    public Pair<byte[],byte[]> Last() throws Exception {
        if(bucket.tx.db==null){
            throw new Exception("tx closed");
        }
        stack = stack.subList(0,1);
        Pair<Page, Node> pair = bucket.pageNode(bucket.root);
        ElementRef ref = new ElementRef();
        ref.index = ref.count()-1;
        stack.add(ref);
        last();
        Triple<byte[],byte[],Integer> triple = keyValue();
        if((triple.right & bucketLeafFlag)!=0){
            return Pair.of(triple.left,null);
        }
        return Pair.of(triple.left, triple.middle);
    }

    private void last() throws Exception {
        while (true){
            ElementRef ref = stack.get(stack.size()-1);
            if(ref.isLeaf()){
                break;
            }
            long pgid;
            if(ref.node!=null){
                pgid = ref.node.inodes.get(ref.index).pgid;
            }else {
                pgid = ref.page.getBranchPageElement(ref.index).pgid;
            }
            Pair<Page,Node> pair = bucket.pageNode(pgid);
            ElementRef nextRef = new ElementRef();
            nextRef.page = pair.left;
            nextRef.node = pair.right;
            nextRef.index = nextRef.count()-1;
            stack.add(nextRef);
        }
    }

    public Pair<byte[],byte[]> Next() throws Exception {
        if(bucket.tx.db==null){
            throw new Exception("tx closed");
        }
        Triple<byte[],byte[],Integer> triple = next();
        if((triple.right & bucketLeafFlag)!=0){
            return Pair.of(triple.left,null);
        }
        return Pair.of(triple.left, triple.middle);
    }

    private Triple<byte[],byte[],Integer> next() throws Exception {
        while (true){
            int i;
            for (i=stack.size()-1; i>=0; i--) {
                ElementRef elementRef = stack.get(i);
                if(elementRef.index<elementRef.count()-1){
                    elementRef.index++;
                    break;
                }
            }
            if(i==-1){
                return Triple.of(null,null,0);
            }
            stack = stack.subList(0,i+1);
            first();
            if(stack.get(stack.size()-1).count()==0){
                continue;
            }
            return keyValue();
        }
    }

    public Pair<byte[],byte[]> Prev() throws Exception {
        if(bucket.tx.db==null){
            throw new Exception("tx closed");
        }
        for (int i = stack.size()-1; i >=0 ; i--) {
            ElementRef elementRef = stack.get(i);
            if(elementRef.index>0){
                elementRef.index--;
                break;
            }
            stack = stack.subList(0,i+1);
        }
        if(stack.size()==0){
            return Pair.of(null,null);
        }
        last();
        Triple<byte[],byte[],Integer> triple = keyValue();
        if((triple.right & bucketLeafFlag)!=0){
            return Pair.of(triple.left,null);
        }
        return Pair.of(triple.left, triple.middle);
    }

    public Pair<byte[],byte[]> Seek(byte[] seek) throws Exception {
        Triple<byte[],byte[],Integer> triple = seek(seek);
        ElementRef ref = stack.get(stack.size()-1);
        if(ref.index> ref.count()){
            triple = next();
        }
        if(triple.left==null){
            return Pair.of(null,null);
        }else if((triple.right & bucketLeafFlag)!=0){
            return Pair.of(triple.left,null);
        }
        return Pair.of(triple.left,triple.middle);
    }

    public void delete() throws Exception {
        if(bucket.tx.db==null){
            throw new ErrTxClose();
        }else if(!bucket.writable()){
            throw new ErrTxNotWritable();
        }
        Triple<byte[],byte[],Integer> triple = keyValue();
        if((triple.right & bucketLeafFlag)!=0){
            throw new ErrIncompatibleValue();
        }
        node().del(triple.left);
    }

    public Triple<byte[],byte[],Integer> seek(byte[] seek) throws Exception {
        if(bucket.tx.db==null){
            throw new Exception("tx closed");
        }
        stack = stack.subList(0,1);
        search(seek,bucket.root);
        ElementRef ref = stack.get(stack.size()-1);
        if(ref.index>=ref.count()){
            return Triple.of(null,null,0);
        }
        return keyValue();
    }

    private void search(byte[] key, long pgid) throws Exception {
        Pair<Page,Node> pair = bucket.pageNode(pgid);
        Page p = pair.left; Node n = pair.right;
        if(p!=null && (p.flags&(branchPageFlag|leafPageFlag))==0){
            throw new Exception(String.format("invalid page type: %d: %x", p.id, p.flags));
        }
        ElementRef e = new ElementRef();
        e.page = p;
        e.node = n;
        if(e.isLeaf()){
            nsearch(key);
            return;
        }
        if(n!=null){
            searchNode(key,n);
        }
        searchPage(key,p);
    }

    private void searchNode(byte[] key, Node n) throws Exception {
        AtomicBoolean exact = new AtomicBoolean(false);
        int index = Utils.search(n.inodes.size(), i->{
            int ret = Utils.compareBytes(n.inodes.get(i).key,key);
            if(ret==0){
                exact.set(true);
            }
            return ret!=-1;
        });
        if(!exact.get() && index>0){
            index--;
        }
        stack.get(stack.size()-1).index = index;
        search(key,n.inodes.get(index).pgid);
    }

    private void nsearch(byte[] key){
        ElementRef e = stack.get(stack.size()-1);
        Page p = e.page; Node n = e.node;
        if(n!=null){
            int index = Node.binarySearchN(n.inodes,key);
            e.index = index;
            return;
        }
        List<LeafPageElement> leafPageElements = p.getLeafPageElements();
        int index = Node.binarySearch(leafPageElements,key);
        e.index = index;
    }

    private void searchPage(byte[] key, Page p) throws Exception {
        List<BranchPageElement> inodes = p.getBranchPageElements();
        AtomicBoolean exact = new AtomicBoolean(false);
        int index = Utils.search(inodes.size(), i->{
            int ret = Utils.compareBytes(inodes.get(i).key(),key);
            if(ret==0){
                exact.set(true);
            }
            return ret!=-1;
        });
        if(!exact.get() && index>0){
            index--;
        }
        stack.get(stack.size()-1).index = index;
        search(key,inodes.get(index).pgid);
    }

    public Triple<byte[], byte[], Integer> keyValue(){
        ElementRef ref = stack.get(stack.size()-1);
        if(ref.count()==0||ref.index>= ref.count()){
            return Triple.of(null,null,0);
        }
        if(ref.node!=null){
            Node.iNode inode = ref.node.inodes.get(ref.index);
            return Triple.of(inode.key,inode.value,inode.flags);
        }
        LeafPageElement element = ref.page.getLeafPageElement(ref.index);
        return Triple.of(element.key(), element.value(), element.flags);
    }

    public Node node() throws Exception {
        if(stack.size()==0){
            throw new Exception(String.format("accessing a node with a zero-length cursor stack"));
        }
        ElementRef ref = stack.get(stack.size()-1);
        if(ref.node!=null && ref.isLeaf()){
            return ref.node;
        }
        Node n = stack.get(0).node;
        if(n==null){
            n = bucket.node(stack.get(0).page.id,null);
        }
        for (int i = 0; i < stack.size(); i++) {
            ElementRef elementRef = stack.get(i);
            if(n.isLeaf){
                throw new Exception(String.format("expected branch node"));
            }
            n = n.childAt(elementRef.index);
        }
        if(!n.isLeaf){
            throw new Exception("expected leaf node");
        }
        return n;
    }
}
