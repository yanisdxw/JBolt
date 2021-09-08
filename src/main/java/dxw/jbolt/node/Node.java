package dxw.jbolt.node;

import dxw.jbolt.bucket.Bucket;
import dxw.jbolt.page.*;
import dxw.jbolt.tx.Tx;
import dxw.jbolt.util.Pair;
import dxw.jbolt.util.Utils;
import java.util.*;

import static dxw.jbolt.util.Consts.*;

/**
 * 代表内存中一个树节点
 * 访问结点时，首先要将 page 反序列化后得到得到 node 结构体，代表 B+ 树中一个结点
 */
public class Node {
    public Bucket bucket;
    public boolean isLeaf; // 用来区分树枝和叶子节点
    public boolean unbalanced;
    public boolean spilled;
    public byte[] key; // 该节点中的最小的key
    public long pgid;
    public Node parent;
    public List<Node> children;
    public List<iNode> inodes; // node中真正存数据的地方

    public static byte branchPageFlag   = 0x01;
    public static byte leafPageFlag     = 0x02;
    public static byte metaPageFlag     = 0x04;
    public static byte freelistPageFlag = 0x10;

    public Node(){

    }

    public Node(boolean isLeaf){
        this.isLeaf = isLeaf;
    }

    // del removes a key from the node.
    public void del(byte[] key){
        int index = binarySearchN(inodes, key);
        if(index>=inodes.size() || Arrays.equals(inodes.get(index).key,key)){
            return;
        }
        inodes.remove(index);
        unbalanced = true;
    }

    // read initializes the node from a page.
    public void read(Page page){
        pgid = page.id;
        isLeaf = (page.flags & leafPageFlag)!=0;
        inodes = new ArrayList<>();
        for (int i = 0; i < page.count; i++) {
            iNode inode = new iNode();
            if(isLeaf){
                LeafPageElement element = page.getLeafPageElement((short) i);
                inode.flags = element.flags;
                inode.key = element.key();
                inode.value = element.value();
            }else {
                BranchPageElement element = page.getBranchPageElement((short) i);
                inode.pgid = element.pgid;
                inode.key = element.key();
            }
            inodes.add(inode);
        }
        if(inodes.size()>0){
            key = inodes.get(0).key;
        }else {
            key = null;
        }
    }

    public void write(Page page) throws Exception {
        // Initialize page.
        if(isLeaf){
            page.flags |= leafPageFlag;
        }else {
            page.flags |= branchPageFlag;
        }
        page.setFlags(page.flags);
        if(inodes.size()>=0xFFFF){
            throw new Exception(String.format("inode overflow: %d (pgid=%d)", inodes.size(), page.id));
        }
        page.count = (short) inodes.size();
        page.setCount(page.count);
        // Stop here if there are no items to write.
        if(page.count==0){
            return;
        }
        // Loop over each item and write it to the page.
        byte[] b = page.data;
        int offset = 0;
        for (int i = 0; i < inodes.size(); i++) {
            if(isLeaf){
                LeafPageElement element = new LeafPageElement();
                element.pos = offset + (page.count-i)*leafPageElementSize;
                element.flags = inodes.get(i).flags;
                element.ksize = inodes.get(i).key.length;
                element.vsize = inodes.get(i).value.length;
                page.setLeafPageElement(i,element);
                /** &leafPageElement + pos == &key
                 *  &leafPageElement + pos + ksize == &val */
                System.arraycopy(inodes.get(i).key,0,b,pageHeaderSize+i*leafPageElementSize+element.pos,element.ksize);
                System.arraycopy(inodes.get(i).value,0,b,pageHeaderSize+i*leafPageElementSize+element.pos+element.ksize,element.vsize);
                offset += element.ksize + element.vsize;
            }else {
                BranchPageElement element = new BranchPageElement();
                element.pos = offset - (page.count-i)*branchPageElementSize;
                element.ksize = inodes.get(i).key.length;
                element.pgid = inodes.get(i).pgid;
                page.setBranchPageElement(i,element);
                System.arraycopy(inodes.get(i).key,0,b,pageHeaderSize+i*branchPageElementSize+element.pos,element.ksize);
                offset += element.ksize;
            }
            // If the length of key+value is larger than the max allocation size
            // then we need to reallocate the byte array pointer.
            int klen = inodes.get(i).key.length;
            int vlen = inodes.get(i).value.length;
            if(b.length<klen+vlen){

            }
            // Write data for the element to the end of the page.

        }
    }

    // put inserts a key/value.
    public void put(byte[] oldKey, byte[] newKey, byte[] value, long pgid, int flags){
        //TODO: tx check
        //oldkey的inode的index
        iNode iNode = new iNode();
        int index = binarySearchN(inodes,oldKey);
        // Add capacity and shift nodes if we don't have an exact match and need to insert.
        boolean exact = inodes.size()>0 && index<inodes.size()
                && Arrays.equals(inodes.get(index).key, oldKey);
        iNode.flags = flags;
        iNode.key = newKey;
        iNode.value = value;
        iNode.pgid = pgid;
        if(!exact){
//            int len = inodes.size()+1;
//            iNode[] temp = Arrays.copyOf(inodes,len);
//            for (int i = temp.length-1; i > index; i--) {
//                temp[i] = inodes[i-1];
//            }
//            inodes = temp;
            inodes.add(index,iNode);
        } else {
            inodes.set(index,iNode);
        }
    }

    /**
     * 分裂
     * 对涉及到的buckets中节点均进行判定及分裂：先对每个sub bucket进行判断，看是否是inline-bucket，
     * 非inline-bucket中每个节点均需进行判定及分裂
     *
     * 分裂流程：
     *
     * 1.从每个bucket中的根节点开始，自顶往下递归到叶子节点
     * 2.对每个节点进行判定及分裂为多个节点（根据填充率分割）
     * 3.对分裂后的多个节点，依次对每个节点分配pageid，如果该节点有pageid的话，
     *   需放入freelist pending中(freelist这个数据结构会用来记录空闲pageid列表和当前待释放pageid列表)。
     * 4.分裂可能会导致父节点key数量过多，所以也需要进行分裂
     * @param pageSize
     * @return
     */
    // split breaks up a node into multiple smaller nodes, if appropriate.
    // This should only be called from the spill() function.
    public List<Node> split(int pageSize){
        List<Node> nodes = new ArrayList<>();
        Node node = this;
        while (true){
            Pair<Node,Node> pair = node.splitTwo(pageSize);
            nodes.add(pair.left);
            if(pair.right==null){
                break;
            }
            node = pair.right;
        }
        return nodes;
    }

    // splitTwo breaks up a node into two smaller nodes, if appropriate.
    // This should only be called from the split() function.
    private Pair<Node,Node> splitTwo(int pageSize){
        // Ignore the split if the page doesn't have at least enough nodes for
        // two pages or if the nodes can fit in a single page.
        if(inodes.size()<=minKeysPerPage*2 || sizeLessThan(pageSize)){
            return Pair.of(this, null);
        }
        // Determine the threshold before starting a new node.
        double fillPercent = bucket.fillPercent;
        if(fillPercent<minFillPercent){
            fillPercent = minFillPercent;
        }else {
            fillPercent = maxFillPercent;
        }
        // Determine split position and sizes of the two pages.
        int threshold = (int) (pageSize*fillPercent);
        int splitIndex = splitIndex(threshold);
        // Split node into two separate nodes.
        // If there's no parent then we'll need to create one.
        if (parent==null){
            parent = new Node();
            parent.bucket = bucket;
            parent.children = new ArrayList<>();
            parent.children.add(this);
        }
        // Create a new node and add it to the parent.
        Node next = new Node();
        next.bucket = bucket;
        next.isLeaf = isLeaf;
        next.parent = parent;
        parent.children.add(next);
        // Split inodes across two nodes.
        next.inodes = inodes.subList(splitIndex, inodes.size());
        inodes = inodes.subList(0,splitIndex);
        // todo: Update the statistics.
        //
        return Pair.of(this,next);
    }

    // splitIndex finds the position where a page will fill a given threshold.
    // It returns the index as well as the size of the first page.
    // This is only be called from split().
    private int splitIndex(int threshold){
        int sz = pageHeaderSize;
        int index = 0;
        // Loop until we only have the minimum number of keys required for the second page.
        for (int i = 0; i < inodes.size()-minKeysPerPage; i++) {
            index = i;
            iNode inode = inodes.get(i);
            int elsize = pageElementSize()+inode.key.length+inode.value.length;
            // If we have at least the minimum number of keys and adding another
            // node would put us over the threshold then exit and return.
            if(i>=minKeysPerPage && sz+elsize>threshold){
                break;
            }
            sz+= elsize;
        }
        return index;
    }

    /**
     * b.spill()函数对Bucket中node进行分裂。
     *
     * 一个节点能被分裂成多个节点需满足下面条件（两个条件都要满足）
     *
     * 1.inodes中的数据量大于4
     * 2.inodes中的数据大小大于pagesize
     * 因为可能存在大key问题：inodes中key数量满足条件1，但它里面的某个key的value值很大，大于pagesize，此时无需分裂。
     */
    // spill writes the nodes to dirty pages and splits nodes as it goes.
    // Returns an error if dirty pages cannot be allocated.
    public void spill(){
        Tx tx = this.bucket.tx;
        if(this.spilled){
            return;
        }
        // Spill child nodes first. Child nodes can materialize sibling nodes in
        // the case of split-merge so we cannot use a range loop. We have to check
        // the children size on every loop iteration.
        for (int i = 0; i < children.size(); i++) {
            children.get(i).spill();
        }
        // We no longer need the child list because it's only used for spill tracking.
        children = null;
        // Split nodes into appropriate sizes. The first node will always be n.
        List<Node> nodes = this.split(tx.db.pageSize);
        for(Node node:nodes){
            // Add node's page to the freelist if it's not new.
            if(node.pgid>0){
                //tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
                node.pgid = 0;
            }
            // Allocate contiguous space for the node.

        }
    }

    // inode represents an internal node inside of a node.
    // It can be used to point to elements in a page or point
    // to an element which hasn't been added to a page yet.
    // inodes中保存k/v数据.树枝和叶子节点公用这个数据结构。
    public static class iNode {
       public int flags; // 仅叶子节点使用。存放数据内容标识,为bucket或普通数据中的一种
       public long pgid; // 仅树枝节点使用。存放子节点的page id
       public byte[] key; // 树枝节点和叶子节点公用。key
       public byte[] value; // 仅叶子节点使用。存放普通数据或bucket
    }

    // sizeLessThan returns true if the node is less than a given size.
    // This is an optimization to avoid calculating a large node when we only need
    // to know if it fits inside a certain page size.
    private boolean sizeLessThan(int v){
        int sz = pageHeaderSize;
        int elsz = this.pageElementSize();
        for (int i = 0; i < inodes.size(); i++) {
            iNode item = inodes.get(i);
            sz += elsz + item.key.length + item.value.length;
            if(sz>=v){
                return false;
            }
        }
        return true;
    }

    private int pageElementSize(){
        if(isLeaf){
            return leafPageElementSize;
        }else {
            return branchPageElementSize;
        }
    }

    public int size(){
        int sz = pageHeaderSize;
        int elsz = pageElementSize();
        for (int i = 0; i < inodes.size(); i++) {
            iNode item = inodes.get(i);
            sz += elsz + item.key.length + item.value.length;
        }
        return sz;
    }

    // childAt returns the child node at a given index.
    public Node childAt(int index) throws Exception {
        if(isLeaf){
            throw new Exception(String.format("invalid childAt(%d) on a leaf node", index));
        }
        return bucket.node(inodes.get(index).pgid,this);
    }

    //二分查找寻找key所在的index，没有就返回-1
    public static int binarySearchN(List<iNode> iNodes, byte[] key){
        int left=0; int right=iNodes.size();
        while (left<right){
            int mid = left + (right-left)/2;
            int cmp = Utils.compareBytes(iNodes.get(mid).key,key);
            if(cmp==0){
                return mid;
            } else if(cmp<0){
                left = mid+1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    public static int binarySearch(List<LeafPageElement> leafPageElements, byte[] key){
        int left=0; int right=leafPageElements.size();
        while (left<right){
            int mid = left + (right-left)/2;
            int cmp = Utils.compareBytes(leafPageElements.get(mid).key(),key);
            if(cmp==0){
                return mid;
            } else if(cmp<0){
                left = mid+1;
            } else {
                right = mid;
            }
        }
        return left;
    }
}
