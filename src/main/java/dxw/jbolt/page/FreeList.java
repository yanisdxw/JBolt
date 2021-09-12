package dxw.jbolt.page;

import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static dxw.jbolt.node.Node.freelistPageFlag;
import static dxw.jbolt.util.Consts.maxAllocSize;
import static dxw.jbolt.util.Consts.pageHeaderSize;

/**
 * 落盘时只存储ids字段，会把pending和ids合并后存储。所以page.ptr指向的是ids列表。
 * 如果ids数量超过0xFFFF（page.count的最大值），则会把ptr指向的第一个位置作为ids的长度。
 */
public class FreeList {
    public List<Long> ids;
    public Map<Long,long[]> pending;
    public Map<Long,Boolean> cache;

    public FreeList() {
        pending = new HashMap<>();
        cache = new HashMap<>();
        ids = new ArrayList<>();
    }

    // size returns the size of the page after serialization.
    public int size(){
        int n = count();
        if(n>=0xFFF){
            n++;
        }
        return pageHeaderSize + n;
    }

    public int count() {
        return pending_count()+free_count();
    }

    public int free_count(){
        return ids.size();
    }

    public int pending_count(){
        AtomicInteger count = new AtomicInteger();
        pending.forEach((id,ids)->{
            count.addAndGet(ids.length);
        });
        return count.get();
    }

    public void copyall(List<Long> ids){
        for(Map.Entry<Long,long[]> entry:pending.entrySet()){
            long[] tmp = entry.getValue();
            for (int i = 0; i < tmp.length; i++) {
                ids.add(tmp[i]);
            }
        }
        Collections.sort(ids);
    }

    // allocate returns the starting page id of a contiguous list of pages of a given size.
    // If a contiguous block cannot be found then 0 is returned.
    public long allocate(int n) throws Exception {
        if(ids.size()==0){
            return 0;
        }
        long initial =0 , previd = 0;
        for (int i = 0; i < ids.size(); i++) {
            if(ids.get(i)<=1){
                throw new Exception(String.format("invalid page allocation: %d", ids.get(i)));
            }
            // Reset initial page if this is not contiguous.
            if(previd==0||ids.get(i)-previd!=1){
                initial = ids.get(i);
            }
            // If we found a contiguous block then remove it and return it.
            if(ids.get(i)-initial+1==n){
                // If we're allocating off the beginning then take the fast path
                // and just adjust the existing slice. This will use extra memory
                // temporarily but the append() in free() will realloc the slice
                // as is necessary.
                if(i+1==n){
                    ids = ids.subList(i+1,ids.size());
                }else {
                    List<Long> tmp = new ArrayList<>();
                    tmp.addAll(ids.subList(0,i+1-n));
                    tmp.addAll(ids.subList(i+1,ids.size()));
                    ids = tmp;
                }
                for (int j = 0; j < n; j++) {
                    cache.remove(initial+1);
                }
                return initial;
            }
            previd = ids.get(i);
        }
        return 0;
    }

    // free releases a page and its overflow for a given transaction id.
    // If the page is already free then a panic will occur.
    public void free(long txid, Page p) throws Exception {
        if(p.id<=1){
            throw new Exception(String.format("cannot free page 0 or 1: %d", p.id));
        }
        // Free page and all its overflow pages.
        long[] ids = pending.get(txid);
        for (long id = p.id; id <= p.id + p.overflow ; id++) {
            // Verify that page is not already free.
            if(cache.get(id)){
                throw new Exception(String.format("page %d already freed", id));
            }
            // Add to the freelist and cache.
            ids = Arrays.copyOf(ids,ids.length+1);
            ids[ids.length-1] = id;
            cache.put(id,true);
        }
        pending.put(txid,ids);
    }

    // release moves all page ids for a transaction id (or older) to the freelist.
    public void release(long txid){
        TreeSet<Long> m = new TreeSet<>();
        pending.forEach((tid,ids)->{
            if(tid<=txid){
                // Move transaction's pending pages to the available freelist.
                // Don't remove from the cache since the page is still free.
                m.addAll(toList(ids));
                pending.remove(tid);
            }
        });
        Collections.sort(ids);
    }

    // rollback removes the pages from a given pending tx.
    public void rollback(long txid){
        long[] ids = pending.get(txid);
        for (int i = 0; i < ids.length; i++) {
            cache.remove(ids[i]);
        }
        pending.remove(txid);
    }

    // freed returns whether a given page is in the free list.
    public boolean freed(long pgid){
        return cache.containsKey(pgid);
    }

    // read initializes the freelist from a freelist page.
    public void read(Page p){
        int idx = 0;
        int count = p.count;
        // If the page.count is at the max uint16 value (64k) then it's considered
        // an overflow and the size of the freelist is stored as the first element.
        if(count==0xFFFF){
            idx = 1;
            count = maxAllocSize;
        }
        // Copy the list of page ids from the freelist.
        if(count==0){
            ids = null;
        }else {
            ids = p.getPgids(idx,count);
            Collections.sort(ids);
        }
        reindex();
    }

    // write writes the page ids onto a freelist page. All free and pending ids are
    // saved to disk since in the event of a program crash, all pending ids will
    // become free.
    public void write(Page p){
        p.setFlags(p.flags|freelistPageFlag);
        // The page.count can only hold up to 64k elements so if we overflow that
        // number then we handle it by putting the size in the first element.
        int lenids = count();
        if(lenids==0){
            p.setCount(lenids);
        } else if (lenids<0xFFFF) {
            p.setCount(lenids);
            copyall(ids);
            for (int i = 0; i < ids.size(); i++) {
                p.setFreeList(i,ids.get(i));
            }
        } else {
            p.setCount(0xFFFF);
            copyall(ids);
            for (int i = 0; i < ids.size(); i++) {
                p.setFreeList(i,ids.get(i));
            }
        }
    }

    public void reload(Page p){
        this.read(p);
        Map<Long,Boolean> pcache = new HashMap<>();
        pending.forEach((pid,pendingIDs)->{
            for (long pendingId = 0; pendingId < pendingIDs.length; pendingId++) {
                pcache.put(pendingId, true);
            }
        });
        List<Long> a = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            long id = ids.get(i);
            if(pcache.get(id)){
                a.add(id);
            }
        }
        ids = a;
        reindex();
    }


    // reindex rebuilds the free cache based on available and pending free lists.
    public void reindex(){
        cache = new HashMap<>();
        if(ids!=null){
            for (long id = 0; id < ids.size(); id++) {
                cache.put(id,true);
            }
        }
        pending.forEach((id, pendingIDs)->{
            for (long pendingID = 0; pendingID < pendingIDs.length; pendingID++) {
                cache.put(pendingID, true);
            }
        });
    }

    private List<Long> toList(long[] ids){
        List<Long> m = new ArrayList<>();
        for (int i = 0; i < ids.length; i++) {
            m.add(ids[i]);
        }
        return m;
    }

    private long[] toArrays(List<Long> ids){
        long[] rst = new long[ids.size()];
        for (int i = 0; i < ids.size(); i++) {
            rst[i] = ids.get(i);
        }
        return rst;
    }

    private long[] toArrays(TreeSet<Long> ids){
        int index = 0;
        long[] rst = new long[ids.size()];
        Iterator<Long> iterator = ids.iterator();
        while (iterator.hasNext()){
            rst[index++] = iterator.next();
        }
        return rst;
    }
}
