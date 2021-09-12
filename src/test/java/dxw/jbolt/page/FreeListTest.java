package dxw.jbolt.page;

import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;

import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static dxw.jbolt.node.Node.freelistPageFlag;
import static org.junit.Assert.*;

public class FreeListTest {

    @SneakyThrows
    @Test
    public void allocateTest(){
        FreeList f = new FreeList();
        f.cache = new HashMap<>();
        f.ids = Arrays.asList(3L, 4L, 5L, 6L, 7L, 9L, 12L, 13L, 18L);
        f.ids = new ArrayList<>(f.ids);
        assertTrue(f.allocate(3)==3);
        assertTrue(f.allocate(1)==6);
        assertTrue(f.allocate(3)==0);
        assertTrue(f.allocate(2)==12);
        assertTrue(f.allocate(1)==7);
        assertTrue(f.allocate(0)==0);
        assertTrue(CollectionUtils.isEqualCollection(f.ids, Arrays.asList(9L,18L)));
        assertTrue(f.allocate(1)==9);
        assertTrue(f.allocate(1)==18);
        assertTrue(f.allocate(1)==0);
        assertTrue(CollectionUtils.isEmpty(f.ids));
    }
    @Test
    public void readTest(){
        byte[] buf = new byte[4096];
        Page page = new Page(buf);
        page.setFlags(freelistPageFlag);
        page.setCount(2);
        long[] ids = new long[]{23,50};
        for (int i = 0; i < ids.length; i++) {
            page.setFreeList(i,ids[i]);
        }
        FreeList f = new FreeList();
        f.read(page);
        assertTrue(CollectionUtils.isEqualCollection(Arrays.asList(23L,50L), f.ids));
    }

    @Test
    public void writeTest(){
        byte[] buf = new byte[4096];
        Page p = new Page(buf);
        FreeList f = new FreeList();
        f.ids = new ArrayList<>(Arrays.asList(12L,39L));
        f.pending.put(100L,new long[]{28,11});
        f.pending.put(101L,new long[]{3});
        f.write(p);
        FreeList f2 = new FreeList();
        f2.read(p);
        assertTrue(CollectionUtils.isEqualCollection(Arrays.asList(3L,11L,12L,28L,39L), f2.ids));
    }

}