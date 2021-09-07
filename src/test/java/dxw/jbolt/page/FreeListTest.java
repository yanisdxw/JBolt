package dxw.jbolt.page;

import lombok.SneakyThrows;
import org.junit.Test;

import java.nio.file.attribute.FileTime;
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
        f.ids = new long[]{3, 4, 5, 6, 7, 9, 12, 13, 18};
        assertTrue(f.allocate(3)==3);
        assertTrue(f.allocate(1)==6);
        assertTrue(f.allocate(3)==0);
        assertTrue(f.allocate(2)==12);
        assertTrue(f.allocate(1)==7);
        assertTrue(f.allocate(0)==0);
        assertTrue(f.allocate(0)==0);
        assertTrue(Arrays.equals(f.ids,new long[]{9,18}));
        assertTrue(f.allocate(1)==9);
        assertTrue(f.allocate(1)==18);
        assertTrue(f.allocate(1)==0);
        assertTrue(Arrays.equals(f.ids,new long[]{}));
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
        assertTrue(Arrays.equals(ids, f.ids));
    }

    @Test
    public void writeTest(){
        byte[] buf = new byte[4096];
        Page p = new Page(buf);
        FreeList f = new FreeList();
        f.ids = new long[]{12,39};
        f.pending.put(100L,new long[]{28,11});
        f.pending.put(101L,new long[]{3});
        f.write(p);
        FreeList f2 = new FreeList();
        f2.read(p);
        assertTrue(Arrays.equals(new long[]{3,11,12,28,39}, f2.ids));
    }

}