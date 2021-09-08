package dxw.jbolt.db;

import dxw.jbolt.exception.ErrVersionMismatch;
import dxw.jbolt.page.Meta;

import dxw.jbolt.util.IOUtils;
import dxw.jbolt.util.Utils;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static dxw.jbolt.util.Consts.pageHeaderSize;
import static dxw.jbolt.util.Consts.pageSize;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class DBTest {

    @Test
    public void TestOpenErrChecksum() throws Exception {
        if(pageSize!= Utils.getOsPageSize()){
            System.out.println("page size mismatch");
        }

        DB db = DB.MustOpenDB();
        String path = db.path;
        db.Close();

        byte[] buf0 = new byte[0x1000];
        IOUtils.readAt(new File(path),buf0,0);
        buf0 = Arrays.copyOfRange(buf0,pageHeaderSize,buf0.length);
        Meta meta0 = Meta.getMeta(buf0);
        meta0.version++;
        buf0 = meta0.toBytes();

        byte[] buf1 = new byte[0x1000];
        IOUtils.readAt(new File(path),buf1, db.pageSize);
        buf1 = Arrays.copyOfRange(buf1,pageHeaderSize,buf1.length);
        Meta meta1 = Meta.getMeta(buf1);
        meta1.version++;
        buf1 = meta1.toBytes();

        IOUtils.writeAt(new File(path),buf0,pageHeaderSize);
        IOUtils.writeAt(new File(path),buf1,pageHeaderSize+pageSize);

        try {
            DB db1 = new DB().Open(path,"rw",null);
        }catch (ErrVersionMismatch e){
            assertTrue(e instanceof ErrVersionMismatch);
        }
    }

}