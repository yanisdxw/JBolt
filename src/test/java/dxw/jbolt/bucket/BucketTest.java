package dxw.jbolt.bucket;

import dxw.jbolt.db.DB;
import lombok.SneakyThrows;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class BucketTest {

    @SneakyThrows
    @Test
    public void TestBucket_Get_NonExistent(){
        DB db = DB.MustOpenDB();
        db.update(tx->{
            Bucket b = tx.createBUcket(new String("widgets").getBytes());
            byte[] v = b.get("foo".getBytes());
            assertTrue(v==null);
        });
        db.close();
    }

    @SneakyThrows
    @Test
    public void TestBucket_Put(){
        DB db = DB.MustOpenDB();
        db.update(tx->{
            Bucket b = tx.createBUcket("widgets".getBytes());
            byte[] key = "foo".getBytes();
            byte[] value = "bar".getBytes();
            b.put(key,value);
            Bucket bb = tx.Bucket("widget".getBytes());
            byte[] v = bb.get(key);
            assertTrue(Arrays.equals(v,value));
        });
    }

    @SneakyThrows
    @Test
    public void TestBucket_Delete(){
        DB db = DB.MustOpenDB();
        db.update(tx->{
            Bucket b = tx.createBUcket("widgets".getBytes());
            byte[] key = "foo".getBytes();
            byte[] value = "bar".getBytes();
            b.put(key,value);
            b.delete(key);
            byte[] v = b.get(key);
            assertTrue(Arrays.equals(v,value));
        });
    }

}