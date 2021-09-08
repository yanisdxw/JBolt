package dxw.jbolt.util;

import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.*;

public class UtilsTest {

    @Test
    public void compareBytesTest(){
//        byte[] sa = new byte[]{2,3};
//        byte[] sb = new byte[]{12,3};
//        byte[] sc = new byte[]{3};
//        byte[] sd = new byte[]{2,1,5};
//        byte[] se = new byte[]{2,3,4};
//        System.out.println(Utils.compareBytes(sa,sb));
//        System.out.println(Utils.compareBytes(sa,sc));
//        System.out.println(Utils.compareBytes(sa,sd));
//        System.out.println(Utils.compareBytes(sa,se));
//        System.out.println();
//        sa = "bc".getBytes();
//        sb = "abc".getBytes();
//        sc = "bcd".getBytes();
//        sd = "ab".getBytes();
//        se = "bc".getBytes();
//        System.out.println(Utils.compareBytes(sa,sb));
//        System.out.println(Utils.compareBytes(sa,sc));
//        System.out.println(Utils.compareBytes(sa,sd));
//        System.out.println(Utils.compareBytes(sa,se));
        System.out.println("a".getBytes().length);
        System.out.println((int) Math.ceil(1/3.0));

        int[] a = new int[]{1,2,3,4,5,6,7,8,9,10};

//        int[] b = Arrays.copyOfRange(a,0,5);
//        int[] c = Arrays.copyOfRange(a,5,10);
//        Utils.printArray(b);
//        Utils.printArray(c);
        byte[] out = bigIntToByteArray(4096);
        out = bigIntToByteArray(10);
        out = bigIntToByteArray(16);
        out = bigIntToByteArray(48);
        out = bigIntToByteArray(128);
        out = bigIntToByteArray(512);
        out = bigIntToByteArray(1024);
        System.out.println(Utils.getOsPageSize());

    }

    private byte[] bigIntToByteArray( final int i ) {
        BigInteger bigInt = BigInteger.valueOf(i);
        return bigInt.toByteArray();
    }

    @Test
    public void testGetOsPageSize(){
        System.out.println(Utils.getOsPageSize());
    }

}