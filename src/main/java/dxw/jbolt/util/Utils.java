package dxw.jbolt.util;

import lombok.SneakyThrows;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.function.Function;

public class Utils {

    public static long[] merge(long[] a, long[] b) {
        long[] answer = new long[a.length + b.length];
        int i = 0, j = 0, k = 0;
        while (i < a.length && j < b.length)
            answer[k++] = a[i] < b[j] ? a[i++] :  b[j++];
        while (i < a.length)
            answer[k++] = a[i++];
        while (j < b.length)
            answer[k++] = b[j++];
        return answer;
    }

    public static int search(int n, Function<Integer,Boolean> f){
        int i = 0; int j = n;
        while (i<j){
            int mid = (i+j)>>>1;
            if(f.apply(mid)){
                i = mid + 1;
            }else {
                j = mid;
            }
        }
        return i;
    }

    public static int compareBytes(byte[] a, byte[] b){
        int i = mismatch(a, b);
        if (i >= 0 && i < Math.min(a.length, b.length))
            return Byte.compare(a[i], b[i]);
        return a.length - b.length;
    }

    public static int mismatch(byte[] a, byte[] b){
        int index=-1;
        for (int i = 0; i < Math.min(a.length,b.length); i++) {
            if (a[i] != b[i]) {
                index=i;
                break;
            }
        }
        if(index==-1 && a.length!=b.length){
            return a.length<b.length?a.length:b.length;
        }else {
            return index;
        }
    }

    public static void printArray(int[] a){
        if(a==null||a.length==0)return;
        System.out.print("[");
        for (int i = 0; i < a.length; i++) {
            System.out.print(a[i]);
            if(i!=a.length-1){
                System.out.print(",");
            }
        }
        System.out.println("]");
    }

    @SneakyThrows
    public static int getOsPageSize() {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        Unsafe unsafe = (Unsafe)f.get(null);
        int pageSize = unsafe.pageSize();
        return pageSize;
    }
}
