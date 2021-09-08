package dxw.jbolt.util;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;

public class IOUtils {

    public static File openFile(String path){
        File file = new File(path);
        if(!file.exists()){
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return file;
    }

    public static FileInfo getFileInfo(File file){
        FileInfo info = new FileInfo();
        info.name = file.getName();
        info.isDir = file.isDirectory();
        info.size = file.length();
        info.fileMode = file.canWrite()?0:1;
        info.modTime = new Date(file.lastModified());
        return info;
    }

    public static void write(File file, byte[] buf){
        if(!file.exists()){
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            // Get file channel in read-write mode
            FileChannel fileChannel = randomAccessFile.getChannel();
            // Get direct byte buffer access using channel.map() operation
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096 * 8 * 8);
            //Write the content using put methods
            buffer.put(buf);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }

    public static byte[] read(File file){
        byte[] buf = null;
        try(RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            FileChannel fileChannel = randomAccessFile.getChannel();
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            //You can read the file from this buffer the way you like.
            int length = buffer.limit();
            buf = new byte[length];
            for (int i = 0; i < length; i++)
            {
                buf[i] = buffer.get();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return buf;
    }

    public static void readAt(File file, byte[] b, int off){
        try(RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            FileChannel fileChannel = randomAccessFile.getChannel();
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            //You can read the file from this buffer the way you like.
            for (int i = off; i < off + b.length; i++) {
                b[i-off] = buffer.get(i);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void writeAt(File file, byte[] input, int from){
        if(!file.exists()){
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            // Get file channel in read-write mode
            FileChannel fileChannel = randomAccessFile.getChannel();
            // Get direct byte buffer access using channel.map() operation
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096 * 8 * 8);
            //Write the content using put methods
            for (int i = from; i < from + input.length; i++) {
                buffer.put(i,input[i-from]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }

    public static void writeShort(byte[] b, int offset, int i){
        b[offset++] = (byte) (i &0xff);
        b[offset++] = (byte) (i>>>8);
    }

    public static short readShort(byte[] b, int offset){
        short i = (short) (b[offset++] & 0xff);
        i |= (b[offset++] & 0xff)<<8;
        return i;
    }

    public static void writeInt(byte[] buffer, int offset, int i) {
        buffer[offset++] = (byte) (i & 0xff);
        buffer[offset++] = (byte) (i >>> 8);
        buffer[offset++] = (byte) (i >>> 16);
        buffer[offset++] = (byte) (i >>> 24);
    }

    public static int readInt(byte[] b, int offset) {
        int i = b[offset++] & 0xff;
        i |= (b[offset++] & 0xff) << 8;
        i |= (b[offset++] & 0xff) << 16;
        i |= (b[offset++] & 0xff) << 24;
        return i;
    }

    public static void writeLong(byte[] buffer, int offset, long i) {
        buffer[offset++] = (byte) (i & 0xff);
        buffer[offset++] = (byte) (i >>> 8);
        buffer[offset++] = (byte) (i >>> 16);
        buffer[offset++] = (byte) (i >>> 24);
        buffer[offset++] = (byte) (i >>> 32);
        buffer[offset++] = (byte) (i >>> 40);
        buffer[offset++] = (byte) (i >>> 48);
        buffer[offset++] = (byte) (i >>> 56);
    }

    public static long readLong(byte[] b, int offset) {
        long l = (b[offset++] & 0xff);
        l |= (long) (b[offset++] & 0xff) << 8;
        l |= (long) (b[offset++] & 0xff) << 16;
        l |= (long) (b[offset++] & 0xff) << 24;
        l |= (long) (b[offset++] & 0xff) << 32;
        l |= (long) (b[offset++] & 0xff) << 40;
        l |= (long) (b[offset++] & 0xff) << 48;
        l |= (long) (b[offset++] & 0xff) << 56;
        return l;
    }

}
