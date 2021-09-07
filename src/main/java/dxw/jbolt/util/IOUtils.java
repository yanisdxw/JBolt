package dxw.jbolt.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class IOUtils {

    public static void write(File file, byte[] buf){
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw"))
        {
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
        }
    }

    public static byte[] read(File file){
        byte[] buf = null;
        try(RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            FileChannel fileChannel = randomAccessFile.getChannel();
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            //You can read the file from this buffer the way you like.
            buf = buffer.array();
        }catch (Exception e){
            e.printStackTrace();
        }
        return buf;
    }

}
