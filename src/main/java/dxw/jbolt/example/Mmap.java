package dxw.jbolt.example;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Mmap {

    public static String path = "C:\\Users\\yanis\\Desktop\\JBolt\\test.txt";

    public void read() throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(new File(path), "r"))
        {
            //Get file channel in read-only mode
            FileChannel fileChannel = file.getChannel();
            //Get direct byte buffer access using channel.map() operation
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            //You can read the file from this buffer the way you like.
            for (int i = 0; i < buffer.limit(); i++)
            {
                System.out.print((char) buffer.get()); //Print the content of file
            }
        }
    }

    public void write() throws Exception{
        // Create file object
        File file = new File(path);
        //Delete the file; we will create a new file
        file.delete();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw"))
        {
            // Get file channel in read-write mode
            FileChannel fileChannel = randomAccessFile.getChannel();
            // Get direct byte buffer access using channel.map() operation
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096 * 8 * 8);
            //Write the content using put methods
            buffer.put("howtodoinjava.com".getBytes());
        }
    }

    public static void main(String[] args) {
        try {
            Mmap mmap = new Mmap();
            mmap.write();
            mmap.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
