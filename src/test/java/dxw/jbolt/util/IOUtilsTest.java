package dxw.jbolt.util;

import dxw.jbolt.page.LeafPageElement;
import org.junit.Test;

import java.io.File;

import static dxw.jbolt.node.Node.branchPageFlag;
import static dxw.jbolt.node.Node.leafPageFlag;
import static org.junit.Assert.*;

public class IOUtilsTest {

    @Test
    public void IOTest(){
        String path = "C:\\Users\\yanis\\Desktop\\JBolt\\test.txt";
        File file = new File(path);
        byte[] in = "hello world".getBytes();
        IOUtils.write(file, in);
        byte[] out = IOUtils.read(file);
        System.out.println(new String(out));
    }

    @Test
    public void ObjectToByteTest(){

    }

}