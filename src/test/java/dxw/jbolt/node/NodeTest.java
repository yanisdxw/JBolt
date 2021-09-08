package dxw.jbolt.node;

import dxw.jbolt.bucket.Bucket;
import dxw.jbolt.db.DB;
import dxw.jbolt.page.Meta;
import dxw.jbolt.page.LeafPageElement;
import dxw.jbolt.page.Page;
import dxw.jbolt.tx.Tx;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static dxw.jbolt.node.Node.*;
import static dxw.jbolt.util.Consts.leafPageElementSize;
import static org.junit.Assert.*;

public class NodeTest {

    @Test
    public void binarySearchTest(){
        List<Node.iNode> inodes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Node.iNode inode = new Node.iNode();
            inode.key = String.valueOf(i).getBytes();
            inodes.add(inode);
        }
        int index = binarySearchN(inodes, "4".getBytes());
        System.out.println(index);
    }


    @Test
    public void putTest(){
        Node n = new Node();
        n.inodes = new ArrayList<>();
        n.put("baz".getBytes(),"baz".getBytes(),"2".getBytes(),0,0);
        n.put("foo".getBytes(),"foo".getBytes(),"0".getBytes(),0,0);
        n.put("bar".getBytes(),"bar".getBytes(),"1".getBytes(),0,0);
        n.put("foo".getBytes(),"foo".getBytes(),"3".getBytes(),0, leafPageFlag);
        assertTrue(n.inodes.size()==3);
        assertTrue(new String(n.inodes.get(0).key).equals("bar")
                && new String(n.inodes.get(0).value).equals("1"));
        assertTrue(new String(n.inodes.get(1).key).equals("baz")
                && new String(n.inodes.get(1).value).equals("2"));
        assertTrue(new String(n.inodes.get(2).key).equals("foo")
                && new String(n.inodes.get(2).value).equals("3"));
        assertTrue(n.inodes.get(2).flags==leafPageFlag);
    }

    @Test
    public void readTest(){
        //create a page
        byte[] buf = new byte[4096];
        Page page = new Page(buf,0);
        page.setCount(2);
        page.setFlags(leafPageFlag);

        //写入 LeafPageElement， 读出 Node
        //Insert 2 elements at the beginning.
        //sizeof(leafPageElement)==16： 一个leafPageElement占16个字节
        LeafPageElement element1 = new LeafPageElement(0,leafPageElementSize*2,3,4); // pos = sizeof(leafPageElement)*3
        LeafPageElement element2 = new LeafPageElement(0,leafPageElementSize*1+3+4,10,3); // pos = sizeof(leafPageElement)*2 + 3 + 4
        //LeafPageElement element3 = new LeafPageElement(0,leafPageElementSize*1+3+4+10+3,5,5); // pos = sizeof(leafPageElement)*1 + 10 + 3
        //write data for the nodes at the end.
        page.setLeafPageElement(0,element1);
        page.setLeafPageElement(1,element2);
        //page.setLeafPageElement(2,element3);
        System.arraycopy("barfooz".getBytes(),0,buf,page.offset,7);
        System.arraycopy("helloworldbye".getBytes(),0,buf,page.offset+7,13);
        //System.arraycopy("helloworld".getBytes(),0,buf,page.offset+7+13,10);

        // Deserialize page into a leaf.
        Node n = new Node();
        n.read(page);

        // Check that there are two inodes with correct data.
        assertTrue(n.isLeaf);
        assertTrue(n.inodes.size()==2);
        String key1 = new String(n.inodes.get(0).key);
        String value1 = new String(n.inodes.get(0).value);
        assertTrue(key1.equals("bar") && value1.equals("fooz"));
        String key2 = new String(n.inodes.get(1).key);
        String value2 = new String(n.inodes.get(1).value);
        assertTrue(key2.equals("helloworld") && value2.equals("bye"));
//        String key3 = new String(n.inodes.get(2).key);
//        String value3 = new String(n.inodes.get(2).value);
//        assertTrue(key3.equals("hello") && value3.equals("world"));
    }

    @Test
    public void writeTest() throws Exception {
        Node n = new Node();
        n.isLeaf = true;
        n.inodes = new ArrayList<>();
        n.put("susy".getBytes(),"susy".getBytes(),"que".getBytes(),0,0);
        n.put("ricki".getBytes(),"ricki".getBytes(),"lake".getBytes(),0,0);
        n.put("john".getBytes(),"john".getBytes(),"johnson".getBytes(),0,0);
        byte[] buf = new byte[4096];
        Page page = new Page(buf,0);
        n.write(page);

        Node n2 = new Node();
        n2.read(page);
        n2.put("susy".getBytes(),"susy你好".getBytes(),"que".getBytes(),0,0);

        assertTrue(n.inodes.size()==3);
        String key1 = new String(n2.inodes.get(0).key);
        String value1 = new String(n2.inodes.get(0).value);
        String key2 = new String(n2.inodes.get(1).key);
        String value2 = new String(n2.inodes.get(1).value);
        String key3 = new String(n2.inodes.get(2).key);
        String value3 = new String(n2.inodes.get(2).value);
        assertTrue("john".equals(key1) && "johnson".equals(value1));
        assertTrue("ricki".equals(key2) && "lake".equals(value2));
        assertTrue("susy你好".equals(key3) && "que".equals(value3));
        //        String key3 = new String(n2.inodes[2].key);
//        String value3 = new String(n2.inodes[2].value);
//        assertTrue("john".equals(key3) && "johnson".equals(value3));
    }

    @Test
    public void splitTest(){
        //creata a node
        Node node = new Node();
        node.inodes = new ArrayList<>();
        node.bucket = new Bucket();
        node.bucket.tx = new Tx();
        node.bucket.tx.db = new DB();
        node.bucket.tx.meta = new Meta(1);
        node.put("00000001".getBytes(),"00000001".getBytes(),"0123456701234567".getBytes(),0,0);
        node.put("00000002".getBytes(),"00000002".getBytes(),"0123456701234567".getBytes(),0,0);
        node.put("00000003".getBytes(),"00000003".getBytes(),"0123456701234567".getBytes(),0,0);
        node.put("00000004".getBytes(),"00000004".getBytes(),"0123456701234567".getBytes(),0,0);
        node.put("00000005".getBytes(),"00000005".getBytes(),"0123456701234567".getBytes(),0,0);
        //Split between 2 & 3.
        node.split(100);
        Node parent = node.parent;
        assertTrue(parent.children.size()==2);
        assertTrue(parent.children.get(0).inodes.size()==2);
        assertTrue(parent.children.get(1).inodes.size()==3);
    }

    @Test
    public void splitMinKeysTest(){
        //creata a node
        Node node = new Node();
        node.inodes = new ArrayList<>();
        node.bucket = new Bucket();
        node.bucket.tx = new Tx();
        node.bucket.tx.db = new DB();
        node.bucket.tx.meta = new Meta(1);
        node.put("00000001".getBytes(),"00000001".getBytes(),"0123456701234567".getBytes(),0,0);
        node.put("00000002".getBytes(),"00000002".getBytes(),"0123456701234567".getBytes(),0,0);
        node.split(20);
        assertTrue(node.parent==null);
    }

    @Test
    public void splitSinglePageTest(){
        //creata a node
        Node node = new Node();
        node.inodes = new ArrayList<>();
        node.bucket = new Bucket();
        node.bucket.tx = new Tx();
        node.bucket.tx.db = new DB();
        node.bucket.tx.meta = new Meta(1);
        node.put("00000001".getBytes(),"00000001".getBytes(),"0123456701234567".getBytes(),0,0);
        node.put("00000002".getBytes(),"00000002".getBytes(),"0123456701234567".getBytes(),0,0);
        node.put("00000003".getBytes(),"00000003".getBytes(),"0123456701234567".getBytes(),0,0);
        node.put("00000004".getBytes(),"00000004".getBytes(),"0123456701234567".getBytes(),0,0);
        node.put("00000005".getBytes(),"00000005".getBytes(),"0123456701234567".getBytes(),0,0);
        node.split(4096);
        assertTrue(node.parent==null);
    }

}