package dxw.jbolt.page;

import org.junit.Test;

import static org.junit.Assert.*;

public class PageTest {

    @Test
    public void bufferWrapperTest(){
        byte[] buf = new byte[4096];
        Page page = new Page(buf,0);
        page.setFlags(2);
        page.setCount(2);
    }

}