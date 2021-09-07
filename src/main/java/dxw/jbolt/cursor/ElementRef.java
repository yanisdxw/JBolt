package dxw.jbolt.cursor;

import dxw.jbolt.node.Node;
import dxw.jbolt.page.Page;

import static dxw.jbolt.node.Node.leafPageFlag;

public class ElementRef {
    public Page page;
    public Node node;
    public int index;

    public ElementRef(){

    }

    public ElementRef(Page p, Node n, int index){
        this.page = p;
        this.node = n;
        this.index = index;
    }

    public boolean isLeaf(){
        if(node!=null){
            return node.isLeaf;
        }
        return (page.flags & leafPageFlag)!=0;
    }

    public int count(){
        if(node!=null){
            return node.inodes.size();
        }
        return page.count;
    }
}
