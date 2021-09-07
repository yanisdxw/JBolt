package dxw.jbolt.db;
// Options represents the options that can be set when opening a database.
public class Option {
    public int timeOut;
    public boolean noGrowSync;
    public boolean readOnly;
    public int mmapFlags;
    public int initialMmapSize;

    public Option(int timeOut, boolean noGrowSync){
        this.timeOut = timeOut;
        this.noGrowSync = noGrowSync;
    }
}
