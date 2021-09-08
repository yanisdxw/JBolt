package dxw.jbolt.util;

public class Consts {
    // maxAllocSize is the size used when creating array pointers.
    public static int maxAllocSize = 0x7FFFFFFF;
    // leafPageElement size
    public static int leafPageElementSize = 16;
    // branchPageElement size
    public static int branchPageElementSize = 16;
    // freelistPageElement size
    public static int freelistPageElementSize = 8;
    // pageSize
    public static int pageSize = 4096;

    public static int minKeysPerPage = 2;
    public static int pageHeaderSize = 16;

    public static double minFillPercent = 0.1;
    public static double maxFillPercent = 1.0;

    // Represents a marker value to indicate that a file is a Bolt DB.
    public static int magic= 0xED0CDAED;
    // The data file format version.
    public static int version = 2;

    // Invented values to support what package os expects.
    public static final String O_RDONLY = "r";
    public static final String O_RDWR =   "rw";
    public static final String O_SYNC =   "rws";
    public static final String O_DSYNC =  "rwd";
}
