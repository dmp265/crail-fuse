package dmp265.crailfuse;

import java.io.IOException;
import java.lang.System;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Objects;

import jnr.ffi.BaseStruct;
import jnr.ffi.Platform;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.Struct;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.ffi.types.u_int64_t;

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailResult;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;

import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

/**
 * @author Daniel Perry dmp265@cornell.edu
 * Before beginning, please read note above write()
 */
public class CrailFuse extends FuseStubFS {
  private final long PG_SIZE = 4 * 1024; // 4 KB Page Size

  // Class to hold the metadata for each file
  private static class File_Meta {
    private long size;
    private long pages;

    public File_Meta() {
      size = 0;
      pages = 0;
    }

    // Getters/setters:

    public long get_size() {
      return this.size;
    }

    public void set_size(long size) {
      this.size = size;
    }

    public long get_pages() {
      return this.pages;
    }

    public void set_pages(long pages) {
      this.pages = pages;
    }
  }

  // Underlying Crail store
  private CrailStore store;

  // Collection of metadata objects for every file in the filesystem
  private HashMap<String, File_Meta> meta;

  private CrailStore store() throws Exception {
    return store;
  }

  private HashMap<String, File_Meta> meta() {
    return meta;
  }

  /* Maps fileysystem file name and page number to name of page in underlying
  Crail store
  */
  private String page_file(String path, long page) {
    return path + ".#" + String.valueOf(page);
  }

  // FUSE equivalent to POSIX's stat()
  @Override
  public int getattr(String path, FileStat stat) {
    // Special case for root (the only directory in the system)
    if (path.equals("/")) {
      stat.st_mode.set(FileStat.S_IFDIR | 0755);
      stat.st_nlink.set(2);
      return 0;
    }

    // Fail if directory
    if (path.indexOf('/') > 0) {
      System.err.println("getattr: " + path + " is a directory!");
      return -ErrorCodes.EISDIR();
    }

    // Fail if file does not exist
    if(!meta().containsKey(path)) {
      System.err.println("getattr: failed to stat " + path);
      return -ErrorCodes.ENOENT();
    }

    File_Meta md = meta().get(path);

    // All files should return the same values (besides size)
    stat.st_mode.set(FileStat.S_IFREG | 0666);
    stat.st_nlink.set(1);
    stat.st_size.set(md.get_size());

    System.out.println("getattr: successful " + path + " " + 0);
    return 0;
  }

  // Same as POSIX
  @Override
  public int unlink(String path) {
    // Fail if directory
    if (path.indexOf('/') > 0) {
      System.err.println("unlink: " + path + " is a directory");
      return -ErrorCodes.EISDIR();
    }

    // Fail if file does not exist
    if(!meta().containsKey(path)) {
      System.err.println("unlink: " + path + " not found");
      return -ErrorCodes.ENOENT();
    }

    File_Meta md = meta().get(path);

    // Delete every page for this file in the Crail store
    for(long i = 0; i < md.get_pages(); i++) {
      try {
        store().delete(page_file(path, i), true);
      }
      catch (Exception e) {
        System.err.println("unlink: failed to delete " + page_file(path, i));
        e.printStackTrace();
        return -ErrorCodes.EIO();
      }
    }

    // Delete metadata for this file
    meta.remove(path);

    System.out.println("unlink: successful " + path);
    return 0;
  }

  // Same as POSIX
  @Override
  public int open(String path, FuseFileInfo fi) {
    // Fail if directory
    if (path.indexOf('/') > 0) {
      System.err.println("open: " + path + " is a directory");
      return -ErrorCodes.EISDIR();
    }

    // Fail if file does not exist
    if(!meta().containsKey(path)) {
      System.err.println("open: " + path + " not found");
      return -ErrorCodes.ENOENT();
    }

    // More or less a no-op with some (redundant) safety checks

    System.out.println("open: successful " + path);
    return 0;
  }

  // Same as POSIX creat()
  @Override
  public int create(String path, @mode_t long mode, FuseFileInfo fi) {
    // Fail if directory
    if (path.indexOf('/') > 0) {
      System.err.println("create: " + path + " is a directory");
      return -ErrorCodes.EISDIR();
    }

    // Fail if file already exists
    if(meta().containsKey(path)) {
      System.err.println("create: " + path + " already exists");
      return -ErrorCodes.EEXIST();
    }

    File_Meta md = new File_Meta();

    /* Just create metadata object for this file, write will take care of
       actual data creation
    */
    meta().put(path, md);

    System.err.println("create: successful " + path);
    return 0;
  }

  /* FUSE equivalent to POSIX's close()
     NOTE: If unfamiliar with FUSE, also take a look at FUSE's flush()
  */
  @Override
  public int release(String path, FuseFileInfo fi) {
    // No-op

    return 0;
  }

  // Same as POSIX
  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    // Fail if directory
    if (path.indexOf('/') > 0) {
      System.err.println("read: " + path + " is a directory");
      return -ErrorCodes.EISDIR();
    }

    // Fail if file does not exist
    if(!meta().containsKey(path)) {
      System.err.println("read: " + path + " not found");
      return -ErrorCodes.ENOENT();
    }

    File_Meta md = meta().get(path);

    /* Determine which page to read from. We only read from one page at a time,
       and FUSE will issue another read() each page after the first
    */
    long page = offset / PG_SIZE;
    long off = offset % PG_SIZE;

    // No-op if trying to read beyond the end of the file
    if (page >= md.get_pages()) {
      return 0;
    }

    // Create input stream for the page
    CrailBufferedInputStream instream;
    try {
      instream = store().lookup(page_file(path, page)).get().asKeyValue().getBufferedInputStream(PG_SIZE);
    }
    catch (Exception e) {
      System.err.println("read: failed to open input stream for " + path);
      e.printStackTrace();
      return -ErrorCodes.EIO();
    }

    byte dataBuf[] = new byte[(int)PG_SIZE];

    // Read page from input stream
    long res;
    try {
      res = instream.read(dataBuf);
    }
    catch (IOException e) {
      System.err.println("read: failed to read from stream for " + path);
      e.printStackTrace();
      return -ErrorCodes.EIO();
    }

    // Close input stream
    try {
      instream.close();
    }
    catch (IOException e) {
      System.err.println("read: failed to close stream for " + path + ". Ignoring");
      e.printStackTrace();
    }

    // Determine how much data to move to optput buffer
    if(res < 0) {
      System.err.println("read: failed to read from " + path);
      return -ErrorCodes.EIO();
    }
    else if (res > off) {
      res = Math.min(res - off, size);
      buf.put(0, dataBuf, (int)off, (int)res);
    }
    else {
      res = 0;
    }

    System.out.println("read: successful " + path);
    return (int)res;
  }

  /* Same as POSIX
     NOTE: The Crail API does not allow random writes, and does not allow
     handles on existing files to be opened with write access. To work around
     these limitations, we have implemented the filesystem by breaking each file
     into pages and saving each page as a separate Crail "file" in the
     underlying store. To write data to a page, if the page already exists, the
     old data is read from the Crail store, edited, then written back to the
     store, replacing the old page.
  */
  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    // Fail if directory
    if (path.indexOf('/') > 0) {
      System.err.println("write: " + path + " is a directory");
      return -ErrorCodes.EISDIR();
    }

    // Fail if file does not exist
    if(!meta().containsKey(path)) {
      System.err.println("write: " + path + " not found");
      return -ErrorCodes.ENOENT();
    }

    File_Meta md = meta().get(path);

    /* Determine which page to read from. We only read from one page at a time,
       and FUSE will issue another read() each page after the first
    */
    long page = offset / PG_SIZE;
    long off = offset % PG_SIZE;

    byte dataBuf[] = new byte[(int)PG_SIZE];
    long res;
    Boolean exists = true;
    CrailNode node = null;

    // Determine if a previously written version of the page already exists
    if (page >= md.get_pages()) {
      exists = false;
    }
    else {
      try {
        node = store().lookup(page_file(path, page)).get();
      }
      catch (Exception e) {
        System.err.println("write: failed to find existing page in " + path + " will attempt to create");
        e.printStackTrace();
        exists = false;
      }
    }

    /* If the page already exists, read it into a buffer (nearly identical to
       the code in read())
    */
    if (exists) {
      CrailBufferedInputStream instream;
      try {
        instream = node.asKeyValue().getBufferedInputStream(PG_SIZE);
      }
      catch (Exception e) {
        System.err.println("write: failed to open input stream for " + path);
        e.printStackTrace();
        return -ErrorCodes.EIO();
      }

      try {
        res = instream.read(dataBuf);
      }
      catch (IOException e) {
        System.err.println("write: failed to read from stream for " + path);
        e.printStackTrace();
        return -ErrorCodes.EIO();
      }

      try {
        instream.close();
      }
      catch (IOException e) {
        System.err.println("write: failed to close stream for " + path + ". Ignoring");
        e.printStackTrace();
      }

      if(res < PG_SIZE) {
        System.err.println("write: failed to read from " + path);
        return -ErrorCodes.EIO();
      }
    }

    buf.get(0, dataBuf, (int)off, (int)Math.min(size, PG_SIZE - off));

    /* Create output stream for the page
       NOTE: create() throws an exception if the page file already exists in
       the Crail store. See note before function for why we need to call
       create() The simple solution was to delete any previously existing page
       first. If there's a more elegant solution, should go with that instead.
    */
    CrailBufferedOutputStream outstream;
    try {
      store().delete(page_file(path, page), true);
      outstream = store().create(page_file(path, page), CrailNodeType.KEYVALUE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, false).get().asKeyValue().getBufferedOutputStream(PG_SIZE);
    }
    catch (Exception e) {
      System.err.println("write: failed to open output stream for " + path + "Ignoring");
      e.printStackTrace();
      return -ErrorCodes.EIO();
    }

    // Write page to the output stream
    try {
      outstream.write(dataBuf);
    }
    catch (IOException e) {
      System.err.println("write: failed to write to stream for " + path);
      e.printStackTrace();
      return -ErrorCodes.EIO();
    }

    // Close the output stream
    try {
      outstream.close();
    }
    catch (IOException e) {
      System.err.println("write: failed to close stream for " + path + ". Ignoring");
      e.printStackTrace();
    }

    // Adjust metadata for the file (if necessary)
    md.set_pages(Math.max(page + 1, md.get_pages()));
    md.set_size(md.get_pages() * PG_SIZE);

    System.out.println("write: successful " + path);
    return (int)Math.min(size, PG_SIZE - off);
  }

  // Initializes the fileysystem
  @Override
  public Pointer init(Pointer conn) {
    try {
      // Initialize the underlying Crail store
      CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
      store = CrailStore.newInstance(conf);

      meta = new HashMap<>();
    }
    catch (Exception e) {
      System.err.println("init: failed to initialize context");
      e.printStackTrace();
      System.exit(-1);
    }

    return null;
  }

  // Destroys the filesystem
  @Override
  public void destroy(Pointer initResult) {
    try {
      // Close the underlying Crail store
      store().close();
    }
    catch (Exception e) {
      System.err.println("destroy: failed to close store");
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    CrailFuse stub = new CrailFuse();
    try {
      // Determine mount path
      String path;
      switch (Platform.getNativePlatform().getOS()) {
        case WINDOWS:
          System.exit(-1);
        default:
          if (args.length > 0) {
            path = args[0];
          }
          else {
            path = "/tmp/crail-mount";
          }
      }

      // Launch FUSE
      stub.mount(Paths.get(path), true, true);
    }
    finally {
      stub.umount();
    }
  }
}
