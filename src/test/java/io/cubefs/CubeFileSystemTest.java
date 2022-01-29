package io.cubefs;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

public class CubeFileSystemTest extends TestCase {
    FsShell shell;
    FileSystem fs;
    Configuration cfg;

    @Override
    protected void setUp() throws Exception {
        cfg = new Configuration();
        cfg.addResource(CubeFileSystemTest.class.getClassLoader().getResourceAsStream("core-site.xml"));
        fs = FileSystem.get(cfg);
        cfg.setQuietMode(false);
        shell = new FsShell(cfg);
    }

    @Override
    protected void tearDown() throws Exception {
        fs.close();
    }


    public void testFsStatus() throws IOException {
        FsStatus st = fs.getStatus();
        System.out.println(st.getCapacity());
        System.out.println(st.getRemaining());
        assertTrue("capacity", st.getCapacity() > 0);
        assertTrue("remaining", st.getRemaining() > 0);
    }

    public void testFileRead()throws IOException{
        FSDataInputStream dfsin = null;
        dfsin = fs.open(new Path("/tmp/test.txt.soft2"));
        IOUtils.copyBytes(dfsin, System.out, 4096, false);
    }

    public void testMkdir() throws IOException {
        Path path = new Path("a/b/c/d");
        assertFalse(fs.exists(path));

        fs.mkdirs(path);
        assertTrue(fs.exists(new Path("a/b/c/d")));
        assertTrue(fs.isDirectory(path));

        fs.mkdirs(new Path("a"));
        assertTrue(fs.exists(new Path("a")));

        fs.mkdirs(new Path("a/b"));
        assertTrue(fs.exists(new Path("a/b/")));

        fs.delete(path);
        assertFalse(fs.exists(path));

    }

    public void testCreateFile() throws IOException {
        Path file = new Path("/a/file.txt");

        fs.create(file);
        assertTrue(fs.exists(file));
        assertTrue(fs.isFile(file));

        fs.delete(file);
        assertFalse(fs.exists(file));
    }

    public void testRename() throws IOException {
        Path src = new Path("/a/b/c");
        fs.mkdirs(src);
        Path dst = new Path("/a/b/d");
        assertTrue(fs.rename(src, dst));

        assertFalse(fs.exists(src));
        assertTrue(fs.exists(dst));

        fs.delete(dst);
    }

    public void testRenameFile() throws IOException {
        Path src = new Path("/a/b/c");
        fs.mkdirs(src);
        Path dst = new Path("/a/b/d");
        fs.mkdirs(dst);
        assertTrue(fs.rename(src, dst));

        assertFalse(fs.exists(src));
        assertTrue(fs.exists(dst));

        fs.delete(dst);
    }

    public void testReadWriter() throws IOException {
        String testString = "Is there anyone out there?";
        String readChars = null;

        FSDataOutputStream dfsOut = null;
        dfsOut = fs.create(new Path("/test1.txt"));
        for(int i=0;i<12000;i++){
            dfsOut.writeUTF(testString+i+"\n");
        }
        dfsOut.close();

        FSDataInputStream dfsin = null;

        dfsin = fs.open(new Path("/test1.txt"));
        readChars = dfsin.readUTF();
        dfsin.close();

        assertEquals(testString, readChars);

        fs.delete(new Path("/test1.txt"), true);

        assertFalse(fs.exists(new Path("/test1")));
    }

    public void testFilesForRelativePath() throws Exception {

        Path subDir1 = new Path("tf_dir.1");
        Path baseDir = new Path("tf_testDirs1");
        Path file1 = new Path("tf_dir.1/foo.1");
        Path file2 = new Path("tf_dir.1/foo.2");

        fs.mkdirs(baseDir);
        assertTrue(fs.isDirectory(baseDir));
//        fs.setWorkingDirectory(baseDir);

        fs.mkdirs(subDir1);

        FSDataOutputStream s1 = fs.create(file1, true, 4096, (short) 1, (long) 4096, null);
        FSDataOutputStream s2 = fs.create(file2, true, 4096, (short) 1, (long) 4096, null);

        s1.close();
        s2.close();

        FileStatus[] p = fs.listStatus(subDir1);
        assertEquals(p.length, 2);

        fs.delete(file1, true);
        p = fs.listStatus(subDir1);
        assertEquals(p.length, 1);

        fs.delete(file2, true);
        p = fs.listStatus(subDir1);
        assertEquals(p.length, 0);

        fs.delete(baseDir, true);
        assertFalse(fs.exists(baseDir));

        fs.delete(subDir1);
        fs.delete(file1);
        fs.delete(file2);
    }

    public void testListStatusFOrAbsentPath() throws IOException {
        Path path = new Path("/bigdir1");
        int len = 10;

        for (int i = 0; i < len; i++) {
            fs.create(new Path("/bigdir1/file_" + i));
        }
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus);
        }

        assertTrue(fileStatuses.length == len);
        fs.delete(path, true);
    }

    public void testFileIO() throws Exception{

        Path subDir1=new Path("tfio_dir.1");
        Path file1=new Path("tfio_dir.1/foo.1");
        Path baseDir=new Path("tfio_testDirs1");

        fs.mkdirs(baseDir);
        assertTrue(fs.isDirectory(baseDir));
        // fs.setWorkingDirectory(baseDir);

        fs.mkdirs(subDir1);

        FSDataOutputStream s1=fs.create(file1, true, 4096, (short) 1, (long) 4096, null);

        int bufsz=4096;
        byte[] data=new byte[bufsz];

        for(int i=0;i<data.length;i++)
            data[i]=(byte) (i%16);

        // write 4 bytes and read them back; read API should return a byte per
        // call
        s1.write(32);
        s1.write(32);
        s1.write(32);
        s1.write(32);
        // write some data
        s1.write(data, 0, data.length);
        // flush out the changes
        s1.close();

        // Read the stuff back and verify it is correct
        FSDataInputStream s2=fs.open(file1, 4096);
        int v;

        v=s2.read();
        assertEquals(v, 32);
        v=s2.read();
        assertEquals(v, 32);
        v=s2.read();
        assertEquals(v, 32);
        v=s2.read();
        assertEquals(v, 32);

//        assertEquals(s2.available(), data.length);

        byte[] buf=new byte[bufsz];
        s2.read(buf, 0, buf.length);
        for(int i=0;i<data.length;i++)
            assertEquals(data[i], buf[i]);

        assertEquals(s2.available(), 0);

        s2.close();

        fs.delete(file1, true);
        assertFalse(fs.exists(file1));
        fs.delete(subDir1, true);
        assertFalse(fs.exists(subDir1));
        fs.delete(baseDir, true);
        assertFalse(fs.exists(baseDir));

        fs.delete(subDir1);
        fs.delete(file1);
        fs.delete(baseDir);

    }


}
