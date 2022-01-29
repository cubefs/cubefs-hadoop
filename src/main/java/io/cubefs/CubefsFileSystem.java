// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package io.cubefs;


import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/****************************************************************
 * Implement the Hadoop FileSystem API for Cubefs
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class CubefsFileSystem extends FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(CubefsFileSystem.class);
    private Path workingDir;
    private String name;
    private long blockSize;
    private URI uri;
    private CubefsMount cfs;
    private UserGroupInformation ugi;
    private FsPermission fsPermission;
    private String homeDirPrefix = "/user";
    private int bufferSize;
    private int readBufferSize;
    private final static int uMask = 0777;
    private final int maxRetryTimes = 10;

    private final static String CFS_SCHEME_NAME = "cfs";
    private static final DirectBufferPool bufferPool = new DirectBufferPool();

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        setConf(conf);
        if (cfs == null) {
            cfs = new CubefsMount();
        }
        String volumeName = uri.getHost();
        if (StringUtils.isEmpty(volumeName)) {
            throw new IOException("volume name is required.");
        }
        String masterAddress = conf.get(CubefsConfigs.CFS_MASTER_ADDRESS_KEY, CubefsConfigs.CFS_MASTER_ADDRESS_KEY_DEFAULT);
        if (StringUtils.isEmpty(masterAddress)) {
            throw new IOException("master address is required.");
        }
        String logDir = conf.get(CubefsConfigs.CFS_LOG_DIR_KEY, CubefsConfigs.CFS_LOG_DIR_KEY_DEFAULT);
        String logLevel = conf.get(CubefsConfigs.CFS_LOG_LEVEL_KEY, CubefsConfigs.CFS_LOG_LEVEL_KEY_DEFAULT);
        String enableBcache = conf.get(CubefsConfigs.CFS_BCACHE_ENABLE, CubefsConfigs.CFS_BCACHE_ENABLE_DEFAULT);
        String accessKey = conf.get(CubefsConfigs.CFS_ACCESS_KEY);
        if (StringUtils.isEmpty(accessKey)) {
            throw new IOException("ak is required.");
        }
        String secretKey = conf.get(CubefsConfigs.CFS_SECRET_KEY);
        if (StringUtils.isEmpty(secretKey)) {
            throw new IOException("sk is required.");
        }


        cfs.setClient("volName", volumeName);
        cfs.setClient("masterAddr", masterAddress);
        cfs.setClient("logDir", logDir);
        cfs.setClient("logLevel", logLevel);
        cfs.setClient("enableBcache", enableBcache);
        cfs.setClient("accessKey", accessKey);
        cfs.setClient("secretKey", secretKey);

        blockSize = conf.getLong("cfs.block.size", conf.getLong("dfs.blocksize", 128 << 20));
        bufferSize = conf.getInt(CubefsConfigs.CFS_MIN_BUFFER_SIZE, CubefsConfigs.CFS_MIN_BUFFER_SIZE_DEFAULT);
        readBufferSize = conf.getInt(CubefsConfigs.CFS_MIN_READ_BUFFER_SIZE, CubefsConfigs.CFS_MIN_READ_BUFFER_SIZE_DEFAULT);
        this.ugi = UserGroupInformation.getCurrentUser();
        String userName = ugi.getUserName();
        String userGroup = "nogroup";
        if (ArrayUtils.isNotEmpty(ugi.getGroupNames())) {
            userGroup = StringUtils.join(ugi.getGroupNames(), ",");
        }
        String superUser = conf.get(CubefsConfigs.CFS_SUPER_USER_NAME, CubefsConfigs.CFS_SUPER_USER_NAME_DEFAULT);
        String superGroup = conf.get(CubefsConfigs.CFS_SUPER_USER_GROUP, CubefsConfigs.CFS_SUPER_USER_GROUP_DEFAULT);
        homeDirPrefix = conf.get("dfs.user.home.dir.prefix", "/user");
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = getHomeDirectory();
        LOG.debug("configure args: " + conf.toString());

        int ret = cfs.startClient();
        if (ret < 0) {
            throw new IOException(String.format("Chubaofs initialize fail for cfs://%s,code=%s", volumeName, ret));
        }
    }

    @Override
    public String getScheme() {
        return CFS_SCHEME_NAME;
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public long getDefaultBlockSize() {
        return blockSize;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        statistics.incrementBytesRead(1);
        // throws Exception if open fail.
        int fd = cfs.open(formatPath(path), CubefsMount.O_RDONLY, uMask);
        if (fd < 0) {
            throw CubefsMount.error(fd, formatPath(path));
        }
        LOG.debug("Open path: " + path.toString() + " bufferSize: " + bufferSize + " fd:" + fd);
        return new FSDataInputStream(new CubefsInputStream(cfs, statistics, bufferPool, path, fd, checkReadBufferSize(bufferSize)));
    }

    private int checkBufferSize(int bufferSize) {
        return bufferSize < this.bufferSize ? this.bufferSize : bufferSize;
    }

    private int checkReadBufferSize(int bufferSize) {
        return bufferSize < this.readBufferSize ? this.readBufferSize : bufferSize;
    }

    private String formatPath(Path path) {
        return makeQualified(path).toUri().getPath();
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission,
                                     boolean overwrite, int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
        LOG.debug("Create path: " + path.toString() + " permission: "
                + Integer.toHexString((int) permission.toShort()) + " overwrite: " + overwrite + " bufferSize: " + bufferSize
                + " replication: " + replication + " blockSize: " + blockSize + " progress: " + progress);
        statistics.incrementWriteOps(1);

        int flags = CubefsMount.O_WRONLY | CubefsMount.O_CREAT;
        while (true) {
            int fd = cfs.open(formatPath(path), flags, uMask);
            if (fd == CubefsMount.ENOENT) {
                Path parent = path.getParent();
                fsPermission = FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf()));
                try {
                    mkdirs(parent);
                } catch (Exception e) {

                }
                continue;
            }
            if (fd == CubefsMount.EEXIST) {
                if (!overwrite || isDirectory(path)) {
                    throw new FileAlreadyExistsException(formatPath(path));
                }
                flags |= CubefsMount.O_TRUNC;
                continue;
            }
            if (fd < 0) {
                throw CubefsMount.error(fd, formatPath(path));
            }
            return new FSDataOutputStream(new CubefsOutputStream(cfs, statistics, fd, 0L, checkBufferSize(bufferSize)), statistics);
        }

    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable) throws IOException {
        statistics.incrementWriteOps(1);
        LOG.debug("Append path: " + path.toString());
        int fd = cfs.open(formatPath(path), CubefsMount.O_WRONLY | CubefsMount.O_APPEND, uMask);
        if (fd < 0) {
            throw CubefsMount.error(fd, formatPath(path));
        }
        FileStatus status = getFileStatus(path);
        if (status.isDirectory()) {
            throw new FileAlreadyExistsException("Cannot append to directory " + formatPath(path) + "; already exists as a directory.");
        }

        return new FSDataOutputStream(new CubefsOutputStream(cfs, statistics, fd, status.getLen(), checkBufferSize(bufferSize)), statistics);
    }

    /**
     * Rename a file or directory.
     *
     * @param src The current path of the file/directory
     * @param dst The new name for the path.
     * @return true if the rename succeeded, false otherwise.
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        statistics.incrementWriteOps(1);
        if (exists(dst)) {
            return false;
        }
        int r = cfs.rename(formatPath(src), formatPath(dst));
        if (r == CubefsMount.EEXIST) {
            try {
                FileStatus st = getFileStatus(dst);
                if (st.isDirectory()) {
                    dst = new Path(dst, src.getName());
                    r = cfs.rename(formatPath(src), formatPath(dst));
                } else {
                    return false;
                }
            } catch (FileNotFoundException ignored) {
            }

        }
        if (r == CubefsMount.ENOENT || r == CubefsMount.EEXIST) {
            return false;
        }
        if (r < 0) {
            throw CubefsMount.error(r, formatPath(src));
        }
        return true;
    }


    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        statistics.incrementWriteOps(1);
        int r = cfs.unlink(formatPath(path));
        if (r == 0) {
            return true;
        }
        if (r == CubefsMount.ENOENT) {
            return false;
        }
        if (r == CubefsMount.EISDIR) {
            //get directory contents
            FileStatus[] fileStatuses = listStatus(path);
            if (fileStatuses == null) {
                return false;
            }
            if (!recursive && ArrayUtils.isNotEmpty(fileStatuses)) {
                throw new PathIsNotEmptyDirectoryException(path.toString());
            }
            for (FileStatus fs : fileStatuses) {
                if (!delete(fs.getPath(), recursive)) {
                    return false;
                }
            }
        } else {
            CubefsMount.error(r, formatPath(path));
        }
        cfs.rmdir(formatPath(path), true);
        return true;
    }


    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        statistics.incrementReadOps(1);
        int batchSize = 100;
        FileStatus fileStatus = getFileStatus(path);
        if (fileStatus.isFile()) {
            return new FileStatus[]{fileStatus};
        }
        int fd = cfs.open(formatPath(path), CubefsMount.O_RDONLY, uMask);
        if (fd < 0) {
            throw CubefsMount.error(fd, formatPath(path));
        }
        ArrayList<FileStatus> arrayList = new ArrayList<>();

        while (true) {
            CubefsLib.Dirent dirent = new CubefsLib.Dirent();
            CubefsLib.Dirent[] dirents = (CubefsLib.Dirent[]) dirent.toArray(100);

            int count = cfs.readdir(fd, dirents, dirents.length);
            if (count < 0) {
                cfs.close(fd);
                throw new IOException(String.format("readdir fail for %s,code=%s", path, count));
            }
            if (count == 0) {
                break;
            }
            long[] inodes = new long[count];
            Map<Long, String> names = new HashMap<Long, String>(count);
            for (int i = 0; i < count; i++) {
                inodes[i] = dirents[i].ino;
                names.put(dirents[i].ino, new String(dirents[i].name, 0, dirents[i].nameLen, "utf-8"));
            }
            CubefsLib.StatInfo statInfo = new CubefsLib.StatInfo();
            CubefsLib.StatInfo[] statInfos = (CubefsLib.StatInfo[]) statInfo.toArray(count);
            CubefsLib.DirentArray.ByValue direntArray = new CubefsLib.DirentArray.ByValue();

            direntArray.data = statInfos[0].getPointer();
            direntArray.len = batchSize;
            direntArray.cap = batchSize;
            int num = cfs.cfs_batch_get_inodes(fd, inodes, direntArray, count);
            if (num < 0) {
                throw new IOException(String.format("cfs_batch_get_inodes fail for %s,code=%s", path, count));
            }
            for (int i = 0; i < num; i++) {
                statInfos[i].read();
                CubefsLib.StatInfo stat = statInfos[i];
                FileStatus status = new FileStatus(stat.size, isDir(stat.mode),
                        3, blockSize, stat.mtime * 1000 + (long) (stat.mtime_nsec / Math.pow(10, 6)),
                        stat.atime * 1000 + (long) (stat.atime_nsec / Math.pow(10, 6)), new FsPermission((short) stat.mode),
                        System.getProperty("user.name"), System.getProperty("user.name"), new Path(makeQualified(path), names.get(stat.ino)));
                arrayList.add(status);
            }

        }
        FileStatus[] fileStatuses = new FileStatus[arrayList.size()];
        return arrayList.toArray(fileStatuses);
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        workingDir = fixRelativePart(newDir);
        checkPath(workingDir);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }


    @Override
    public Path getHomeDirectory() {
        return makeQualified(new Path(homeDirPrefix + "/" + ugi.getShortUserName()));
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) {
        statistics.incrementWriteOps(1);
        int r = -1;
        if (path == null) {
            throw new IllegalArgumentException("path is required");
        }
        if ("/".equals(formatPath(path))) {
            return true;
        }
        for (int i = 0; i < maxRetryTimes; ++i) {
            try {
                r = cfs.mkdirs(this.formatPath(path), uMask);
                if (r == 0) {
                    break;
                }
                Thread.sleep(100L);
            } catch (InterruptedException e1) {
            } catch (IOException e) {
                LOG.warn("mkdir error, path:" + path + " retry idx: " + i);
            }
        }

        return r == 0 ? true : false;

    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        statistics.incrementReadOps(1);
        CubefsLib.StatInfo stat = new CubefsLib.StatInfo();
        cfs.getAttr(formatPath(path), stat);
        //todo set user-group
        FileStatus status = new FileStatus(stat.size, isDir(stat.mode),
                3, blockSize, stat.mtime * 1000 + (long) (stat.mtime_nsec / Math.pow(10, 6)),
                stat.atime * 1000 + (long) (stat.atime_nsec / Math.pow(10, 6)), new FsPermission((short) stat.mode),
                System.getProperty("user.name"), System.getProperty("user.name"), makeQualified(path));
        return status;
    }

    private boolean isDir(int mode) {
        return (mode & CubefsMount.S_IFDIR) == CubefsMount.S_IFDIR;
    }


    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        if (file == null) {
            return null;
        }
        if (start < 0 || len < 0) {
            throw new IllegalArgumentException("Invalid start or len parameter");
        }
        if (file.getLen() <= start) {
            return new BlockLocation[0];
        }
        ArrayList<BlockLocation> resultList = new ArrayList();
        long totalLen = file.getLen();
        long blockSize = file.getBlockSize();
        long blockStart = 0;
        long end = Math.min(start + len, totalLen);
        boolean isEnd = false;
        while (!isEnd) {
            long blockEnd = blockStart + blockSize;
            if (Math.max(start, blockStart) <= Math.min(end, blockEnd)) {
                String[] name = {"localhost" + blockStart + ":50010"};
                String[] host = {"localhost" + blockStart};
                resultList.add(new BlockLocation(name, host, blockStart, Math.min(blockEnd, totalLen) - blockStart));
            }
            isEnd = blockEnd >= totalLen;
            blockStart = blockEnd;
        }
        BlockLocation[] results = new
                BlockLocation[resultList.size()];
        return resultList.toArray(results);
    }

    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
        //todo
        return super.getContentSummary(f);
    }

    @Override
    protected void checkPath(Path path) {
        return;
    }

    @Override
    public void close() throws IOException {
        super.close();
        cfs.closeClient();
    }
}
