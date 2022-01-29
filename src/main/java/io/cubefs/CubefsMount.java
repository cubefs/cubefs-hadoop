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

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;


public class CubefsMount {
    public final static int O_RDONLY = 0;
    public final static int O_WRONLY = 1;
    public final static int O_RDWR = 2;
    public final static int O_ACCMODE = 3;
    public final static int O_CREAT = 0100;
    public final static int O_TRUNC = 01000;
    public final static int O_APPEND = 02000;
    public final static int O_DIRECT = 040000;

    public final static int S_IFDIR = 0040000;
    public final static int S_IFREG = 0100000;
    public final static int S_IFLNK = 0120000;

    public final static int DT_UNKNOWN = 0x0;
    public final static int DT_DIR = 0x4;
    public final static int DT_REG = 0x8;
    public final static int DT_LNK = 0xa;


    public final static int SETATTR_MODE = 1;
    public final static int SETATTR_UID = 2;
    public final static int SETATTR_GID = 4;
    public final static int SETATTR_MTIME = 8;
    public final static int SETATTR_ATIME = 16;

    public final static int EIO = -5;
    public final static int ENOENT = -2;
    public final static int EACCESS = -0xd;
    public final static int EEXIST = -0x11;
    public final static int ENOTDIR = -0x14;
    public final static int EISDIR = -0x15;
    public final static int EINVAL = -0x16;
    public final static int ENOSPACE = -0x1c;
    public final static int EROFS = -0x1e;


    private CubefsLib libcfs;
    private long cid;
    private static final Logger LOGGER = LoggerFactory.getLogger(CubefsFileSystem.class);

    public CubefsMount() {
        libcfs = (CubefsLib) Native.load("libcfs.so", CubefsLib.class);
        cid = libcfs.cfs_new_client();
    }

    public int setClient(String key, String val) throws IOException {
        int r = libcfs.cfs_set_client(this.cid, key, val);
        if (r < 0) {
            throw error(r, key);
        }
        return r;
    }

    public static IOException error(int errno, String p) {
        if (errno == ENOTDIR) {
            return new ParentNotDirectoryException();
        } else if (errno == ENOENT) {
            return new FileNotFoundException("No such file or directory: " + p);

        } else if (errno == EACCESS) {
            return new AccessControlException("Permission denied: " + p);
        } else if (errno == EEXIST) {
            return new FileAlreadyExistsException();
        } else if (errno == EINVAL) {
            return new InvalidRequestException("Invalid parameter");
        } else if (errno == ENOSPACE) {
            return new IOException("No space");
        } else if (errno == EROFS) {
            return new IOException("Read-only Filesystem");
        } else if (errno == EIO) {
            return new IOException("I/O error: " + p);
        } else {
            return new IOException("errno: " + errno + " ;path=" + p);
        }
    }

    public int startClient() {
        return libcfs.cfs_start_client(this.cid);
    }

    public void closeClient() throws IOException {
        libcfs.cfs_close_client(this.cid);

    }

    public int chdir(String path) throws IOException {
        int r = libcfs.cfs_chdir(this.cid, path);
        if (r < 0) {
            throw error(r, path);
        }
        return r;
    }

    public String getcwd() throws IOException {
        return libcfs.cfs_getcwd(this.cid);
    }

    public int getAttr(String path, CubefsLib.StatInfo stat) throws IOException {
        int r = libcfs.cfs_getattr(this.cid, path, stat);
        if (r < 0) {
            throw error(r, path);
        }
        return r;
    }

    public int setAttr(String path, CubefsLib.StatInfo stat, int mask) throws IOException {
        int r = libcfs.cfs_setattr(this.cid, path, stat, mask);
        if (r < 0) {
            throw error(r, path);
        }
        return r;
    }

    public int open(String path, int flags, int mode) throws IOException {
        int r = libcfs.cfs_open(this.cid, path, flags, mode, 0, 0);
//        if (r < 0) {
//            throw error(r, path);
//        }
        return r;
    }

    public void close(int fd) {
        libcfs.cfs_close(this.cid, fd);
    }

    public long write(int fd, Pointer buf, long size, long offset) {
        return libcfs.cfs_write(this.cid, fd, buf, size, offset);

    }

    public long write(int fd, byte[] buf, long size, long offset) {
        return libcfs.cfs_write(this.cid, fd, buf, size, offset);

    }

    public int flush(int fd) {
        return libcfs.cfs_flush(this.cid, fd);
    }

    public long read(int fd, Pointer buf, long size, long offset, String path) throws IOException {
        long r = libcfs.cfs_read(this.cid, fd, buf, size, offset);
        if (r < 0) {
            throw error((int) r, path);
        }
        return r;
    }

    public int rename(String from, String to) throws IOException {
        int r = libcfs.cfs_rename(this.cid, from, to);
        return r;
    }

    /*
     * Note that the memory allocated for Dirent[] must be countinuous. For example,
     * (new Dirent()).toArray(count).
     */
    public int readdir(int fd, CubefsLib.Dirent[] dents, int count) {
        Pointer arr = dents[0].getPointer();
        CubefsLib.DirentArray.ByValue slice = new CubefsLib.DirentArray.ByValue();
        slice.data = arr;
        slice.len = (long) count;
        slice.cap = (long) count;

        long arrSize = libcfs.cfs_readdir(this.cid, fd, slice, count);
        if (arrSize > 0) {
            for (int i = 0; i < (int) arrSize; i++) {
                dents[i].read();
            }
        }

        return (int) arrSize;
    }

    public int fchmod(int fd, int mode) throws IOException {
        return libcfs.cfs_fchmod(this.cid, fd, mode);

    }

    public int unlink(String path) throws IOException {
        int r = libcfs.cfs_unlink(cid, path);
        return r;
    }

    public int rmdir(String path, boolean recursive) throws IOException {
        int r = libcfs.cfs_rmdir(cid, path, recursive);
        if (r < 0) {
            throw error(r, path);
        }
        return r;
    }

    public int cfs_batch_get_inodes(int fd, long[] inodes, CubefsLib.DirentArray.ByValue direntArray, int count) {
        return libcfs.cfs_batch_get_inodes(cid, fd, inodes, direntArray, count);
    }

    public int mkdirs(String path, int mode) throws IOException {
        int r = libcfs.cfs_mkdirs(cid, path, mode);
        if (r < 0) {
            throw error(r, path);
        }
        return r;
    }
}

