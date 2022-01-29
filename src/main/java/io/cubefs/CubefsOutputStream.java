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


import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class CubefsOutputStream extends OutputStream {
    private boolean closed;
    private int fileHandle;
    private CubefsMount cfs;
    private ByteBuffer bf;
    private long offset = 0;
    private FileSystem.Statistics statistics;
    private static final Logger LOG = LoggerFactory.getLogger(CubefsFileSystem.class);

    public CubefsOutputStream() {
        super();
    }

    public CubefsOutputStream(CubefsMount cfs, FileSystem.Statistics statistics, int fd, long offset, int bufferSize) {
        this.cfs = cfs;
        this.fileHandle = fd;
        this.statistics = statistics;
        this.bf = ByteBuffer.allocate(bufferSize);
        this.offset = offset;
    }

    @Override
    public void flush() throws IOException {
        LOG.debug("flush,fd = " + fileHandle);
        cfsWrite();
        int result = cfs.flush(fileHandle);
        if (result != 0) {
            throw new IOException("flush failed: " + result);
        }
    }

    private void cfsWrite() throws IOException {
        if (bf.position() == 0) {
            return;
        }
        long size = cfs.write(fileHandle, bf.array(), bf.position(), offset);
        statistics.incrementBytesWritten(size);
        if (size != bf.position()) {
            throw new IOException("write failed:" + size);
        }
        offset += bf.position();
        ((Buffer) bf).clear();
    }

    @Override
    public void write(int b) throws IOException {
        bf.put((byte) b);
        if (bf.position() == bf.capacity()) {
            cfsWrite();
        }
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        LOG.debug("write: fd=" + this.fileHandle + " offset=" + off + " length=" + len);
        while (len > 0) {
            int wsize = bf.capacity() - bf.position();
            if (len < wsize) {
                wsize = len;
            }
            bf.put(b, off, wsize);
            off += wsize;
            len -= wsize;
            if (bf.position() == bf.capacity()) {
                cfsWrite();
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        LOG.debug("close,fd = " + fileHandle);
        if (closed) {
            return;
        }
        try {
            flush();
        } finally {
            try {
                this.bf = null;
                closed = true;
                cfs.close(fileHandle);
            } catch (Exception e) {
                throw new IOException("close failed, fd = " + fileHandle);
            }
        }
    }
}
