package org.embulk.encoder;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.cpio.CpioArchiveEntry;
import org.apache.commons.compress.archivers.cpio.CpioArchiveOutputStream;
import org.apache.commons.compress.archivers.jar.JarArchiveEntry;
import org.apache.commons.compress.archivers.jar.JarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.embulk.encoder.CommonsCompressEncoderPlugin.PluginTask;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.FileOutput;
import org.embulk.util.file.FileOutputOutputStream;
import org.embulk.util.file.OutputStreamFileOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

class CommonsCompressArchiveProvider implements OutputStreamFileOutput.Provider {
    private enum ArchiveFormat {
        CPIO, JAR, TAR, ZIP;

        static ArchiveFormat toArchiveFormat(String format) {
            return ArchiveFormat.valueOf(format.toUpperCase());
        }
    }

    private static final AtomicInteger baseNumSeq = new AtomicInteger(-1);

    private final ArchiveFormat format;
    private final FileOutput underlyingFileOutput;
    private final FileOutputOutputStream output;
    private final String entryNamePrefix;
    private final int baseNum;
    private int count;
    private ArchiveOutputStream archiveOut;
    
    // NOTE: ArchiveEntry should set size to put data. This may use much memory.
    // So, this is not good.
    private ByteArrayOutputStream tmpOut;

    static boolean isArchiveFormat(String format) {
        try {
            return ArchiveFormat.toArchiveFormat(format) != null;
        } catch (Throwable t) {
            // Any exceptions are false.
            return false;
        }
    }

    CommonsCompressArchiveProvider(PluginTask task, FileOutput fileOutput, BufferAllocator bufferAllocator) {
        this.format = ArchiveFormat.toArchiveFormat(task.getFormat());
        this.underlyingFileOutput = fileOutput;
        this.output = new FileOutputOutputStream(fileOutput, bufferAllocator, FileOutputOutputStream.CloseMode.FLUSH);
        this.entryNamePrefix = task.getPrefix();
        this.baseNum = baseNumSeq.incrementAndGet();
    }

    @Override
    public OutputStream openNext() throws IOException {
        output.nextFile();
        return tmpOut = new ByteArrayOutputStream();
    }

    @Override
    public void finish() throws IOException {
        if (tmpOut != null) {
            archiveOut = createArchiveOutputStream();
            archiveOut.putArchiveEntry(createEntry(tmpOut.size()));
            archiveOut.write(tmpOut.toByteArray());
            archiveOut.closeArchiveEntry();
            archiveOut.finish();
            tmpOut = null;
        }
    }

    @Override
    public void close() throws IOException {
        if (tmpOut != null) {
            finish();
        }
        if (archiveOut != null) {
            archiveOut.close();
            archiveOut = null;
        }
        underlyingFileOutput.close();
    }

    ArchiveOutputStream createArchiveOutputStream() throws IOException {
        switch (format) {
        case CPIO:
            return new CpioArchiveOutputStream(output);
        case JAR:
            return new JarArchiveOutputStream(output);
        case TAR:
            return new TarArchiveOutputStream(output);
        case ZIP:
            return new ZipArchiveOutputStream(output);
        }

        // Normally, this code is not called because of the above switch.
        throw new IOException("Format is configured.");
    }
    
    ArchiveEntry createEntry(long size) throws IOException {
        // TODO: How to set entry name and indexes.
        String name =  String.format(entryNamePrefix, baseNum, count++);

        switch (format) {
        case CPIO:
            CpioArchiveEntry cpioEntry = new CpioArchiveEntry(name);
            cpioEntry.setSize(size);
            cpioEntry.setTime(System.currentTimeMillis());
            return cpioEntry;
        case JAR:
            JarArchiveEntry jarEntry = new JarArchiveEntry(name);
            jarEntry.setSize(size);
            jarEntry.setTime(System.currentTimeMillis());
            return jarEntry;
        case TAR:
            TarArchiveEntry tarEntry = new TarArchiveEntry(name);
            tarEntry.setSize(size);
            tarEntry.setModTime(System.currentTimeMillis());
            return tarEntry;
        case ZIP:
            ZipArchiveEntry zipEntry = new ZipArchiveEntry(name);
            zipEntry.setSize(size);
            zipEntry.setTime(System.currentTimeMillis());
            return zipEntry;
        }

        // Normally, this code is not called because of the above switch.
        throw new IOException("Format is configured.");
    }
}
