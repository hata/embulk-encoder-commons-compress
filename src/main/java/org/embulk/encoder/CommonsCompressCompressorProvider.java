package org.embulk.encoder;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.embulk.encoder.CommonsCompressEncoderPlugin.PluginTask;
import org.embulk.spi.FileOutput;
import org.embulk.spi.util.FileOutputOutputStream;
import org.embulk.spi.util.OutputStreamFileOutput;


class CommonsCompressCompressorProvider implements OutputStreamFileOutput.Provider {
    private enum CompressorFormat {
        BZIP2, DEFLATE, GZIP;

        static CompressorFormat toCompressorFormat(String format) {
            return CompressorFormat.valueOf(format.toUpperCase());
        }
    }

    private final CompressorFormat format;
    private final FileOutput underlyingFileOutput;
    private final FileOutputOutputStream output;

    static boolean isCompressorFormat(String format) {
        try {
            return CompressorFormat.toCompressorFormat(format) != null;
        } catch (Throwable ignore) {
            // All exceptions are not compressor format.
            return false;
        }
    }
    
    CommonsCompressCompressorProvider(PluginTask task, FileOutput fileOutput) {
        this.format = CompressorFormat.toCompressorFormat(task.getFormat());
        this.underlyingFileOutput = fileOutput;
        this.output = new FileOutputOutputStream(fileOutput, task.getBufferAllocator(), FileOutputOutputStream.CloseMode.FLUSH);
    }

    @Override
    public OutputStream openNext() throws IOException {
        output.nextFile();
        return createCompressorOutputStream();
    }

    @Override
    public void finish() throws IOException {
        output.finish();
    }

    @Override
    public void close() throws IOException {
        output.close();
        underlyingFileOutput.close();
    }
    
    CompressorOutputStream createCompressorOutputStream() throws IOException {
        switch (format) {
        case BZIP2:
            return new BZip2CompressorOutputStream(output);
        case DEFLATE:
            return new DeflateCompressorOutputStream(output);
        case GZIP:
            return new GzipCompressorOutputStream(output);
        }

        // Normally, this exception is not thrown.
        throw new IOException("Unknown format found.");
    }
}
