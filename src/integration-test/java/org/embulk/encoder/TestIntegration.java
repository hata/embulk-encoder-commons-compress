package org.embulk.encoder;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.junit.Test;

public class TestIntegration {
    static final String TEST_DIR = System.getProperty("embulk.integrationtest.dir");

    private static String getTestFile(String name) {
        return TEST_DIR + File.separator + name;
    }

    @Test
    public void testSolidCompressionFormatTarGz() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles("sample_1.csv"),
                getChecksumFromCompressedFiles("gz", "result_gz_000.00.csv.gz"));
    }

    @Test
    public void testSolidCompressionFormatTarZ() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles("sample_1.csv"),
                getChecksumFromCompressedFiles("bzip2", "result_bz2_000.00.csv.bz2"));
        
        
    }

    @Test
    public void testSolidCompressionFormatZip() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles("sample_1.csv"),
                getChecksumFromArchiveFiles("zip", "result_zip_000.00.csv.zip"));
    }

    private long getChecksumFromFiles(String ... files) throws IOException {
        Checksum cksum = new CRC32();

        for (String srcFile : files) {
            try (BufferedReader reader = new BufferedReader(new FileReader(getTestFile(srcFile)))) {
                getChecksum(cksum, reader);
            }
        }
        
        return cksum.getValue();
    }

    private long getChecksumFromArchiveFiles(String archiveName, String ... files) throws IOException, ArchiveException {
        Checksum cksum = new CRC32();

        ArchiveStreamFactory factory = new ArchiveStreamFactory();

        for (String srcFile : files) {
            ArchiveInputStream in = factory.createArchiveInputStream(archiveName, new FileInputStream(getTestFile(srcFile)));
            while (in.getNextEntry() != null) {
                try(BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                    getChecksum(cksum, reader);
                }
            }
        }
        return cksum.getValue();
    }

    private long getChecksumFromCompressedFiles(String compressName, String ... files) throws IOException, CompressorException {
        Checksum cksum = new CRC32();

        CompressorStreamFactory factory = new CompressorStreamFactory();

        for (String srcFile : files) {
            try(BufferedReader reader = new BufferedReader(new InputStreamReader(
                    factory.createCompressorInputStream(compressName, new FileInputStream(getTestFile(srcFile)))))) {
                getChecksum(cksum, reader);
            }
        }

        return cksum.getValue();
    }

    private long getChecksum(Checksum cksum, BufferedReader reader) throws IOException {
        String line = reader.readLine();
        while (line != null) {
            byte[] lineBuf = line.trim().getBytes();
            if (lineBuf.length > 0) {
                // System.out.println("line:" + new String(lineBuf));
                cksum.update(lineBuf, 0, lineBuf.length);
            }
            line = reader.readLine();
        }
        return cksum.getValue();
    }
}
