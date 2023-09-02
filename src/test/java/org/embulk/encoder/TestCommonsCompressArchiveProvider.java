/*
 * $Id$
 * 
 * Copyright 2015 Hiroki Ata
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.embulk.encoder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

import org.apache.commons.compress.archivers.cpio.CpioArchiveOutputStream;
import org.apache.commons.compress.archivers.jar.JarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.embulk.encoder.CommonsCompressEncoderPlugin.PluginTask;
import org.embulk.spi.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

public class TestCommonsCompressArchiveProvider {
    @Mocked
    PluginTask task;

    @Mocked
    FileOutput fileOutput;

    CommonsCompressArchiveProvider provider;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        provider = null;
    }

    @Test
    public void testIsCompressorFormat() {
        assertFalse(CommonsCompressArchiveProvider.isArchiveFormat("bzip2"));
        assertFalse(CommonsCompressArchiveProvider.isArchiveFormat("gzip"));
        assertFalse(CommonsCompressArchiveProvider.isArchiveFormat("deflate"));
        assertTrue(CommonsCompressArchiveProvider.isArchiveFormat("tar"));
        assertTrue(CommonsCompressArchiveProvider.isArchiveFormat("zip"));
        assertTrue(CommonsCompressArchiveProvider.isArchiveFormat("cpio"));
        assertTrue(CommonsCompressArchiveProvider.isArchiveFormat("jar"));
    }

    @Test
    public void testCommonsCompressArchiveProvider() {
        new NonStrictExpectations() {{
            task.getFormat(); result = "zip";
            task.getPrefix(); result = "prefix";
        }};

        assertNotNull("Check constructor.", new CommonsCompressArchiveProvider(task, fileOutput, new MockBufferAllocator()));
    }

    @Test
    public void testOpenNext() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "zip";
            task.getPrefix(); result = "prefix";
        }};

        provider = new CommonsCompressArchiveProvider(task, fileOutput, new MockBufferAllocator());
        OutputStream out = provider.openNext();
        assertTrue("Verify a stream instance.", out instanceof ByteArrayOutputStream);
        provider.close();
        
        new Verifications() {{
            fileOutput.nextFile(); times = 1;
        }};
    }

    @Test
    @Ignore // TODO: bug fix
    public void testFinish() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "zip";
            task.getPrefix(); result = "prefix";
        }};

        provider = new CommonsCompressArchiveProvider(task, fileOutput, new MockBufferAllocator());
        provider.finish();
        provider.close();

        new Verifications() {{
            fileOutput.finish(); times = 1;
        }};
    }


    @Test
    public void testClose() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "zip";
            task.getPrefix(); result = "prefix";
        }};

        provider = new CommonsCompressArchiveProvider(task, fileOutput, new MockBufferAllocator());
        provider.close();
        
        new Verifications() {{
            fileOutput.close(); times = 1;
        }};
    }

    @Test
    public void testCreateCompressorOutputStreamCpio() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "cpio";
            task.getPrefix(); result = "prefix";
        }};

        provider = new CommonsCompressArchiveProvider(task, fileOutput, new MockBufferAllocator());
        OutputStream out = provider.createArchiveOutputStream();
        assertTrue("Verify a stream instance.", out instanceof CpioArchiveOutputStream);
        provider.close();
        
        new Verifications() {{
            fileOutput.close(); times = 1;
        }};
    }

    @Test
    public void testCreateCompressorOutputStreamJar() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "jar";
            task.getPrefix(); result = "prefix";
        }};

        provider = new CommonsCompressArchiveProvider(task, fileOutput, new MockBufferAllocator());
        OutputStream out = provider.createArchiveOutputStream();
        assertTrue("Verify a stream instance.", out instanceof JarArchiveOutputStream);
        provider.close();

        new Verifications() {{
            fileOutput.close(); times = 1;
        }};
    }

    @Test
    public void testCreateCompressorOutputStreamTar() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "tar";
            task.getPrefix(); result = "prefix";
        }};


        provider = new CommonsCompressArchiveProvider(task, fileOutput, new MockBufferAllocator());
        OutputStream out = provider.createArchiveOutputStream();
        assertTrue("Verify a stream instance.", out instanceof TarArchiveOutputStream);
        provider.close();

        new Verifications() {{
            fileOutput.close(); times = 1;
        }};
    }

    @Test
    public void testCreateCompressorOutputStreamZip() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "zip";
            task.getPrefix(); result = "prefix";
        }};

        provider = new CommonsCompressArchiveProvider(task, fileOutput, new MockBufferAllocator());
        OutputStream out = provider.createArchiveOutputStream();
        assertTrue("Verify a stream instance.", out instanceof ZipArchiveOutputStream);
        provider.close();

        new Verifications() {{
            fileOutput.close(); times = 1;
        }};
    }
    private class MockBufferAllocator implements BufferAllocator {
        @Override
        public Buffer allocate() {
            return allocate(8192);
        }

        @Override
        public Buffer allocate(int size) {
            return Buffer.allocate(size);
        }
    }
}
