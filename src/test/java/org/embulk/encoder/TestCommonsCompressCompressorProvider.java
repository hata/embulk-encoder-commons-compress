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

import java.io.OutputStream;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.embulk.encoder.CommonsCompressEncoderPlugin.PluginTask;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.FileOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCommonsCompressCompressorProvider {
    @Mocked
    PluginTask task;

    @Mocked
    FileOutput fileOutput;

    CommonsCompressCompressorProvider provider;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        provider = null;
    }

    @Test
    public void testIsCompressorFormat() {
        assertTrue(CommonsCompressCompressorProvider.isCompressorFormat("bzip2"));
        assertTrue(CommonsCompressCompressorProvider.isCompressorFormat("gzip"));
        assertTrue(CommonsCompressCompressorProvider.isCompressorFormat("deflate"));
        assertFalse(CommonsCompressCompressorProvider.isCompressorFormat("tar"));
        assertFalse(CommonsCompressCompressorProvider.isCompressorFormat("zip"));
    }

    @Test
    public void testCommonsCompressCompressorProvider() {
        new NonStrictExpectations() {{
            task.getFormat(); result = "gzip";
            task.getBufferAllocator(); result = new MockBufferAllocator();
        }};

        assertNotNull("Check constructor.", new CommonsCompressCompressorProvider(task, fileOutput));
    }

    @Test
    public void testOpenNext() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "gzip";
            task.getBufferAllocator(); result = new MockBufferAllocator();
        }};

        provider = new CommonsCompressCompressorProvider(task, fileOutput);
        OutputStream out = provider.openNext();
        assertTrue("Verify a stream instance.", out instanceof GzipCompressorOutputStream);
        provider.close();
        
        new Verifications() {{
            fileOutput.nextFile(); times = 1;
        }};
    }

    @Test
    public void testFinish() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "gzip";
            task.getBufferAllocator(); result = new MockBufferAllocator();
        }};

        provider = new CommonsCompressCompressorProvider(task, fileOutput);
        provider.finish();
        provider.close();
        
        new Verifications() {{
            fileOutput.finish(); times = 1;
        }};
    }

    @Test
    public void testClose() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "gzip";
            task.getBufferAllocator(); result = new MockBufferAllocator();
        }};

        provider = new CommonsCompressCompressorProvider(task, fileOutput);
        provider.close();
        
        new Verifications() {{
            fileOutput.close(); times = 1;
        }};
    }

    @Test
    public void testCreateCompressorOutputStreamGzip() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "gzip";
            task.getBufferAllocator(); result = new MockBufferAllocator();
        }};

        provider = new CommonsCompressCompressorProvider(task, fileOutput);
        OutputStream out = provider.createCompressorOutputStream();
        assertTrue("Verify a stream instance.", out instanceof GzipCompressorOutputStream);
        provider.close();
        
        new Verifications() {{
            fileOutput.close(); times = 1;
        }};
    }

    @Test
    public void testCreateCompressorOutputStreamBzip2() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "bzip2";
            task.getBufferAllocator(); result = new MockBufferAllocator();
        }};

        provider = new CommonsCompressCompressorProvider(task, fileOutput);
        OutputStream out = provider.createCompressorOutputStream();
        assertTrue("Verify a stream instance.", out instanceof BZip2CompressorOutputStream);
        provider.close();
        
        new Verifications() {{
            fileOutput.close(); times = 1;
        }};
    }


    @Test
    public void testCreateCompressorOutputStreamDeflate() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "deflate";
            task.getBufferAllocator(); result = new MockBufferAllocator();
        }};

        provider = new CommonsCompressCompressorProvider(task, fileOutput);
        OutputStream out = provider.createCompressorOutputStream();
        assertTrue("Verify a stream instance.", out instanceof DeflateCompressorOutputStream);
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
