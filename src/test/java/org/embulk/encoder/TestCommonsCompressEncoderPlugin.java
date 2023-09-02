package org.embulk.encoder;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.EncoderPlugin;
import org.embulk.spi.FileOutput;

import org.embulk.util.file.OutputStreamFileOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestCommonsCompressEncoderPlugin
{
    @Rule
    public EmbulkTestRuntime runtime;

    @Mocked
    CommonsCompressEncoderPlugin.PluginTask task;

    @Mocked
    FileOutput output;

    @Mocked
    EncoderPlugin.Control control;

    CommonsCompressEncoderPlugin plugin;

    @Before
    public void setUp() throws Exception {
        plugin = new CommonsCompressEncoderPluginForTest();
        runtime = new EmbulkTestRuntime();
    }

    @After
    public void tearDown() throws Exception {
        plugin = null;
    }

    @Test
    public void testTransaction() {
        ConfigSource config = runtime.getExec().newConfigSource().set("format", "zip");
        plugin.transaction(config, control);
        
        new Verifications() {{
            control.run((TaskSource) any); times = 1;
        }};
    }

    @Test
    public void testOpen() {
        TaskSource taskSource = runtime.getExec().newTaskSource().set("Format", "zip").set("Prefix", "test");
        assertNotNull(plugin.open(taskSource, output));
    }

    @Test
    public void testCreateProvider() {
        new NonStrictExpectations() {{
            task.getFormat(); result = "gzip";
        }};

        OutputStreamFileOutput.Provider provider = plugin.createProvider(task, output);
        assertTrue("Verify a provider instance.", provider instanceof CommonsCompressCompressorProvider);
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

    private class CommonsCompressEncoderPluginForTest extends CommonsCompressEncoderPlugin {
        BufferAllocator bufferAllocator() {
            return new MockBufferAllocator();
        }
    }
}
