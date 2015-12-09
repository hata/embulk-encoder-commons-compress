package org.embulk.encoder;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.encoder.CommonsCompressEncoderPlugin.PluginTask;
import org.embulk.spi.EncoderPlugin;
import org.embulk.spi.FileOutput;
import org.embulk.spi.util.OutputStreamFileOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCommonsCompressEncoderPlugin
{
    @Mocked
    CommonsCompressEncoderPlugin.PluginTask task;

    @Mocked
    FileOutput output;
    
    @Mocked
    TaskSource taskSource;

    @Mocked
    ConfigSource config;
    
    @Mocked
    EncoderPlugin.Control control;

    CommonsCompressEncoderPlugin plugin;

    @Before
    public void setUp() throws Exception {
        plugin = new CommonsCompressEncoderPlugin();
    }

    @After
    public void tearDown() throws Exception {
        plugin = null;
    }

    @Test
    public void testTransaction() {
        new NonStrictExpectations() {{
            config.loadConfig(PluginTask.class); result = task;
            task.dump(); result = taskSource;
        }};

        plugin.transaction(config, control);
        
        new Verifications() {{
            control.run(taskSource); times = 1;
        }};
    }

    @Test
    public void testOpen() {
        new NonStrictExpectations() {{
            task.getFormat(); result = "gzip";
            taskSource.loadTask(PluginTask.class); result = task;
        }};

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
}
