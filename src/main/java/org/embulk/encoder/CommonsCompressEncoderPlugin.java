package org.embulk.encoder;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.EncoderPlugin;
import org.embulk.spi.FileOutput;
import org.embulk.spi.util.OutputStreamFileOutput;

public class CommonsCompressEncoderPlugin
        implements EncoderPlugin
{
    public interface PluginTask extends Task {
        @Config("format")
        public String getFormat();
        
        @Config("prefix")
        @ConfigDefault("\"entry-\"")
        public String getPrefix();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public void transaction(ConfigSource config, EncoderPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);
        control.run(task.dump());
    }

    @Override
    public FileOutput open(TaskSource taskSource, final FileOutput fileOutput) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new OutputStreamFileOutput(createProvider(task, fileOutput));
    }
    
    OutputStreamFileOutput.Provider createProvider(PluginTask task, FileOutput output) {
        String format = task.getFormat();
        if (CommonsCompressCompressorProvider.isCompressorFormat(format)) {
            return new CommonsCompressCompressorProvider(task, output);
        } else if (CommonsCompressArchiveProvider.isArchiveFormat(format)) {
            return new CommonsCompressArchiveProvider(task, output);
        } else {
            throw new RuntimeException("Unknown format is set format:" + format);
        }
    }
}
