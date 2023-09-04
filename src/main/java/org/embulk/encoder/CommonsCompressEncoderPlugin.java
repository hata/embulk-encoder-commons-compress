package org.embulk.encoder;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.EncoderPlugin;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutput;
import org.embulk.util.config.*;
import org.embulk.util.config.modules.TypeModule;
import org.embulk.util.file.OutputStreamFileOutput;

public class CommonsCompressEncoderPlugin
        implements EncoderPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    public interface PluginTask extends Task {
        @Config("format")
        public String getFormat();
        
        @Config("prefix")
        @ConfigDefault("\"result.%1$03d.%1$03d\"")
        public String getPrefix();
    }

    @Override
    public void transaction(ConfigSource config, EncoderPlugin.Control control) {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final PluginTask task = configMapper.map(config, PluginTask.class);
        control.run(task.toTaskSource());
    }

    @Override
    public FileOutput open(TaskSource taskSource, final FileOutput fileOutput) {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        return new OutputStreamFileOutput(createProvider(task, fileOutput));
    }
    
    OutputStreamFileOutput.Provider createProvider(PluginTask task, FileOutput output) {
        String format = task.getFormat();
        if (CommonsCompressCompressorProvider.isCompressorFormat(format)) {
            return new CommonsCompressCompressorProvider(task, output, bufferAllocator());
        } else if (CommonsCompressArchiveProvider.isArchiveFormat(format)) {
            return new CommonsCompressArchiveProvider(task, output, bufferAllocator());
        } else {
            throw new RuntimeException("Unknown format is set format:" + format);
        }
    }

    BufferAllocator bufferAllocator() {
        return Exec.getBufferAllocator();
    }
}
