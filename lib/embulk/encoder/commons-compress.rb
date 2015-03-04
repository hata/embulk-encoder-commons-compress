Embulk::JavaPlugin.register_encoder(
  "commons-compress", "org.embulk.encoder.CommonsCompressEncoderPlugin",
  File.expand_path('../../../../classpath', __FILE__))
