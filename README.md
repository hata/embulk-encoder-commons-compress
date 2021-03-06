# Commons Compress Encoder plugin for Embulk
[![Build Status](https://travis-ci.org/hata/embulk-encoder-commons-compress.svg)](https://travis-ci.org/hata/embulk-encoder-commons-compress)

This encoder plugin supports to encode a format like bzip2 and gzip.

## Overview

* **Plugin type**: encoder
* **Load all or nothing**: yes
* **Resume supported**: no

## Configuration

- **format**: File format like bzip2 and gzip (string, required)

## Example

```yaml
out:
  type: file
  path_prefix: any path
  file_ext: csv.bz2
  formatter:
    type: csv
  encoders:
  - type: commons-compress
    format: bzip2
```

## Build

```
$ ./gradlew gem
```

To run integrationTest
```
$ ./gradlew -DenableIntegrationTest=true gem
```

