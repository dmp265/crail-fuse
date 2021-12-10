crail-fuse
==

crail-fuse is a very basic mountable FUSE filesystem built on top of an underlying Apache Crail storage system.

## Purpose

This filesystem was created as part of my Cornell MEng. project on disaggregated memory.

## Original Code

This repo was initially a clone of [jnr-fuse](https://github.com/SerCeMan/jnr-fuse), which provides a java API for [libfuse](https://github.com/libfuse/libfuse). NOTE: There may still be invalid leftover files from jnr-fuse.

## Underlying Storage

This filesystem relies on [Apache Crail](https://github.com/apache/incubator-crail), a high-performance distributed data store.

## Building

Please install [gradle](https://gradle.org/) before building.

`./gradlew shadowJar`

## Running

Before attempting to run:
* install Crail
* ensure `$CRAIL_HOME` is set
* add `build/libs/crail-fuse-0.1-shadow.jar` to your class path
* create your mount directory (the default is `/tmp/crail-mount`)
* start Crail (namenode/datanode, e.g.)

`java dmp265.crailfuse.crailFuse [mount dir]`