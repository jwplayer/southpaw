# Southpaw Base

## Description
This directory contains tooling for building a base image for Southpaw applications.

## Usage
- Extend this image in order to include your Southpaw configs / relations files as well as any additional scripting necessary such as loading secrets

## Versioning

This image is versioned off of the following pattern:

`sp-<southpaw_version>-<image_version>`

- southpaw_version: The version of the southpaw jar
- image_version: The version of the image in the case that the build / run scripts change

## Environment Variables

### Required

- `SOUTHPAW_CONFIG`: The path to your Southpaw configuration file that needs to be mounted or baked into your image
- `SOUTHPAW_RELATIONS_FILE`: The Path to your Southpaw relations file that needs to be mounted or baked into your image

### Optional

- `SOUTHPAW_RUN_OPTS`: Flags for the mode Southpaw should run in (ex: --build / --delete-backup / --debug / etc)
    - Default: --restore --build
- `SOUTHPAW_CLASS_PATH`: Used to change the path where your Southpaw related Jars are located
    - Default: /etc/southpaw/jars
- `SOUTHPAW_LOG4J_OPTS`: Used to modify log4j flags
    - Default: unset
- `JMX_HOSTNAME`: The hostname of your service. Required to be set in order to enable JMX metrics
    - Default: unset
- `JMX_PORT`: The port to expose JMX metrics over
    - Default: 9010
- `JMX_AUTH`: Boolean as to whether to require authentication for JMX connection
    - Default: false
- `JMX_SSL`: Boolean as to whether to enable SSL for JMX connection
    - Default: false
- `HEAP_OPTS`: Used in order to override heap size settings
    - Default: -Xms500m -Xmx500m
- `SENTRY_OPTS`: Used to add any flags for sentry configs

## Advanced Usage

- Extend Southpaw with custom filters / jars
    - If you need to add additional jars to Southpaw's class path just copy / mount them to the `SOUTHPAW_CLASS_PATH` directory.

- Use a custom Southpaw Jar
    - The simplest path forward would be to copy / mount the jar to a new directory and set `SOUTHPAW_CLASS_PATH` to that path.
