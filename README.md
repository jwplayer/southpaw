[![Build Status](https://travis-ci.org/jwplayer/southpaw.svg?branch=master)](https://travis-ci.org/jwplayer/southpaw)

# Southpaw

## Overview

Southpaw is a tool that creates denormalized records from input records based on hierarchical relationships. These relationships are similar to a LEFT OUTER JOIN defined by the following SQL statement:

    SELECT ...
    FROM table_a LEFT OUTER JOIN table_b on a_key = b_key

In this case 'table_b' is a child relationship of 'table_a.' 'a_key' is equivalent to the _parent key_ and 'b_key' is equivalent to the _join key_ in a child relation. Ultimately, one 'table' is the root relation. The record key in each topic for all input and denormalized records is treated as the primary key, which is used by the various indices and within the denormalized entities themselves.

## Why?

While robust tools like Flink or Kafka Streams support joins, they are extremely limited. The typical use case is to enrich a stream of records with another stream that is used as a small lookup table. For Southpaw, we wanted to be able to create denormalized records in a streaming fashion as the input topics receive new records or updates are made to existing records. The results should be similar to running large JOIN queries against a standard SQL DB, but the results should be processed in a streaming fashion.

## How?

Southpaw maintains a state of all records it sees, keeping the latest version of each record. In addition to this, it builds two types of indices. The first type is the parent index. This index tells Southpaw which denormalized records it should create whenever it sees a new or updated child record. The second type of index is the join index. This tells Southpaw which child records to include in an denormalized record when it is being created. WIth these two types of indices, Southpaw can create and recreate the denormalized records as input records are streamed from the input topics.

## Running Southpaw

Southpaw accepts command line arguments and has a help option:

    Option (* = required)  Description                          
    ---------------------  -----------                          
    --build                Builds denormalized records using an
                             existing state.
    * --config             Path to the Southpaw config file
    --debug                Sets logging to DEBUG.
    --delete-backup        Deletes existing backups specified in
                             the config file. BE VERY CAREFUL
                             WITH THIS!!!
    --delete-state         Deletes the existing state specified
                             in the config file. BE VERY CAREFUL
                             WITH THIS!!!
    --help                 Since you are seeing this, you
                             probably know what this is for. :)
    * --relations          Paths to one or more files containing
                             input record relations
    --restore              Restores the state from existing
                             backups.

A typical use would look like this:

    java -cp ./southpaw.jar com.jwplayer.southpaw.Southpaw --config conf/stretch.yaml --relations relations/media.json --restore --build

## Project Structure

* conf - Configuration
* relations - Relation definitions
* src - Java code
  * index - Index classes
  * json - Auto-generated POJO objects created from the JSON schemas
  * record - Record abstractions (e.g. JSON and Avro)
  * serde - Kafka serializers and deserializers (e.g. JSON and Avro)
  * state - State abstraction used for storing indices and data
  * topic - Topic (log) abstractions used for reading and storing records
  * util - utility code

## State

Southpaw uses [RocksDB](http://rocksdb.org/) for its state, an embedded key/value store. RocksDB supports both persistence and backups. Southpaw can sink backups to S3. While RocksDB is currently the only supported state, other states can be added, such as Redis.

### S3 backups

If you specify an S3 URI (using the 's3' scheme) for the rocks.db.backup.uri config option, it will store backups locally under the RocksDB URI. This uses the standard AWS S3 methods for getting the region and credentials as the CLI does (env vars, config file, etc.), so you just need to use one of these methods to be able to store backups in S3.

Links for setting up the credentials and region:
* [Default Credential Provider Chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default)
* [Default Region Provider Chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/java-dg-region-selection.html#automatically-determine-the-aws-region-from-the-environment)

## Relations

Here is an example relations file:

    [
      {
        "DenormalizedName": "DeFeed",
        "Entity": "playlist",
        "Children": [
          {
            "Entity": "user",
            "JoinKey": "user_id",
            "ParentKey": "user_id"
          },
          {
            "Entity": "playlist_tag",
            "JoinKey": "playlist_id",
            "ParentKey": "id",
            "Children": [
              {
                "Entity": "user_tag",
                "JoinKey": "id",
                "ParentKey": "user_tag_id"
              }
            ]
          },
          {
            "Entity": "playlist_custom_params",
            "JoinKey": "playlist_id",
            "ParentKey": "id"
          },
          {
            "Entity": "playlist_media",
            "JoinKey": "playlist_id",
            "ParentKey": "id",
            "Children": [
              {
                "Entity": "media",
                "JoinKey": "id",
                "ParentKey": "media_id"
              }
            ]
          }
        ]
      }
    ]

As specified above, the best way to think about this is as a series of LEFT OUTER JOIN statements. The above would translate to:

    SELECT ...
    FROM
        playlist
        LEFT OUTER JOIN user ON playlist.user_id = user.user_id
        LEFT OUTER JOIN playlist_tag ON playlist.id = playlist_tag.playlist_id
        LEFT OUTER JOIN user_tag ON playlist_tag.user_tag_id = user_tag.id
        LEFT OUTER JOIN playlist_custom_params ON playlist.id = playlist_custom_params.playlist.id
        LEFT OUTER JOIN playlist_media ON playlist.id = playlist_media.playlist_id
        LEFT OUTER JOIN media ON playlist_media.media_id = media.id

The root node in this relationship tree (playilst in the example) is special. It must have a DenormalizedName in addition to an Entity, but it has no ParentKey or JoinKey. Each child node also has an Entity in addition to a ParentKey and JoinKey. Each node (root or child) may or may not have children.  

The Entity and DenormalizedName fields should match corresponding entries under topics in the configuration. This allows different input and output topics to have different configuration. You could even specify different servers for each topic.

You can also specify multiple types of denormalized records in a single file, but a standard use may only create a single type per instance of Southpaw.

## Config

The config is broken up into multiple sections:

### Generic Config

* log.level - The level to log at (e.g. INFO or DEBUG). In the future should just be replaced with a properties file.
* backup.on.shutdown - Instruct Southpaw to backup on shutdown (or not)
* backup.time.s - The amount of time in seconds between backups
* commit.time.s - The amount of time in seconds between full state commits
* create.records.trigger - Number of denormalized record create actions to queue before creating denormalized records. Only queues creation of records when lagging. 
* index.lru.cache.size - The number of index entries to cache in memory 
* index.write.batch.size - The number of entries each index holds in memory before flushing to the state
* topic.lag.trigger - Southpaw will stick to a single topic until it falls below a certain lag threshold before switching to the next topic. This is for performance purposes. This option controls that threshold.

### RocksDB Config

Currently, Southpaw uses RocksDB for its state, though this could be made pluggable in the future. Many of these options correspond directly to RocksDB options. Check the RocksDB documentation for more information.

* rocks.db.backup.uri - Where to store backups. The local file system and S3 is supported.
* rocks.db.backups.auto.rollback (default: false) - Rollback to previous rocksdb backup upon state restoration corruption
* rocks.db.backups.to.keep - # of backups to keep
* rocks.db.compaction.read.ahead.size - Heap allocated to the compaction read ahead process
* rocks.db.max.background.compactions - Number of threads used for background compactions
* rocks.db.max.background.flushes - Number of threads used for background flushes
* rocks.db.max.subcompactions - Number of threads used for subcompactions
* rocks.db.max.write.buffer.number - Number of threads used to flush write buffers
* rocks.db.memtable.size - Heap allocated for RocksDB memtables
* rocks.db.parallelism - Generic number of threads used for a number of RocksDB background processes
* rocks.db.put.batch.size - The number of puts that are batched by the state before automatically committing
* rocks.db.restore.mode - How RocksDB state should be restored on normal startup
    * never - (Default) RocksDB state will never be auto restored on startup
    * always - RocksDB state will attempt to restore from backup on each startup
    * when_needed - RocksDB state will attempt to restore from backup only if a local db cannot be opened
* rocks.db.uri - Location where RocksDB is stored. Only the local file system is supported

### S3 Config (For RocksDB backups to S3)

* aws.s3.access.key.id - AWS access key
* aws.s3.secret.key - AWS secret key
* aws.s3.region - S3 region
* aws.s3.exception.on.error (default: true) - Allows processing to continue even if a sync of RocksDB backups to S3 fails. All exceptions are logged no matter the value of this setting. Disabling this is useful in cases where continuing processing is more important than timely backups to S3.

### Topic Config

Similar to the state, Southpaw is built around Kafka for the log store. The topic config is different from the normal config. All topic config is under the topics entry. Underneath that are one or more sections that should match the entity names of the different normalized entities from the relations file. In addition to those is a "default" section. Each topic created gets its config by taking the default section and then using the section corresponding to its entity as overrides for the default options. Most options come directly from the Kafka consumer/producer config, but there are a few added by Southpaw:

* jackson.serde.class - The full class name of the deserialized object created by the JacksonSerde class
* key.serde.class - The full name of the serde class for the record key
* poll.timeout - The Kafka consumer poll() timeout in milliseconds
* topic.class - The full class name of the class used by the topic
* topic.name - The name of the topic (not the entity name for this topic!)
* value.serde.class - The full name of the serde class for the record value

### Example

    log.level: "INFO"

    backup.time.s: 600
    commit.time.s: 120
    create.records.trigger: 1000000
    index.write.batch.size: 25000
    topic.lag.trigger: 100

    rocks.db.backup.uri: "file:///tmp/RocksDB/southpawBackup"
    rocks.db.backups.to.keep: 5
    rocks.db.compaction.read.ahead.size: 2097152
    rocks.db.memtable.size: 1073741824
    rocks.db.parallelism: 4
    rocks.db.uri: "file:///tmp/RocksDB/southpaw"
    rocks.db.put.batch.size: 25000

    topics:
      default:
        acks: "all"
        auto.offset.reset: "earliest"
        bootstrap.servers: "my-kafka:9092"
        client.id: "southpaw"
        enable.auto.commit: false
        key.serde.class: "com.jwplayer.southpaw.serde.AvroSerde"
        poll.timeout: 100
        schema.registry.url: "http://my-schema-registry:8081"
        topic.class: "com.jwplayer.southpaw.topic.KafkaTopic"
        value.serde.class: "com.jwplayer.southpaw.serde.AvroSerde"
      DeFeed:
        jackson.serde.class: "com.jwplayer.southpaw.json.DenormalizedRecord"
        key.serde.class: "org.apache.kafka.common.serialization.Serdes$ByteArraySerde"
        topic.class: "com.jwplayer.southpaw.topic.BlackHoleTopic"
        topic.name: "discovery.southpaw.feed"
        value.serde.class: "com.jwplayer.southpaw.serde.JacksonSerde"
      media:
        topic.name: "media"
      playlist:
        topic.name: "playlist"
      playlist_custom_params:
        topic.name: "playlist_custom_params"
      playlist_media:
        topic.name: "playlist_media"
      playlist_tag:
        topic.name: "playlist_tag"
      user:
        topic.name: "user"
      user_tag:
        topic.name: "user_tag"

## Denormalized Record

The denormalized record is a hierarchy, similar to the relations that define it. Each node contains a record and its children for the normalized entity. The record field is a map containing all of the fields from the source normalized record. The children is a map of the type of the normalized record named by the entity name of that record from the relations file (since a node can have multiple types of children). Each entry value is a list to support one to many and many to many relationships.

    {
        "Record": {
            "FieldA": "Value1",
            "FieldB": 2,
            ...
        },
        Children": {
            "child_type_1": [{
                "Record": {
                    "FieldA": "Value1",
                    "FieldB": 2,
                    ...
                },
                Children": {}
            },
            ...
            }]
        }
    }

### Example

Example denormalized record:

    {
        "Record": {
            "title": "I'm a playlist!",
            "user_id": 4321,
            "id": 1234,
        },
        "Children": {
            "user": [{
                "Record": {
                    "usage_type": "monthly",
                    "user_id": 4321,
                    "email": "suzy@example.com",
                    "user_name": "Suzy",
                },
                "Children": {}
            }],
            "playlist_custom_params": [{
                "Record": {
                    "playlist_id": 1234,
                    "name": "name",
                    "id": 5678,
                    "value": "value"
                },
                "Children": {}
            }],
            "playlist_tag": [],
            "playlist_media": [{
                "Record": {
                    "pos": 1,
                    "playlist_id": 1234,
                    "media_id": 1,
                    "id": 123
                },
                "Children": {
                    "media": [{
                        "Record": {
                            "title": "I like cats",
                            "user_id": 4321,
                            "id": 1,
                            "status": "ready"
                        },
                        "Children": {}
                    }]
                }
            }, {
                "Record": {
                    "pos": 2,
                    "playlist_id": 1234,
                    "media_id": 2,
                    "id": 124
                },
                "Children": {
                    "media": [{
                        "Record": {
                            "title": "Dogs videos are good",
                            "user_id": 4321,
                            "id": 3,
                            "status": "ready"
                        },
                        "Children": {}
                    }]
                }
            }, {
                "Record": {
                    "pos": 3,
                    "playlist_id": 1234,
                    "media_id": 3,
                    "id": 125
                },
                "Children": {
                    "media": [{
                        "Record": {
                            "title": "This is not an animal video",
                            "user_id": 4321,
                            "id": 3,
                            "status": "ready"
                        },
                        "Children": {}
                    }]
                }
            }]
        }
    }

## Monitoring

Southpaw exposes basic metrics about its operation and performance through JMX under the 'jw.southpaw' domain using the [Drop Wizard](http://metrics.dropwizard.io/3.1.0/getting-started/]) metrics library. The following metrics are exposed:

* backups.created (Timer) - The count and time taken for backup creation  
* backups.deleted (Meter) - The count and rate of backup deletion
* backups.restored (Timer) - The count and time taken for backup restoration
* denormalized.records.created (Meter) - The count and rate for records created
* denormalized.records.created.[RECORD_NAME] (Meter) - Similar to denormalized.records.created, but broken down by the specific type of denormalized record created
* denormalized.records.to.create (Meter) - The count of denormalized records that are queued to be created 
* denormalized.records.to.create.[RECORD_NAME] (Meter) - Similar to denormalized.records.to.create, but broken down by the specific type of denormalized record queued
* filter.deletes.[ENTITY_NAME] (Meter) - The count and rate of input records marked for deletion by the supplied or default filter
* filter.skips.[ENTITY_NAME] (Meter) - The count and rate of input records marked for skipping by the supplied or default filter
* filter.updates.[ENTITY_NAME] (Meter) - The count and rate of input records marked for updating by the supplied or default filter
* records.consumed (Meter) - The count and rate of records consumed from all normalized entity topics
* records.consumed.[ENTITY_NAME] (Meter) - Similar to records.consumer, but broken down by the specific normalized entity
* s3.downloads (Timer) - The count and time taken for state downloads from S3
* s3.files.deleted (Meter) - The count and rate of files deleted in S3
* s3.files.downloaded (Meter) - The count and rate of files downloaded from S3
* s3.files.uploaded (Meter) - The count and rate of files uploaded to S3
* s3.upload.failures (Meter) - The count and rate of failures of backup syncs to S3. Useful if the "aws.s3.exception.on.error" setting is set to false.
* s3.uploads (Timer) - The count and time taken for state uploads to S3
* state.committed (Timer) - The count and time taken for committing the state
* states.deleted (Meter) - The count and rate of state deletion
* topic.lag (Gauge) - Snapshots of the overall lag (end offset - current offset) for the input topics
* topic.lag.[ENTITY_NAME] (Gauge) - Similar to topic.lag, but broken down by the specific normalized entity

## Known Issues

* RocksDB may not work on newer Windows: https://github.com/facebook/rocksdb/issues/2531
