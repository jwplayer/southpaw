# Change log

## 0.7.2
July 12, 2022

### Bug Fixes
* Bump jackson-databind from 2.11.1 to 2.12.6.1 [#106](https://github.com/jwplayer/southpaw/pull/106)
* Upgrades Kafka dependencies and adds Testcontainers to tests [#107](https://github.com/jwplayer/southpaw/pull/107)

## 0.7.1
May 12th, 2022

### Bug Fixes
* Fixes a bug where the Kafka consumer was not correctly seeking past the beginning on multi-partition topics if there were pre-existing offsets [#104](https://github.com/jwplayer/southpaw/pull/104)

## 0.7.0
March 25th, 2022

### Upgrade Notes:
* The support for multi-partition topics is not a breaking change. Existing states are still supported.

### New Features
* Adds support for multi-partition topics [#102](https://github.com/jwplayer/southpaw/pull/102)

### Bug Fixes
* Bump Guava from 29.0-jre to 30.0-jre [#92](https://github.com/jwplayer/southpaw/pull/92) 
* Bump commons-io from 2.6 to 2.7 [#96](https://github.com/jwplayer/southpaw/pull/96)
* Bump snakeyaml from 1.20 to 1.26 [#101](https://github.com/jwplayer/southpaw/pull/101)

## 0.6.1
November 20th, 2020

### Bug Fixes
*  Fix backup path when using an s3 uri [#86](https://github.com/jwplayer/southpaw/pull/86)

## 0.6.0
November 18th, 2020

### Upgrade Notes:
* It is recommended to rebuild southpaw state if there is a chance of a relation containing a parent record with a child relation joining 1,000 or more records

### New Features
* Adds support for building against Java 11 [#65](https://github.com/jwplayer/southpaw/pull/65)
* Adds slf4j bridge dependency for capturing logs from aws sdk [#70](https://github.com/jwplayer/southpaw/pull/70)

### Bug Fixes
* Removal of some unused variables [#69](https://github.com/jwplayer/southpaw/pull/69)
* Fix deprecation warnings [#68](https://github.com/jwplayer/southpaw/pull/68)
* Fixes outdated docs regarding logging [#71](https://github.com/jwplayer/southpaw/pull/71)
* Bump junit from 4.11 to 4.13.1 [#72](https://github.com/jwplayer/southpaw/pull/72)
* Reuse RocksDB BackupEngine object [#67](https://github.com/jwplayer/southpaw/pull/67)
* Add missing jwplayer CODE_OF_CONDUCT.md file [#85](https://github.com/jwplayer/southpaw/pull/85)
* Fix serialization warning and unnecessary conversion of set to array for iteration [#79](https://github.com/jwplayer/southpaw/pull/79)
* Fixes a bug in ByteArraySet chunk iteration that can lead to data loss on large joins of 1,000+ child records [#75](https://github.com/jwplayer/southpaw/pull/75)

## 0.5.3
September 14th, 2020

### New Features
* slf4j/logback [#64](https://github.com/jwplayer/southpaw/pull/64)

## 0.5.2
September 10th, 2020

### New Features
* log4j-core updates [#62](https://github.com/jwplayer/southpaw/pull/62)
* Jackson/Guava updates [#61](https://github.com/jwplayer/southpaw/pull/61)
* Upgrade to log4j2 [#56](https://github.com/jwplayer/southpaw/pull/56)
* Use https for maven central [#55](https://github.com/jwplayer/southpaw/pull/55)

## 0.5.1
August 22nd, 2019

### New Features
* Added new 'time.since.last.backup' metric for a non-sparse backup metric to monitor [#51](https://github.com/jwplayer/southpaw/pull/51)

## 0.5.0
August 9th, 2019

### New Features
* Add --verify-state CLI command for checking integrity of the RocksDB join indices [#45](https://github.com/jwplayer/southpaw/pull/45)

### Bug Fixes
* Update jackson-databind version to fix CVE-2019-12086 [#44](https://github.com/jwplayer/southpaw/pull/44)
* Incrementally flush column families to ensure data integrity [#46](https://github.com/jwplayer/southpaw/pull/46)
* Ensure RocksDB join index and reverse index are idempotent [#47](https://github.com/jwplayer/southpaw/pull/47)
* Add tests for multi-index consistency [#48](https://github.com/jwplayer/southpaw/pull/48)

## 0.4.1
May 14th, 2019

### Bug Fixes
* Cap the size on the fronting set in the ByteArraySet class to prevent uncontrolled growth and OOM errors [#42](https://github.com/jwplayer/southpaw/pull/42) 

## 0.4.0
May 9th, 2019

### New Features
* Add optional setting for configuring RocksDB log level

### Bug Fixes
* Avoid excessive RocksDB column family flushes [#41](https://github.com/jwplayer/southpaw/pull/41) *Note: This change may lead to higher memory utilization than previously experienced as we are flushing memory to disk less often with the trade off of more efficient read/writes*

## 0.3.2
April 30th, 2019

### Bug Fixes
* Keep track of and close all iterators on RocksDBState close [#40](https://github.com/jwplayer/southpaw/pull/40)
* Catch and optionally ignore the proper s3 exceptions [#39](https://github.com/jwplayer/southpaw/pull/39)
* Shutdown threadpools when shutting down RocksDbState instances [#38](https://github.com/jwplayer/southpaw/pull/38)
* Move topic ordering outside the main loop [#34](https://github.com/jwplayer/southpaw/pull/34)
* Add logging around syncFromS3 [#33](https://github.com/jwplayer/southpaw/pull/33)
* Ensure RocksDB closes on exceptions [#32](https://github.com/jwplayer/southpaw/pull/32)

## 0.3.1
April 11th, 2019

### Bug Fixes
* Fix potential memory leak in KafkaTopics.flush() [#31](https://github.com/jwplayer/southpaw/pull/31)

## 0.3.0
April 3rd, 2019

### New Features
* Add support for restore modes [#30](https://github.com/jwplayer/southpaw/pull/30)
* Add optional setting for auto restoring previous rocksdb backup [#23](https://github.com/jwplayer/southpaw/pull/23)

### Bug Fixes
* Cleanup SouthpawTest temp directories [#29](https://github.com/jwplayer/southpaw/pull/29)
* Cleanup temporary directory usage [#28](https://github.com/jwplayer/southpaw/pull/28)
* Remove unused curator-framework dependency[#25](https://github.com/jwplayer/southpaw/pull/25)
* Cleanup tests that write to disk[#24](https://github.com/jwplayer/southpaw/pull/24)
* Cleanup rocksdb options reference handling [#22](https://github.com/jwplayer/southpaw/pull/22)
* Close backup engine when no backups [#21](https://github.com/jwplayer/southpaw/pull/21)

## 0.2.4
March 5th, 2019

* Ensure rocksdb backup engine closes [#20](https://github.com/jwplayer/southpaw/pull/20)

## 0.2.3
February 21st, 2019

* Fixes a thread leak in S3Helper [#19](https://github.com/jwplayer/southpaw/pull/19)

## 0.2.2
February 20th, 2019

* Reduced # of flushes with RocksDB state [#18](https://github.com/jwplayer/southpaw/pull/18)  

## 0.2.1
January 11th, 2019

[#16](https://github.com/jwplayer/southpaw/pull/16)
* Simplified filter functionality 
* Added new metrics for filters
* Made RocksDB S3 syncs for backups run in a background thread
* Added new metrics for S3 functionality
* Added new setting to allow errors in syncs to S3 to not kill Southpaw 

## 0.2.0
January 8th, 2018

* Add support for object change detection in filters [#2](https://github.com/jwplayer/southpaw/pull/2)
* Bump jackson versions [#13](https://github.com/jwplayer/southpaw/pull/13)
* Add travis ci [#12](https://github.com/jwplayer/southpaw/pull/12)
* Ensure rocksdb test directories are created/deleted [#11](https://github.com/jwplayer/southpaw/pull/11)

## 0.1.0

* Initial release
