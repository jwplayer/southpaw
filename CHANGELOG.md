# Change log

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
