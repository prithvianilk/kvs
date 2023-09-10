# kvs

- My second attempt at writing a simple key value store, indexed by a hashmap
- I want to write key value pairs to a log, index them, then perform compaction
- Provide a leveldb / pebble like API

### API

- Writes: ```kvs.Write(key, value)```
- Reads: ```kvs.Read(key)```

### File format

Key value pairs will be stored in this format

```text
metadata (9 bytes)
key's size (uint32, 4 bytes)
key (key's size bytes)
value's size (uint32, 4 bytes)
value (value's size bytes)


metadata:
1 byte :- is this record a tombstone?
8 bytes :- timestamp
```

### Compaction strategy (wip)

- The db will be stored in a directory `./<db_name>`
- Within this directory, the main log file will always be named `HEAD`
- Files that have not been touched by compaction will be of format `L_*`
- Files that have been compacted will be of format `C_*`
- The index will store a mapping of key -> (log file name, offset, timestamp)
- The writer keeps writing to `HEAD`, and starts writing to a new file when it reaches
  threshold `LogFileSizeThresholdInBytes`
- A compaction worker thread runs occasionally and aims to concurrently compact all log files
- The compaction worker finds all files other than `HEAD` and adds all keys to a new mapping.
  This will not have all new data, since it does not include `HEAD`.
- Once this is done, we write all key, value pairs to a new set of compaction files, with data from the newly created
  mapping.
- We finally lock the db for a temporary while to update the index and delete old files. To prevent us overwriting valid
  data in the index, we use the timestamp to confirm if the new file and offset is the latest value

### TODOs

- [x] Simple writes and reads from log file
- [x] Hashmap index
- [x] Mock shell to write and read interactively
- [x] Delete support with tombstones
- [x] Make the writes and reads thread safe
- [ ] Compaction 