# kvs

- My second attempt at writing a simple key value store, indexed by a hashmap
- I want to write key value pairs to a log, index them, then perform compaction
- Provide a [pebble](https://github.com/cockroachdb/pebble) like API and have
  a [bitcask](https://github.com/basho/bitcask) style compaction logic

### CLI

```
Usage of ./kvs <db_name>:
  -compaction-sleep-time int
        Log file compaction worker sleep time in millis (default 10000)
  -max-log-size int
        Log file size threshold in bytes (default 5000)
```

### API

- Writes: ```kvs.Write(key, value)```
- Reads: ```kvs.Read(key)```
- Reads: ```kvs.Delete(key)```

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

### Compaction strategy

- The db will be stored in a directory `./<db_name>`
- The file kvs will be appending to is called `HEAD`
- The index will store a mapping of key -> (log file name, offset, timestamp)
- The writer keeps writing to `HEAD`, and starts writing to a new file (this becomes the new `HEAD`) when it reaches
  threshold `max-log-size`
- A compaction worker thread runs occasionally and aims to concurrently compact all log files
- The compaction worker finds all files other than `HEAD` and adds all keys to a new mapping.
  This will not have all new data, since it does not include `HEAD`.
- Once this is done, we write all key, value pairs to a new set of compaction files, with data from the newly created
  mapping.

### TODOs

- [x] Simple writes and reads from log file
- [x] Hashmap index
- [x] Mock shell to write and read interactively
- [x] Delete support with tombstones
- [x] Make the writes and reads thread safe
- [x] Compaction 