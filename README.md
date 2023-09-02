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
key's size (uint32)
key (key's size)
value's size (uint32)
value (value's size)
```

### TODOs

- [x] Simple writes and reads from log file
- [x] Hashmap index
- [x] Mock shell to write and read interactively
- [ ] Compaction 