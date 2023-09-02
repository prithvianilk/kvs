# KVS

- My second attempt at writing a simple key value store, indexed by a hashmap
- We want to write key value pairs to a log, index them, then perform compaction

### API

- Writes: ```kvs.Write(key, value)```
- Reads: ```kvs.Read(key)```

### Design
- Key value pairs will be stored in this format
```text
key's size (uint32)
key (key's size)
value's size (uint32)
value (value's size)
```

### TODOs
- [ ] Simple writes and reads from log file
- [ ] Hashmap index
- [ ] Mock shell to write and read interactively
- [ ] Compaction 