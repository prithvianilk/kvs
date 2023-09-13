package hashmap

import (
	"errors"
	"kvs/pkg/index"
)

var ErrKeyNotInMap = errors.New("key not present in map")

type HashMap struct {
	keyToOffsetMap map[string]*index.Value
}

func New() index.Index {
	return &HashMap{keyToOffsetMap: map[string]*index.Value{}}
}

func (index *HashMap) Set(key []byte, value *index.Value) error {
	index.keyToOffsetMap[string(key)] = value
	return nil
}

func (index *HashMap) Get(key []byte) (*index.Value, error) {
	value, ok := index.keyToOffsetMap[string(key)]
	if !ok {
		return nil, ErrKeyNotInMap
	}
	return value, nil
}

func (index *HashMap) Delete(key []byte) error {
	delete(index.keyToOffsetMap, string(key))
	return nil
}

func (index *HashMap) Size() int {
	return len(index.keyToOffsetMap)
}

func (index *HashMap) Keys() [][]byte {
	var keys [][]byte
	for key, _ := range index.keyToOffsetMap {
		keys = append(keys, []byte(key))
	}
	return keys
}
