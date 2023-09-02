package hashmap

import (
	"errors"
	"kvs/pkg/index"
)

var ErrKeyNotInMap = errors.New("key not present in map")

type HashMap struct {
	keyToOffsetMap map[string]int64
}

func New() index.Index {
	return &HashMap{keyToOffsetMap: map[string]int64{}}
}

func (index *HashMap) Set(key []byte, offset int64) error {
	index.keyToOffsetMap[string(key)] = offset
	return nil
}

func (index *HashMap) Get(key []byte) (int64, error) {
	offset, ok := index.keyToOffsetMap[string(key)]
	if !ok {
		return 0, ErrKeyNotInMap
	}
	return offset, nil
}

func (index *HashMap) Delete(key []byte) error {
	delete(index.keyToOffsetMap, string(key))
	return nil
}

func (index *HashMap) Size() int {
	return len(index.keyToOffsetMap)
}
