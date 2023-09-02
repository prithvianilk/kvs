package hashmap

import "errors"

var KeyNotInMap = errors.New("key not present in map")

type HashMap struct {
	keyToOffsetMap map[string]int64
}

func New() *HashMap {
	return &HashMap{keyToOffsetMap: map[string]int64{}}
}

func (index *HashMap) Set(key []byte, offset int64) error {
	index.keyToOffsetMap[string(key)] = offset
	return nil
}

func (index *HashMap) Get(key []byte) (int64, error) {
	offset, ok := index.keyToOffsetMap[string(key)]
	if !ok {
		return 0, KeyNotInMap
	}
	return offset, nil
}
