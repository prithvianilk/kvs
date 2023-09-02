package kvs

import (
	"errors"
	"io"
	"kvs/pkg/index"
	"kvs/pkg/index/hashmap"
	"os"
)

const (
	Flags = os.O_RDWR | os.O_CREATE
	Perm  = 0600

	FieldSizeInBytes = 4
)

var ErrEntryNotFound = errors.New("entry not found")

type KVS struct {
	file  *os.File
	index index.Index
}

type Metadata struct {
	isTombstone bool
}

func New(fileName string) (*KVS, error) {
	file, err := os.OpenFile(fileName, Flags, Perm)
	if err != nil {
		return nil, err
	}

	_, err = file.Seek(0, 2)
	if err != nil {
		return nil, err
	}

	kvs := &KVS{file: file, index: hashmap.New()}
	if err := kvs.buildIndex(); err != nil {
		return nil, err
	}
	return kvs, nil
}

func (kvs *KVS) buildIndex() error {
	offset := int64(0)
	for {
		keyOffset := offset

		metadata, err := kvs.readMetadata(offset)
		if err != nil {
			return nil
		}
		offset += 1

		keySize, err := kvs.readFieldSize(offset)
		if err != nil {
			return nil
		}
		offset += int64(FieldSizeInBytes)

		keyAsBytes := make([]byte, keySize)
		if _, err = kvs.file.ReadAt(keyAsBytes, offset); err != nil {
			return nil
		}
		offset += int64(keySize)

		if !metadata.isTombstone {
			if err = kvs.index.Set(keyAsBytes, keyOffset); err != nil {
				return err
			}
		} else {
			if err := kvs.index.Delete(keyAsBytes); err != nil {
				return err
			}
		}

		valueSize, err := kvs.readFieldSize(offset)
		if err != nil {
			return nil
		}
		offset += int64(FieldSizeInBytes + valueSize)
	}
}

func (kvs *KVS) readMetadata(offset int64) (*Metadata, error) {
	buffer := make([]byte, 1)
	if _, err := kvs.file.ReadAt(buffer, offset); err != nil {
		return nil, err
	}

	if buffer[0] == 0 {
		return &Metadata{isTombstone: false}, nil
	}
	return &Metadata{isTombstone: true}, nil
}

func (kvs *KVS) Close() error {
	return kvs.file.Close()
}

func (kvs *KVS) Write(key, value []byte) error {
	offset, err := kvs.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err = kvs.index.Set(key, offset); err != nil {
		return err
	}
	if err := kvs.writeMetadata(&Metadata{isTombstone: false}); err != nil {
		return err
	}
	if err := kvs.writeFieldWithSize(key); err != nil {
		return err
	}
	return kvs.writeFieldWithSize(value)
}

func (kvs *KVS) writeMetadata(metadata *Metadata) error {
	buffer := []byte{0}
	if metadata.isTombstone {
		buffer[0] = 1
	}
	_, err := kvs.file.Write(buffer)
	return err
}

func (kvs *KVS) writeFieldWithSize(field []byte) error {
	size := uint32(len(field))
	sizeAsBytes := kvs.uint32ToBytes(size)
	if _, err := kvs.file.Write(sizeAsBytes); err != nil {
		return err
	}
	_, err := kvs.file.Write(field)
	return err
}

func (kvs *KVS) uint32ToBytes(num uint32) []byte {
	buffer := make([]byte, FieldSizeInBytes)
	mask := uint32((1 << 8) - 1)
	for i := 0; i < FieldSizeInBytes; i++ {
		indexFromEnd := FieldSizeInBytes - 1 - i
		maskedNum := num & mask
		buffer[indexFromEnd] = byte(maskedNum >> (8 * i))
		mask = mask << 8
	}
	return buffer
}

func (kvs *KVS) Read(key []byte) ([]byte, error) {
	offset, err := kvs.index.Get(key)
	if err != nil {
		return nil, ErrEntryNotFound
	}

	offset += 1
	keySize, err := kvs.readFieldSize(offset)
	if err != nil {
		return nil, err
	}
	offset += int64(FieldSizeInBytes + keySize)

	valueSize, err := kvs.readFieldSize(offset)
	if err != nil {
		return nil, err
	}
	offset += int64(FieldSizeInBytes)

	valueAsBytes := make([]byte, valueSize)
	if _, err = kvs.file.ReadAt(valueAsBytes, offset); err != nil {
		return nil, err
	}

	return valueAsBytes, nil
}

func (kvs *KVS) readFieldSize(offset int64) (uint32, error) {
	fieldSizeAsBytes := make([]byte, FieldSizeInBytes)
	_, err := kvs.file.ReadAt(fieldSizeAsBytes, offset)
	if err != nil {
		return 0, err
	}
	return kvs.bytesToUint32(fieldSizeAsBytes), nil
}

func (kvs *KVS) bytesToUint32(buffer []byte) uint32 {
	num := uint32(0)
	for i := 0; i < FieldSizeInBytes; i++ {
		indexFromEnd := FieldSizeInBytes - 1 - i
		mask := uint32(buffer[indexFromEnd]) << (8 * i)
		num = num | mask
	}
	return num
}

func (kvs *KVS) Delete(key []byte) error {
	if _, err := kvs.index.Get(key); err != nil {
		return ErrEntryNotFound
	}
	if err := kvs.index.Delete(key); err != nil {
		return err
	}
	if err := kvs.writeMetadata(&Metadata{isTombstone: true}); err != nil {
		return err
	}
	if err := kvs.writeFieldWithSize(key); err != nil {
		return err
	}
	return kvs.writeFieldWithSize([]byte{})
}
