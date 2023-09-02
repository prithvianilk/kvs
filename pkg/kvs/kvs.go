package kvs

import (
	"errors"
	"io"
	"os"
)

const (
	Flags = os.O_RDWR | os.O_CREATE
	Perm  = 0600

	FieldSizeInBytes = 4
)

var EntryNotFound = errors.New("entry not found")

type KVS struct {
	file *os.File
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

	kvs := &KVS{file: file}
	return kvs, nil
}

func (kvs *KVS) Write(key, value []byte) error {
	if err := kvs.writeFieldWithSize(key); err != nil {
		return err
	}
	return kvs.writeFieldWithSize(value)
}

func (kvs *KVS) writeFieldWithSize(field []byte) error {
	size := uint32(len(field))
	sizeAsBytes := kvs.uint32ToBytes(size)
	_, err := kvs.file.Write(sizeAsBytes)
	if err != nil {
		return err
	}
	_, err = kvs.file.Write(field)
	return err
}

func (kvs *KVS) uint32ToBytes(num uint32) []byte {
	bytes := make([]byte, FieldSizeInBytes)
	mask := uint32((1 << 8) - 1)
	for i := 0; i < FieldSizeInBytes; i++ {
		indexFromEnd := FieldSizeInBytes - 1 - i
		maskedNum := num & mask
		bytes[indexFromEnd] = byte(maskedNum >> (8 * i))
		mask = mask << 8
	}
	return bytes
}

func (kvs *KVS) Read(key []byte) ([]byte, error) {
	offset := int64(0)
	for {
		keySize, err := kvs.readFieldSize(offset)
		if err != nil {
			return nil, kvs.mapError(err)
		}
		offset += int64(FieldSizeInBytes)

		keyAsBytes := make([]byte, keySize)
		_, err = kvs.file.ReadAt(keyAsBytes, offset)
		if err != nil {
			return nil, kvs.mapError(err)
		}
		offset += int64(keySize)

		valueSize, err := kvs.readFieldSize(offset)
		if err != nil {
			return nil, kvs.mapError(err)
		}
		offset += int64(FieldSizeInBytes)

		valueAsBytes := make([]byte, valueSize)
		_, err = kvs.file.ReadAt(valueAsBytes, offset)
		if err != nil {
			return nil, kvs.mapError(err)
		}
		offset += int64(valueSize)

		if isEqual(key, keyAsBytes) {
			return valueAsBytes, nil
		}
	}
}

func (kvs *KVS) mapError(err error) error {
	if err == io.EOF {
		return EntryNotFound
	}
	return err
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

func isEqual(lhs, rhs []byte) bool {
	if len(lhs) != len(rhs) {
		return false
	}

	for i := 0; i < len(lhs); i++ {
		if lhs[i] != rhs[i] {
			return false
		}
	}
	return true
}
