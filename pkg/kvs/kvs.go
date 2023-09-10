package kvs

import (
	"errors"
	"io"
	"io/fs"
	"io/ioutil"
	"kvs/pkg/index"
	"kvs/pkg/index/hashmap"
	"kvs/pkg/kvs/config"
	"kvs/pkg/rw_lock"
	"os"
	"time"
)

const (
	Flags = os.O_RDWR | os.O_CREATE
	Perm  = 0600

	FieldSizeInBytes    = 4
	MetadataSizeInBytes = 9

	SingleByteMask = (1 << 8) - 1
)

var ErrEntryNotFound = errors.New("entry not found")

type KVS struct {
	config              *config.Config
	index               index.Index
	rwLock              *rw_lock.ReaderWriterLock
	filePathToFileMap   map[string]*os.File
	headFileSizeInBytes int
	currentFileName     string
}

type Metadata struct {
	isTombstone bool
	timestamp   time.Time
}

func New(config *config.Config) (*KVS, error) {
	if err := os.Mkdir(config.DbName, os.ModePerm); err != nil && !os.IsExist(err) {
		return nil, err
	}

	files, err := ioutil.ReadDir(config.DbName)
	if err != nil {
		return nil, err
	}

	idx := hashmap.New()
	filePathToFileMap := map[string]*os.File{}

	for _, file := range files {
		filePath := config.DbName + "/" + file.Name()
		f, err := os.OpenFile(filePath, Flags, Perm)
		if err != nil {
			return nil, err
		}

		filePathToFileMap[filePath] = f

		if _, err = f.Seek(0, 2); err != nil {
			return nil, err
		}

		if err := buildIndex(f, idx); err != nil {
			return nil, err
		}
	}

	var currentFileName string
	if len(files) == 0 {
		currentFileName = time.Now().Format(time.RFC3339)
		filePath := config.DbName + "/" + currentFileName
		f, err := os.OpenFile(filePath, Flags, Perm)
		if err != nil {
			return nil, err
		}
		filePathToFileMap[filePath] = f
	} else {
		currentFileName, err = getLatestFileName(files)
		if err != nil {
			return nil, err
		}
	}

	kvs := &KVS{
		config:              config,
		filePathToFileMap:   filePathToFileMap,
		index:               idx,
		rwLock:              rw_lock.New(),
		headFileSizeInBytes: 0,
		currentFileName:     currentFileName,
	}
	return kvs, nil
}

func getLatestFileName(files []fs.FileInfo) (string, error) {
	var fileName string
	maxDuration := time.Now().Add(-time.Hour * 1e5)
	for _, file := range files {
		t, err := time.Parse(time.RFC3339, file.Name())
		if err != nil {
			return "", err
		}
		if t.After(maxDuration) {
			fileName = file.Name()
		}
	}
	return fileName, nil
}

func buildIndex(file *os.File, idx index.Index) error {
	offset := int64(0)
	for {
		keyOffset := offset

		metadata, err := readMetadata(file, offset)
		if err != nil {
			return nil
		}
		offset += MetadataSizeInBytes

		keySize, err := readFieldSize(file, offset)
		if err != nil {
			return nil
		}
		offset += int64(FieldSizeInBytes)

		keyAsBytes := make([]byte, keySize)
		if _, err = file.ReadAt(keyAsBytes, offset); err != nil {
			return nil
		}
		offset += int64(keySize)

		if !metadata.isTombstone {
			if err = idx.Set(keyAsBytes, &index.Value{FilePath: file.Name(), Offset: keyOffset, Timestamp: metadata.timestamp}); err != nil {
				return err
			}
		} else {
			if err := idx.Delete(keyAsBytes); err != nil {
				return err
			}
		}

		valueSize, err := readFieldSize(file, offset)
		if err != nil {
			return nil
		}
		offset += int64(FieldSizeInBytes + valueSize)
	}
}

func readMetadata(file *os.File, offset int64) (*Metadata, error) {
	buffer := make([]byte, 9)
	if _, err := file.ReadAt(buffer, offset); err != nil {
		return nil, err
	}

	isTombstone := false
	if buffer[0] == 1 {
		isTombstone = true
	}

	timestamp := int64(0)
	for i := 0; i < 8; i++ {
		currentByte := buffer[i+1]
		timestamp |= int64(currentByte) << (8 * i)
	}

	return &Metadata{
		isTombstone: isTombstone,
		timestamp:   time.Unix(timestamp, 0),
	}, nil
}

func (kvs *KVS) Close() error {
	for _, value := range kvs.filePathToFileMap {
		value.Close()
	}
	return nil
}

func (kvs *KVS) getFile(fileName string) *os.File {
	filePath := kvs.getFilePath(fileName)
	return kvs.filePathToFileMap[filePath]
}

func (kvs *KVS) getFilePath(fileName string) string {
	return kvs.config.DbName + "/" + fileName
}

func (kvs *KVS) Write(key, value []byte) error {
	kvs.rwLock.OnWrite()
	defer kvs.rwLock.OnWriteEnd()

	file := kvs.getFile(kvs.currentFileName)
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	indexValue := &index.Value{Offset: offset, FilePath: kvs.getFilePath(kvs.currentFileName), Timestamp: time.Now()}
	if err = kvs.index.Set(key, indexValue); err != nil {
		return err
	}

	metadata := &Metadata{isTombstone: false, timestamp: time.Now()}
	if err := kvs.writeMetadata(file, metadata); err != nil {
		return err
	}

	if err := kvs.writeFieldWithSize(file, key); err != nil {
		return err
	}
	if err := kvs.writeFieldWithSize(file, value); err != nil {
		return err
	}

	kvs.headFileSizeInBytes += MetadataSizeInBytes + 4 + len(key) + 4 + len(value)
	return kvs.handleHeadFileSizeThresholdBreach()
}

func (kvs *KVS) handleHeadFileSizeThresholdBreach() error {
	isHeadFileSizeThresholdBreached := kvs.headFileSizeInBytes >= kvs.config.LogFileSizeThresholdInBytes
	if isHeadFileSizeThresholdBreached {
		return kvs.createNewHeadFile()
	}
	return nil
}

func (kvs *KVS) createNewHeadFile() error {
	fileName := time.Now().Format(time.RFC3339)
	filePath := kvs.config.DbName + "/" + fileName
	f, err := os.OpenFile(filePath, Flags, Perm)
	if err != nil {
		return err
	}
	kvs.filePathToFileMap[filePath] = f
	kvs.currentFileName = fileName
	return nil
}

func (kvs *KVS) writeMetadata(file *os.File, metadata *Metadata) error {
	buffer := make([]byte, 9)
	buffer[0] = 0
	if metadata.isTombstone {
		buffer[0] = 1
	}
	timestampInSecs := metadata.timestamp.Unix()
	mask := int64(SingleByteMask)
	for i := 0; i < 8; i++ {
		currentByte := (timestampInSecs & mask) >> (8 * i)
		buffer[i+1] = byte(currentByte)
		mask = mask << 8
	}
	_, err := file.Write(buffer)
	return err
}

func (kvs *KVS) writeFieldWithSize(file *os.File, field []byte) error {
	size := uint32(len(field))
	sizeAsBytes := kvs.uint32ToBytes(size)
	if _, err := file.Write(sizeAsBytes); err != nil {
		return err
	}
	_, err := file.Write(field)
	return err
}

func (kvs *KVS) uint32ToBytes(num uint32) []byte {
	buffer := make([]byte, FieldSizeInBytes)
	mask := uint32(SingleByteMask)
	for i := 0; i < FieldSizeInBytes; i++ {
		indexFromEnd := FieldSizeInBytes - 1 - i
		maskedNum := num & mask
		buffer[indexFromEnd] = byte(maskedNum >> (8 * i))
		mask = mask << 8
	}
	return buffer
}

func (kvs *KVS) Read(key []byte) ([]byte, error) {
	kvs.rwLock.OnRead()
	defer kvs.rwLock.OnReadEnd()
	value, err := kvs.index.Get(key)
	if err != nil {
		return nil, ErrEntryNotFound
	}

	offset := value.Offset
	file := kvs.filePathToFileMap[value.FilePath]

	offset += MetadataSizeInBytes
	keySize, err := readFieldSize(file, offset)
	if err != nil {
		return nil, err
	}
	offset += int64(FieldSizeInBytes + keySize)

	valueSize, err := readFieldSize(file, offset)
	if err != nil {
		return nil, err
	}
	offset += int64(FieldSizeInBytes)

	valueAsBytes := make([]byte, valueSize)
	if _, err = file.ReadAt(valueAsBytes, offset); err != nil {
		return nil, err
	}

	return valueAsBytes, nil
}

func readFieldSize(file *os.File, offset int64) (uint32, error) {
	fieldSizeAsBytes := make([]byte, FieldSizeInBytes)
	_, err := file.ReadAt(fieldSizeAsBytes, offset)
	if err != nil {
		return 0, err
	}
	return bytesToUint32(fieldSizeAsBytes), nil
}

func bytesToUint32(buffer []byte) uint32 {
	num := uint32(0)
	for i := 0; i < FieldSizeInBytes; i++ {
		indexFromEnd := FieldSizeInBytes - 1 - i
		mask := uint32(buffer[indexFromEnd]) << (8 * i)
		num = num | mask
	}
	return num
}

func (kvs *KVS) Delete(key []byte) error {
	kvs.rwLock.OnWrite()
	defer kvs.rwLock.OnWriteEnd()

	if _, err := kvs.index.Get(key); err != nil {
		return ErrEntryNotFound
	}
	if err := kvs.index.Delete(key); err != nil {
		return err
	}

	file := kvs.getFile(kvs.currentFileName)
	if err := kvs.writeMetadata(file, &Metadata{isTombstone: true, timestamp: time.Now()}); err != nil {
		return err
	}
	if err := kvs.writeFieldWithSize(file, key); err != nil {
		return err
	}
	if err := kvs.writeFieldWithSize(file, []byte{}); err != nil {
		return err
	}

	kvs.headFileSizeInBytes += MetadataSizeInBytes + 4 + len(key) + 4
	return kvs.handleHeadFileSizeThresholdBreach()
}
