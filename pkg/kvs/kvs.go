package kvs

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/fs"
	"io/ioutil"
	"kvs/pkg/index"
	"kvs/pkg/index/hashmap"
	"kvs/pkg/kvs/config"
	"kvs/pkg/rw_lock"
	"log"
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
	headFileName        string
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

	var headFileName string
	if len(files) == 0 {
		headFileName = uuid.New().String()
		filePath := config.DbName + "/" + headFileName
		f, err := os.OpenFile(filePath, Flags, Perm)
		if err != nil {
			return nil, err
		}
		filePathToFileMap[filePath] = f
	} else {
		headFileName, err = getLatestFileName(files)
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
		headFileName:        headFileName,
	}

	go kvs.startCompactionWorker()
	return kvs, nil
}

func getLatestFileName(files []fs.FileInfo) (string, error) {
	latestFile := files[0]
	for _, file := range files {
		if file.ModTime().After(latestFile.ModTime()) {
			latestFile = file
		}
	}
	return latestFile.Name(), nil
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

func (kvs *KVS) startCompactionWorker() {
	duration := time.Duration(kvs.config.CompactionWorkerSleepTimeInMillis) * time.Millisecond
	ticker := time.NewTicker(duration)
	for range ticker.C {
		if err := kvs.compact(); err != nil {
			log.Printf("error during compaction: %v", err)
		}
	}
}

func (kvs *KVS) compact() error {
	kvs.rwLock.OnWrite()
	defer kvs.rwLock.OnWriteEnd()

	if len(kvs.filePathToFileMap) <= 1 {
		return nil
	}

	var (
		compactionHeadFilePath string
		compactionHeadFile     *os.File
		compactionFiles        []*os.File

		compactionHeadOffset = int64(0)
	)

	for _, key := range kvs.index.Keys() {
		isHeadFileSizeThresholdBreached := int(compactionHeadOffset) >= kvs.config.LogFileSizeThresholdInBytes
		if isHeadFileSizeThresholdBreached || compactionHeadFile == nil {
			compactionHeadFileName := uuid.New().String()
			compactionHeadFilePath = kvs.getFilePath(compactionHeadFileName)
			file, err := os.OpenFile(compactionHeadFilePath, Flags, Perm)
			if err != nil {
				return err
			}
			compactionHeadFile = file
			compactionFiles = append(compactionFiles, compactionHeadFile)
			compactionHeadOffset = 0
		}

		indexValue, err := kvs.index.Get(key)
		if err != nil {
			return err
		}

		if indexValue.FilePath == kvs.getFilePath(kvs.headFileName) {
			continue
		}

		file, ok := kvs.filePathToFileMap[indexValue.FilePath]
		if !ok {
			return fmt.Errorf("file not found for file path: %v", indexValue.FilePath)
		}

		compactionKeyOffset := compactionHeadOffset
		offset := indexValue.Offset

		metadata, err := readMetadata(file, offset)
		if err != nil {
			return err
		}
		compactionHeadOffset += MetadataSizeInBytes
		offset += MetadataSizeInBytes

		keySize, err := readFieldSize(file, offset)
		if err != nil {
			return err
		}
		compactionHeadOffset += FieldSizeInBytes
		offset += FieldSizeInBytes

		keyAsBytes := make([]byte, keySize)
		if _, err = file.ReadAt(keyAsBytes, offset); err != nil {
			return err
		}
		compactionHeadOffset += int64(keySize)
		offset += int64(keySize)

		valueSize, err := readFieldSize(file, offset)
		if err != nil {
			return err
		}
		compactionHeadOffset += FieldSizeInBytes
		offset += FieldSizeInBytes

		value := make([]byte, valueSize)
		if _, err = file.ReadAt(value, offset); err != nil {
			return err
		}
		compactionHeadOffset += int64(valueSize)

		indexValue.Offset = compactionKeyOffset
		indexValue.FilePath = compactionHeadFilePath
		if err := writeMetadata(compactionHeadFile, metadata); err != nil {
			return err
		}
		if err := writeFieldWithSize(compactionHeadFile, key); err != nil {
			return err
		}
		if err := writeFieldWithSize(compactionHeadFile, value); err != nil {
			return err
		}
		if err = kvs.index.Delete(key); err != nil {
			return err
		}
		if err = kvs.index.Set(key, indexValue); err != nil {
			return err
		}
	}

	files, err := ioutil.ReadDir(kvs.config.DbName)
	if err != nil {
		return err
	}

	for _, file := range files {
		filePath := kvs.getFilePath(file.Name())
		if filePath == kvs.getFilePath(kvs.headFileName) {
			continue
		}

		f, ok := kvs.filePathToFileMap[filePath]
		if !ok {
			continue
		}
		if err = f.Close(); err != nil {
			return err
		}
		delete(kvs.filePathToFileMap, filePath)
		if err := os.Remove(filePath); err != nil {
			return err
		}
	}

	for _, file := range compactionFiles {
		kvs.filePathToFileMap[file.Name()] = file
	}

	return nil
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

	file := kvs.getFile(kvs.headFileName)
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	indexValue := &index.Value{Offset: offset, FilePath: kvs.getFilePath(kvs.headFileName), Timestamp: time.Now()}
	if err = kvs.index.Set(key, indexValue); err != nil {
		return err
	}

	metadata := &Metadata{isTombstone: false, timestamp: time.Now()}
	if err := writeMetadata(file, metadata); err != nil {
		return err
	}
	if err := writeFieldWithSize(file, key); err != nil {
		return err
	}
	if err := writeFieldWithSize(file, value); err != nil {
		return err
	}

	kvs.headFileSizeInBytes += MetadataSizeInBytes + FieldSizeInBytes + len(key) + FieldSizeInBytes + len(value)
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
	fileName := uuid.NewString()
	filePath := kvs.getFilePath(fileName)
	f, err := os.OpenFile(filePath, Flags, Perm)
	if err != nil {
		return err
	}
	kvs.filePathToFileMap[filePath] = f
	kvs.headFileName = fileName
	kvs.headFileSizeInBytes = 0
	return nil
}

func writeMetadata(file *os.File, metadata *Metadata) error {
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

func writeFieldWithSize(file *os.File, field []byte) error {
	size := uint32(len(field))
	sizeAsBytes := uint32ToBytes(size)
	if _, err := file.Write(sizeAsBytes); err != nil {
		return err
	}
	_, err := file.Write(field)
	return err
}

func uint32ToBytes(num uint32) []byte {
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
	if _, err := file.ReadAt(fieldSizeAsBytes, offset); err != nil {
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

	file := kvs.getFile(kvs.headFileName)
	if err := writeMetadata(file, &Metadata{isTombstone: true, timestamp: time.Now()}); err != nil {
		return err
	}
	if err := writeFieldWithSize(file, key); err != nil {
		return err
	}
	if err := writeFieldWithSize(file, []byte{}); err != nil {
		return err
	}

	kvs.headFileSizeInBytes += MetadataSizeInBytes + 4 + len(key) + 4
	return kvs.handleHeadFileSizeThresholdBreach()
}
