package index

import "time"

type Index interface {
	Set([]byte, *Value) error
	Get([]byte) (*Value, error)
	Delete([]byte) error
	Size() int
}

type Value struct {
	FilePath  string
	Offset    int64
	Timestamp time.Time
}
