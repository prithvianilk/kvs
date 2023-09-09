package rw_lock

import "sync"

// ReaderWriterLock is a lock util for many reader, single writer locking
type ReaderWriterLock struct {
	readMtx     *sync.Mutex
	readerCount int
	writeMtx    *sync.Mutex
}

func New() *ReaderWriterLock {
	return &ReaderWriterLock{readMtx: &sync.Mutex{}, readerCount: 0, writeMtx: &sync.Mutex{}}
}

func (rwl *ReaderWriterLock) OnRead() {
	rwl.readMtx.Lock()
	if rwl.readerCount == 0 {
		rwl.writeMtx.Lock()
	}
	rwl.readerCount++
	rwl.readMtx.Unlock()
}

func (rwl *ReaderWriterLock) OnWrite() {
	rwl.writeMtx.Lock()
}

func (rwl *ReaderWriterLock) OnWriteEnd() {
	rwl.writeMtx.Unlock()
}

func (rwl *ReaderWriterLock) OnReadEnd() {
	rwl.readMtx.Lock()
	rwl.readerCount--
	if rwl.readerCount == 0 {
		rwl.writeMtx.Unlock()
	}
	rwl.readMtx.Unlock()
}
