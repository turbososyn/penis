package datastore

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	outFileName = "current-data"
	bufSize     = 8192
)

var (
	ErrNotFound     = fmt.Errorf("record does not exist")
	ErrHashMismatch = fmt.Errorf("data integrity check failed")
)

type hashIndex map[string]int64

type KeyPosition struct {
	segment  *Segment
	position int64
}

type Db struct {
	out              *os.File
	dir              string
	segmentSize      int64
	lastSegmentIndex int
	putOps           chan *PutOp
	segments         []*Segment
	mu               sync.RWMutex
	closeOnce        sync.Once
}

type PutOp struct {
	entry Entry
	resp  chan error
}

type Segment struct {
	index    hashIndex
	filePath string
}

func NewDb(dir string, segmentSize int64) (*Db, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	db := &Db{
		dir:              dir,
		segmentSize:      segmentSize,
		putOps:           make(chan *PutOp),
		segments:         make([]*Segment, 0),
		lastSegmentIndex: -1,
	}

	if err := db.recoverAll(); err != nil {
		return nil, err
	}

	if len(db.segments) == 0 {
		if err := db.createSegment(); err != nil {
			return nil, err
		}
	} else {
		lastSegment := db.getLastSegment()
		f, err := os.OpenFile(lastSegment.filePath, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		db.out = f
	}

	go db.startPutRoutine()

	return db, nil
}

func (db *Db) Close() error {
	var err error
	db.closeOnce.Do(func() {
		close(db.putOps)
		if db.out != nil {
			err = db.out.Close()
		}
	})
	return err
}

func (db *Db) startPutRoutine() {
	for op := range db.putOps {
		db.mu.Lock()
		currentSize, err := db.out.Seek(0, io.SeekEnd)
		if err != nil {
			op.resp <- err
			db.mu.Unlock()
			continue
		}

		if currentSize+op.entry.GetLength() > db.segmentSize {
			if err := db.createSegment(); err != nil {
				op.resp <- err
				db.mu.Unlock()
				continue
			}
		}

		n, err := db.out.Write(op.entry.Encode())
		if err == nil {
			db.setKey(op.entry.key, int64(n))
		}
		op.resp <- err
		db.mu.Unlock()
	}
}

func (db *Db) createSegment() error {
	db.lastSegmentIndex++
	filePath := db.generateNewFileName()

	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}

	if db.out != nil {
		db.out.Close()
	}

	db.out = f
	newSegment := &Segment{
		filePath: filePath,
		index:    make(hashIndex),
	}
	db.segments = append(db.segments, newSegment)
	return nil
}

func (db *Db) generateNewFileName() string {
	return filepath.Join(db.dir, fmt.Sprintf("%s%d", outFileName, db.lastSegmentIndex))
}

func (db *Db) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.segments) < 2 {
		return nil
	}
	segmentsToCompact := db.segments[:len(db.segments)-1]
	activeSegment := db.getLastSegment()

	db.lastSegmentIndex++
	newFilePath := db.generateNewFileName()
	newFile, err := os.OpenFile(newFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("compaction failed: cannot create new segment file: %v", err)
	}
	defer newFile.Close()

	newSegment := &Segment{
		filePath: newFilePath,
		index:    make(hashIndex),
	}
	var offset int64

	keysToKeep := make(map[string]KeyPosition)
	for i := len(segmentsToCompact) - 1; i >= 0; i-- {
		s := segmentsToCompact[i]
		for key, pos := range s.index {
			if _, exists := keysToKeep[key]; !exists {
				keysToKeep[key] = KeyPosition{segment: s, position: pos}
			}
		}
	}

	for key, keyPos := range keysToKeep {
		entry, err := keyPos.segment.getFromSegment(keyPos.position)
		if err != nil {
			continue
		}
		entry.key = key
		encoded := entry.Encode()
		n, err := newFile.Write(encoded)
		if err == nil {
			newSegment.index[key] = offset
			offset += int64(n)
		}
	}

	db.segments = []*Segment{newSegment, activeSegment}

	for _, oldSegment := range segmentsToCompact {
		os.Remove(oldSegment.filePath)
	}
	return nil
}

func (db *Db) recoverAll() error {
	files, err := os.ReadDir(db.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var segmentFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), outFileName) {
			segmentFiles = append(segmentFiles, file.Name())
		}
	}

	sort.Slice(segmentFiles, func(i, j int) bool {
		numA, _ := strconv.Atoi(strings.TrimPrefix(segmentFiles[i], outFileName))
		numB, _ := strconv.Atoi(strings.TrimPrefix(segmentFiles[j], outFileName))
		return numA < numB
	})

	for _, fileName := range segmentFiles {
		filePath := filepath.Join(db.dir, fileName)
		segment := &Segment{
			filePath: filePath,
			index:    make(hashIndex),
		}

		if err := db.recoverSegment(segment); err != nil && err != io.EOF {
			return err
		}
		db.segments = append(db.segments, segment)
		index, _ := strconv.Atoi(strings.TrimPrefix(fileName, outFileName))
		if index > db.lastSegmentIndex {
			db.lastSegmentIndex = index
		}
	}
	return nil
}

func (db *Db) recoverSegment(segment *Segment) error {
	f, err := os.Open(segment.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	var offset int64
	reader := bufio.NewReaderSize(f, bufSize)
	for {
		data, err := readNext(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		var e Entry
		e.Decode(data)
		segment.index[e.key] = offset
		offset += int64(len(data))
	}
	return nil
}

func (db *Db) setKey(key string, n int64) {
	currentOffset, _ := db.out.Seek(0, io.SeekEnd)
	startPosition := currentOffset - n
	db.getLastSegment().index[key] = startPosition
}

func (db *Db) getPos(key string) (*KeyPosition, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for i := len(db.segments) - 1; i >= 0; i-- {
		s := db.segments[i]
		if pos, ok := s.index[key]; ok {
			return &KeyPosition{s, pos}, nil
		}
	}
	return nil, ErrNotFound
}

func (db *Db) Get(key string) (string, error) {
	keyPos, err := db.getPos(key)
	if err != nil {
		return "", err
	}
	entry, err := keyPos.segment.getFromSegment(keyPos.position)
	if err != nil {
		return "", err
	}
	if entry.calculateHash() != entry.hash {
		return "", ErrHashMismatch
	}
	return entry.value, nil
}

func (db *Db) Put(key, value string) error {
	resp := make(chan error, 1)
	op := &PutOp{
		entry: Entry{
			key:   key,
			value: value,
			hash:  calculateHash(value),
		},
		resp: resp,
	}
	db.putOps <- op
	return <-op.resp
}

func (db *Db) getLastSegment() *Segment {
	if len(db.segments) == 0 {
		return nil
	}
	return db.segments[len(db.segments)-1]
}

func (s *Segment) getFromSegment(position int64) (Entry, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		return Entry{}, err
	}
	defer file.Close()

	if _, err := file.Seek(position, 0); err != nil {
		return Entry{}, err
	}

	reader := bufio.NewReader(file)
	data, err := readNext(reader)
	if err != nil {
		return Entry{}, err
	}

	var e Entry
	e.Decode(data)
	return e, nil
}

func calculateHash(value string) string {
	h := sha1.New()
	h.Write([]byte(value))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func readNext(r *bufio.Reader) ([]byte, error) {
	szBytes, err := r.Peek(4)
	if err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(szBytes)
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}
