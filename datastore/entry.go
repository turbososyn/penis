package datastore

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
)

type Entry struct {
	key, value, hash string
}

func (e *Entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.value)
	e.hash = e.calculateHash()
	hl := len(e.hash)
	size := kl + vl + hl + 12
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	copy(res[kl+12:], e.value)
	copy(res[kl+12+vl:], e.hash)
	return res
}

func (e *Entry) GetLength() int64 {
	return getLength(e.key, e.value) + int64(len(e.hash))
}

func (e *Entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	vl := binary.LittleEndian.Uint32(input[kl+8:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+12:kl+12+vl])
	e.value = string(valBuf)

	hl := len(input) - int(kl+12+vl)
	hashBuf := make([]byte, hl)
	copy(hashBuf, input[kl+12+vl:])
	e.hash = string(hashBuf)
}

func (e *Entry) calculateHash() string {
	h := sha1.New()
	h.Write([]byte(e.value))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func readValue(in *bufio.Reader) (string, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", err
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	return string(data), nil
}

func readHash(in *bufio.Reader) (string, error) {
	hashSize := 40
	hashData := make([]byte, hashSize)
	n, err := in.Read(hashData)
	if err != nil {
		return "", err
	}
	if n != hashSize {
		return "", fmt.Errorf("can't read hash bytes (read %d, expected %d)", n, hashSize)
	}

	return string(hashData), nil
}

func getLength(key string, value string) int64 {
	return int64(len(key) + len(value) + 12)
}
