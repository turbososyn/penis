package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := Entry{key: "key", value: "value"}
	encoded := e.Encode()

	var decoded Entry
	decoded.Decode(encoded)

	if decoded.key != "key" {
		t.Error("incorrect key")
	}
	if decoded.value != "value" {
		t.Error("incorrect value")
	}
	// Точне порівняння хешів
	if decoded.hash != e.hash {
		t.Errorf("incorrect hash: got '%s', want '%s'", decoded.hash, e.hash)
	}
}

func TestReadValue(t *testing.T) {
	e := Entry{key: "key", value: "test-value"}
	data := e.Encode()
	v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v != "test-value" {
		t.Errorf("Got bad value [%s]", v)
	}
}

func TestReadHash(t *testing.T) {
	e := Entry{key: "key", value: "test-value"}
	data := e.Encode()

	buf := bufio.NewReader(bytes.NewReader(data))
	_, err := readValue(buf)
	if err != nil {
		t.Fatal(err)
	}

	hash, err := readHash(buf)
	if err != nil {
		t.Fatal(err)
	}

	expectedHash := e.hash
	if hash != expectedHash {
		t.Errorf("incorrect hash: got '%s', want '%s'", hash, expectedHash)
	}
}
