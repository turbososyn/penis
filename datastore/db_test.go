package datastore

import (
	"os"
	"strconv"
	"testing"
)

const (
	testDir          = "test-db"
	testSegmentSize  = 100
	testValue        = "test-value"
	testKey          = "test-key"
	testRecordsCount = 20
)

func createTestDb(t *testing.T) (*Db, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", testDir)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	db, err := NewDb(dir, testSegmentSize)
	if err != nil {
		t.Fatalf("Failed to create db: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}

	return db, cleanup
}

func getFilesCount(t *testing.T, dir string) int {
	t.Helper()
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read test directory: %v", err)
	}
	return len(files)
}

func TestDb_Put(t *testing.T) {
	db, cleanup := createTestDb(t)
	defer cleanup()

	for i := 0; i < testRecordsCount; i++ {
		key := testKey + strconv.Itoa(i)
		err := db.Put(key, testValue)
		if err != nil {
			t.Fatalf("Cannot put value to the db: %s", err)
		}
		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("Cannot get value from the db: %s", err)
		}
		if value != testValue {
			t.Errorf("Wrong value received from the db. Expected %s, received %s", testValue, value)
		}
	}

	err := db.Put(testKey+"0", "new-value")
	if err != nil {
		t.Fatalf("Cannot put value to the db: %s", err)
	}
	value, err := db.Get(testKey + "0")
	if err != nil {
		t.Fatalf("Cannot get value from the db: %s", err)
	}
	if value != "new-value" {
		t.Errorf("Wrong value received from the db. Expected %s, received %s", "new-value", value)
	}
}

func TestDb_Segmentation(t *testing.T) {
	t.Run("check starting segmentation", func(t *testing.T) {
		db, cleanup := createTestDb(t)
		defer cleanup()

		for i := 0; i < testRecordsCount; i++ {
			key := testKey + strconv.Itoa(i)
			db.Put(key, testValue)
		}

		actualFiles := getFilesCount(t, db.dir)
		if actualFiles <= 1 {
			t.Errorf("Segmentation did not start. Expected more than 1 file, but got %d", actualFiles)
		}
	})

	t.Run("check compaction and result", func(t *testing.T) {
		db, cleanup := createTestDb(t)
		defer cleanup()

		for i := 0; i < testRecordsCount; i++ {
			key := testKey + strconv.Itoa(i)
			db.Put(key, testValue)
		}

		err := db.Compact()
		if err != nil {
			t.Fatalf("Compaction failed: %v", err)
		}

		expectedFiles := 2
		actualFiles := getFilesCount(t, db.dir)
		if actualFiles != expectedFiles {
			t.Errorf("Incorrect number of files after compaction. Expected %d, but got %d", expectedFiles, actualFiles)
		}

		for i := 0; i < testRecordsCount; i++ {
			key := testKey + strconv.Itoa(i)
			value, err := db.Get(key)
			if err != nil {
				t.Fatalf("Cannot get value from the db after compaction: %s", err)
			}
			if value != testValue {
				t.Errorf("Wrong value received after compaction. Expected %s, received %s", testValue, value)
			}
		}
	})
}
