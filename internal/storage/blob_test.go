package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/planetlabs/gpq/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createFile(t *testing.T, data []byte) string {
	f, err := os.CreateTemp("", "file.txt")
	require.NoError(t, err)

	_, err = f.Write(data)
	require.NoError(t, err)

	require.NoError(t, f.Close())
	return f.Name()
}

func removeFile(t *testing.T, name string) {
	require.NoError(t, os.Remove(name))
}

func TestBlobReaderReadAll(t *testing.T) {
	content := randBytes(t, 1024)
	name := createFile(t, content)
	defer removeFile(t, name)

	reader, err := storage.NewBlobReader(context.Background(), "file://"+name)
	require.NoError(t, err)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	assert.Len(t, data, len(content))
	require.NoError(t, reader.Close())
}

func TestBlobReaderReadAt(t *testing.T) {
	content := randBytes(t, 1000)
	name := createFile(t, content)
	defer removeFile(t, name)

	blobReader, err := storage.NewBlobReader(context.Background(), "file://"+name)
	require.NoError(t, err)
	defer blobReader.Close()

	byteReader := bytes.NewReader(content)

	cases := []struct {
		name   string
		offset int
		size   int
		err    string
	}{
		{
			name:   "first read",
			offset: 700,
			size:   50,
		},
		{
			name:   "second read",
			offset: 10,
			size:   10,
		},
		{
			name:   "offset after end",
			offset: len(content) + 10,
			size:   10,
			err:    io.EOF.Error(),
		},
		{
			name:   "offset near end",
			offset: len(content) - 10,
			size:   20,
			err:    io.EOF.Error(),
		},
		{
			name:   "offset before start",
			offset: -1,
			size:   10,
			err:    "attempt to seek to a negative offset: -1",
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%s (case %d)", c.name, i), func(t *testing.T) {
			data := make([]byte, c.size)
			read, err := blobReader.ReadAt(data, int64(c.offset))
			if c.err == "" {
				require.NoError(t, err)
			}
			if err != nil {
				assert.ErrorContains(t, err, c.err)
			}
			expected := make([]byte, c.size)
			expectedRead, _ := byteReader.ReadAt(expected, int64(c.offset))
			require.Equal(t, expectedRead, read)
			assert.Equal(t, expected[:read], data[:read])
		})
	}
}

func TestBlobReaderSeek(t *testing.T) {
	content := randBytes(t, 1000)
	name := createFile(t, content)

	blobReader, err := storage.NewBlobReader(context.Background(), "file://"+name)
	require.NoError(t, err)
	defer blobReader.Close()

	byteReader := bytes.NewReader(content)

	cases := []struct {
		name   string
		offset int
		whence int
		err    string
	}{
		{
			name:   "seek start",
			offset: 700,
			whence: io.SeekStart,
		},
		{
			name:   "seek current",
			offset: 10,
			whence: io.SeekCurrent,
		},
		{
			name:   "seek end",
			offset: -10,
			whence: io.SeekEnd,
		},
		{
			name:   "offset beyond end",
			offset: 10,
			whence: io.SeekEnd,
		},
		{
			name:   "offset before start",
			offset: -1,
			whence: io.SeekStart,
			err:    "attempt to seek to a negative offset: -1",
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%s (case %d)", c.name, i), func(t *testing.T) {
			data := make([]byte, 10)
			offset, seekErr := blobReader.Seek(int64(c.offset), c.whence)
			if c.err == "" {
				require.NoError(t, seekErr)
				return
			}
			if seekErr != nil {
				require.ErrorContains(t, seekErr, c.err)
				return
			}

			total := 0
			for {
				read, readErr := blobReader.Read(data[total:])
				total += read
				if readErr == io.EOF {
					break
				}
				require.NoError(t, readErr)
			}

			expectedOffset, _ := byteReader.Seek(int64(c.offset), c.whence)
			assert.Equal(t, expectedOffset, offset)

			expected := make([]byte, len(data))
			expectedTotal := 0
			for {
				read, err := byteReader.Read(expected[expectedTotal:])
				expectedTotal += read
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
			}

			assert.Equal(t, expectedTotal, total)
			assert.Equal(t, expected[:total], data[:total])
		})
	}
}
