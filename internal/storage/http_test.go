package storage_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/planetlabs/gpq/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func contentUrl(t *testing.T, content []byte) string {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		http.ServeContent(w, r, "content.txt", time.Time{}, bytes.NewReader([]byte(content)))
	}))
	return server.URL
}

func TestHttpReaderReadAll(t *testing.T) {
	content := randBytes(t, 1000)
	url := contentUrl(t, content)

	reader, err := storage.NewHttpReader(url)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, len(content), len(data))
	assert.Equal(t, content, data)
}

func TestHttpReaderReadAt(t *testing.T) {
	content := randBytes(t, 1000)
	url := contentUrl(t, content)

	httpReader, err := storage.NewHttpReader(url)
	require.NoError(t, err)
	defer func() { _ = httpReader.Close() }()

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
			read, err := httpReader.ReadAt(data, int64(c.offset))
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

func TestHttpReaderSeek(t *testing.T) {
	content := randBytes(t, 1000)
	url := contentUrl(t, content)

	httpReader, err := storage.NewHttpReader(url)
	require.NoError(t, err)
	defer func() { _ = httpReader.Close() }()

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
			offset, seekErr := httpReader.Seek(int64(c.offset), c.whence)
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
				read, readErr := httpReader.Read(data[total:])
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
