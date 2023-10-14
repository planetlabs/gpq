package storage_test

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/planetlabs/gpq/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randBytes(t *testing.T, size int) []byte {
	data := make([]byte, size)
	n, err := rand.Read(data)
	require.NoError(t, err)
	require.Equal(t, n, size)
	return data
}

func TestNewHttpReader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	r, err := storage.NewReader(context.Background(), server.URL)
	require.NoError(t, err)

	reader, ok := r.(*storage.HttpReader)
	require.True(t, ok)

	assert.NoError(t, reader.Close())
}
