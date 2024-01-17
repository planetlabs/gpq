package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"gocloud.dev/blob"
)

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

var (
	_ ReaderAtSeeker = (*HttpReader)(nil)
	_ ReaderAtSeeker = (*BlobReader)(nil)
)

func NewReader(ctx context.Context, resource string) (ReaderAtSeeker, error) {
	u, err := url.Parse(resource)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}
	if u.Scheme == "http" || u.Scheme == "https" {
		return NewHttpReader(resource)
	}

	blobSchemes := blob.DefaultURLMux().BucketSchemes()
	for _, scheme := range blobSchemes {
		if u.Scheme == scheme {
			return NewBlobReader(ctx, resource)
		}
	}
	return nil, fmt.Errorf("unable to get storage reader for %q scheme", u.Scheme)
}
