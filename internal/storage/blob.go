package storage

import (
	"context"
	"fmt"
	"io"
	"strings"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
)

type BlobReader struct {
	ctx    context.Context
	bucket *blob.Bucket
	key    string
	size   int64
	offset int64
}

func NewBlobReader(ctx context.Context, name string) (*BlobReader, error) {
	parts := strings.Split(name, "/")
	if len(parts) < 4 {
		return nil, fmt.Errorf("expected a name in the form <scheme>://<bucket>/<key>")
	}
	var bucketName string
	var key string
	if parts[0] == "file:" {
		bucketName = strings.Join(parts[:len(parts)-1], "/")
		key = parts[len(parts)-1]
	} else {
		bucketName = strings.Join(parts[:3], "/")
		key = strings.Join(parts[3:], "/")
	}

	bucket, err := blob.OpenBucket(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to open bucket %s, %w", bucketName, err)
	}

	attrs, err := bucket.Attributes(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get attributes for %s, %w", name, err)
	}

	reader := &BlobReader{
		ctx:    ctx,
		bucket: bucket,
		key:    key,
		size:   attrs.Size,
	}

	return reader, nil
}

func (r *BlobReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		offset = r.offset + offset
	case io.SeekEnd:
		offset = r.size + offset
	}

	if offset < 0 {
		return 0, fmt.Errorf("attempt to seek to a negative offset: %d", offset)
	}
	r.offset = offset
	return offset, nil
}

func (r *BlobReader) ReadAt(data []byte, offset int64) (int, error) {
	_, err := r.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}
	return r.readFull(data)
}

func (r *BlobReader) Read(data []byte) (int, error) {
	return r.readFull(data)
}

func (r *BlobReader) readFull(data []byte) (int, error) {
	rangeReader, err := r.bucket.NewRangeReader(r.ctx, r.key, r.offset, int64(len(data)), nil)
	if err != nil {
		return 0, err
	}
	defer rangeReader.Close()

	total := 0
	for {
		n, err := rangeReader.Read(data[total:])
		total = total + n
		r.offset += int64(n)
		if total >= len(data) {
			break
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (r *BlobReader) Close() error {
	if err := r.bucket.Close(); err != nil {
		if gcerrors.Code(err) == gcerrors.FailedPrecondition {
			// allow mutiple calls to Close
			return nil
		}
		return err
	}
	return nil
}
