package storage

import "io"

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}
