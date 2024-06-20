package pqutil

import (
	"fmt"

	"github.com/apache/arrow/go/v16/parquet/compress"
)

func GetCompression(codec string) (compress.Compression, error) {
	switch codec {
	case "uncompressed":
		return compress.Codecs.Uncompressed, nil
	case "snappy":
		return compress.Codecs.Snappy, nil
	case "gzip":
		return compress.Codecs.Gzip, nil
	case "brotli":
		return compress.Codecs.Brotli, nil
	case "zstd":
		return compress.Codecs.Zstd, nil
	case "lz4":
		return compress.Codecs.Lz4, nil
	default:
		return compress.Codecs.Uncompressed, fmt.Errorf("invalid compression codec %s", codec)
	}
}
