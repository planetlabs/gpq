package geoparquet

import (
	"context"
	"errors"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/apache/arrow/go/v16/parquet/schema"
)

const (
	defaultReadBatchSize = 1024
)

type ReaderConfig struct {
	BatchSize int
	Reader    parquet.ReaderAtSeeker
	File      *file.Reader
	Context   context.Context
}

type RecordReader struct {
	fileReader   *file.Reader
	metadata     *Metadata
	recordReader pqarrow.RecordReader
}

func NewRecordReader(config *ReaderConfig) (*RecordReader, error) {
	batchSize := config.BatchSize
	if batchSize == 0 {
		batchSize = defaultReadBatchSize
	}

	ctx := config.Context
	if ctx == nil {
		ctx = context.Background()
	}

	fileReader := config.File
	if fileReader == nil {
		if config.Reader == nil {
			return nil, errors.New("config must include a File or Reader value")
		}
		fr, frErr := file.NewParquetReader(config.Reader)
		if frErr != nil {
			return nil, frErr
		}
		fileReader = fr
	}

	geoMetadata, geoMetadataErr := GetMetadata(fileReader.MetaData().GetKeyValueMetadata())
	if geoMetadataErr != nil {
		return nil, geoMetadataErr
	}

	arrowReader, arrowErr := pqarrow.NewFileReader(fileReader, pqarrow.ArrowReadProperties{BatchSize: int64(batchSize)}, memory.DefaultAllocator)
	if arrowErr != nil {
		return nil, arrowErr
	}

	recordReader, recordErr := arrowReader.GetRecordReader(ctx, nil, nil)
	if recordErr != nil {
		return nil, recordErr
	}

	reader := &RecordReader{
		fileReader:   fileReader,
		metadata:     geoMetadata,
		recordReader: recordReader,
	}
	return reader, nil
}

func (r *RecordReader) Read() (arrow.Record, error) {
	return r.recordReader.Read()
}

func (r *RecordReader) Metadata() *Metadata {
	return r.metadata
}

func (r *RecordReader) Schema() *schema.Schema {
	return r.fileReader.MetaData().Schema
}

func (r *RecordReader) Close() error {
	r.recordReader.Release()
	return r.fileReader.Close()
}
