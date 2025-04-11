package geoparquet

import (
	"context"
	"errors"
	"fmt"
	"slices"

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
	Columns   []int
	RowGroups []int
}

type RecordReader struct {
	fileReader   *file.Reader
	metadata     *Metadata
	recordReader pqarrow.RecordReader
}

func NewParquetFileReader(config *ReaderConfig) (*file.Reader, error) {
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
	return fileReader, nil
}

func NewArrowFileReader(config *ReaderConfig, parquetReader *file.Reader) (*pqarrow.FileReader, error) {
	batchSize := config.BatchSize
	if batchSize == 0 {
		batchSize = defaultReadBatchSize
	}

	return pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: int64(batchSize)}, memory.DefaultAllocator)
}

func NewRecordReaderFromConfig(config *ReaderConfig) (*RecordReader, error) {
	parquetFileReader, err := NewParquetFileReader(config)
	if err != nil {
		return nil, fmt.Errorf("could not get ParquetFileReader: %w", err)
	}

	arrowFileReader, err := NewArrowFileReader(config, parquetFileReader)
	if err != nil {
		return nil, fmt.Errorf("could not get ArrowFileReader: %w", err)
	}

	geoMetadata, err := GetMetadataFromFileReader(parquetFileReader)
	if err != nil {
		return nil, fmt.Errorf("could not get geo metadata from file reader: %w", err)
	}

	ctx := config.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if config.Columns != nil {
		primaryGeomColIdx := parquetFileReader.MetaData().Schema.ColumnIndexByName(geoMetadata.PrimaryColumn)

		if !slices.Contains(config.Columns, primaryGeomColIdx) {
			return nil, fmt.Errorf("columns must include primary geometry column '%v' (index %v)", geoMetadata.PrimaryColumn, primaryGeomColIdx)
		}
	}

	if config.Columns != nil && len(config.Columns) == 0 {
		config.Columns = nil
	}

	if config.RowGroups != nil && len(config.RowGroups) == 0 {
		config.RowGroups = nil
	}

	recordReader, recordErr := arrowFileReader.GetRecordReader(ctx, config.Columns, config.RowGroups)

	if recordErr != nil {
		return nil, recordErr
	}

	reader := &RecordReader{
		fileReader:   arrowFileReader.ParquetReader(),
		metadata:     geoMetadata,
		recordReader: recordReader,
	}
	return reader, nil
}

func NewRecordReader(ctx context.Context, arrowFileReader *pqarrow.FileReader, geoMetadata *Metadata, columns []int, rowGroups []int) (*RecordReader, error) {
	if columns != nil || len(columns) != 0 {
		primaryGeomColIdx := arrowFileReader.ParquetReader().MetaData().Schema.ColumnIndexByName(geoMetadata.PrimaryColumn)

		if !slices.Contains(columns, primaryGeomColIdx) {
			return nil, fmt.Errorf("columns (%v) must include primary geometry column '%v' (index %v)", columns, geoMetadata.PrimaryColumn, primaryGeomColIdx)
		}
	}

	if columns != nil && len(columns) == 0 {
		columns = nil
	}

	if rowGroups != nil && len(rowGroups) == 0 {
		rowGroups = nil
	}

	recordReader, recordErr := arrowFileReader.GetRecordReader(ctx, columns, rowGroups)

	if recordErr != nil {
		return nil, recordErr
	}

	reader := &RecordReader{
		fileReader:   arrowFileReader.ParquetReader(),
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

func (r *RecordReader) ArrowSchema() *arrow.Schema {
	return r.recordReader.Schema()
}

func (r *RecordReader) NumRows() int64 {
	return r.fileReader.NumRows()
}

func (r *RecordReader) Close() error {
	r.recordReader.Release()
	return r.fileReader.Close()
}
