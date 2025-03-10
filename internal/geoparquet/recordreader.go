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
	BatchSize       int
	Reader          parquet.ReaderAtSeeker
	File            *file.Reader
	Context         context.Context
	ExcludeColNames []string
	IncludeColNames []string
}

type RecordReader struct {
	fileReader   *file.Reader
	metadata     *Metadata
	recordReader pqarrow.RecordReader
}

// A Set type based on map, to hold arrow column indices.
// Implements common Set methods such as Difference() and Contains().
// To instantiate, use the constructor newIndicesSet() followed by either
// Add() if you want to build the Set sequentially or the convenience function
// FromColNames().
type indicesSet map[int]struct{}

func newIndicesSet(size int) *indicesSet {
	var s indicesSet = make(map[int]struct{}, size)
	return &s
}

func (s *indicesSet) Add(col int) *indicesSet {
	(*s)[col] = struct{}{}
	return s
}

func (s *indicesSet) FromColNames(cols []string, schema *arrow.Schema) *indicesSet {
	for _, col := range cols {
		if indicesForColumn := schema.FieldIndices(col); indicesForColumn != nil {
			for _, colIdx := range indicesForColumn {
				s.Add(colIdx)
			}
		}
	}
	return s
}

func (s *indicesSet) Contains(col int) bool {
	_, ok := (*s)[col]
	return ok
}

func (s *indicesSet) Difference(other *indicesSet) *indicesSet {
	sSize := s.Size()
	otherSize := s.Size()
	var newSet *indicesSet
	if sSize < otherSize {
		newSet = newIndicesSet(otherSize - sSize)
	} else {
		newSet = newIndicesSet(sSize - otherSize)
	}
	for key := range *s {
		if !other.Contains(key) {
			newSet.Add(key)
		}
	}
	return newSet
}

func (s *indicesSet) Size() int {
	return len(*s)
}

func (s *indicesSet) List() []int {
	keys := make([]int, 0, len(*s))
	for k := range *s {
		keys = append(keys, k)
	}
	return keys
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

	var recordReader pqarrow.RecordReader
	var recordErr error

	excludeColNamesProvided := len(config.ExcludeColNames) > 0
	includeColNamesProvided := len(config.IncludeColNames) > 0
	if excludeColNamesProvided || includeColNamesProvided {
		if excludeColNamesProvided == includeColNamesProvided {
			return nil, errors.New("config must only contain one of ExcludeColNames/IncludeColNames")
		}

		schema, schemaErr := arrowReader.Schema()
		if schemaErr != nil {
			return nil, schemaErr
		}

		if excludeColNamesProvided {
			if slices.Contains(config.ExcludeColNames, geoMetadata.PrimaryColumn) {
				return nil, fmt.Errorf("can't exclude primary geometry column '%v'", geoMetadata.PrimaryColumn)
			}

			// generate indices from col names and compute the indices to include
			indicesToExclude := newIndicesSet(schema.NumFields()-len(config.ExcludeColNames)).FromColNames(config.ExcludeColNames, schema)
			allIndices := newIndicesSet(schema.NumFields())
			for i := 0; i < schema.NumFields(); i++ {
				allIndices.Add(i)
			}
			indices := allIndices.Difference(indicesToExclude)
			recordReader, recordErr = arrowReader.GetRecordReader(ctx, indices.List(), nil)
		} else {
			if !slices.Contains(config.IncludeColNames, geoMetadata.PrimaryColumn) {
				return nil, fmt.Errorf("column names must include primary geometry column '%v'", geoMetadata.PrimaryColumn)
			}

			// generate indices from col names
			indices := newIndicesSet(len(config.IncludeColNames)).FromColNames(config.IncludeColNames, schema)
			recordReader, recordErr = arrowReader.GetRecordReader(ctx, indices.List(), nil)
		}
	} else {
		recordReader, recordErr = arrowReader.GetRecordReader(ctx, nil, nil)
	}
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
