package geoparquet

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
)

type RecordWriter struct {
	fileWriter       *pqarrow.FileWriter
	metadata         *Metadata
	wroteGeoMetadata bool
}

func NewRecordWriter(config *WriterConfig) (*RecordWriter, error) {
	parquetProps := config.ParquetWriterProps
	if parquetProps == nil {
		parquetProps = parquet.NewWriterProperties()
	}

	arrowProps := config.ArrowWriterProps
	if arrowProps == nil {
		defaults := pqarrow.DefaultWriterProps()
		arrowProps = &defaults
	}

	if config.ArrowSchema == nil {
		return nil, errors.New("schema is required")
	}

	if config.Writer == nil {
		return nil, errors.New("writer is required")
	}
	fileWriter, fileErr := pqarrow.NewFileWriter(config.ArrowSchema, config.Writer, parquetProps, *arrowProps)
	if fileErr != nil {
		return nil, fileErr
	}

	writer := &RecordWriter{
		fileWriter: fileWriter,
		metadata:   config.Metadata,
	}

	return writer, nil
}

func (w *RecordWriter) AppendKeyValueMetadata(key string, value string) error {
	if err := w.fileWriter.AppendKeyValueMetadata(key, value); err != nil {
		return err
	}
	if key == MetadataKey {
		w.wroteGeoMetadata = true
	}
	return nil
}

func (w *RecordWriter) Write(record arrow.Record) error {
	return w.fileWriter.WriteBuffered(record)
}

func (w *RecordWriter) Close() error {
	if !w.wroteGeoMetadata {
		metadata := w.metadata
		if metadata == nil {
			metadata = DefaultMetadata()
		}
		data, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to encode %s file metadata", MetadataKey)
		}
		if err := w.fileWriter.AppendKeyValueMetadata(MetadataKey, string(data)); err != nil {
			return fmt.Errorf("failed to append %s file metadata", MetadataKey)
		}

	}
	return w.fileWriter.Close()
}
