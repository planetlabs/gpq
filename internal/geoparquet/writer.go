package geoparquet

import (
	"io"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
)

type WriterConfig struct {
	Writer             io.Writer
	Metadata           *Metadata
	ParquetWriterProps *parquet.WriterProperties
	ArrowWriterProps   *pqarrow.ArrowWriterProperties
	ArrowSchema        *arrow.Schema
}
