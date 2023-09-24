package geoparquet

import (
	"io"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

type WriterConfig struct {
	Writer             io.Writer
	Metadata           *Metadata
	ParquetWriterProps *parquet.WriterProperties
	ArrowWriterProps   *pqarrow.ArrowWriterProperties
	ArrowSchema        *arrow.Schema
}
