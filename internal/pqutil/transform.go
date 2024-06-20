package pqutil

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/compress"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/apache/arrow/go/v16/parquet/schema"
)

type ColumnTransformer func(*arrow.Field, *arrow.Field, *arrow.Chunked) (*arrow.Chunked, error)

type SchemaTransformer func(*file.Reader) (*schema.Schema, error)

type TransformConfig struct {
	Reader          parquet.ReaderAtSeeker
	Writer          io.Writer
	Compression     *compress.Compression
	RowGroupLength  int
	TransformSchema SchemaTransformer
	TransformColumn ColumnTransformer
	BeforeClose     func(*file.Reader, *pqarrow.FileWriter) error
}

func getWriterProperties(config *TransformConfig, fileReader *file.Reader) (*parquet.WriterProperties, error) {
	var writerProperties []parquet.WriterProperty
	if config.Compression != nil {
		writerProperties = append(writerProperties, parquet.WithCompression(*config.Compression))
	} else {
		// retain existing column compression (from the first row group)
		if fileReader.NumRowGroups() > 0 {
			rowGroupMetadata := fileReader.RowGroup(0).MetaData()
			for colNum := 0; colNum < rowGroupMetadata.NumColumns(); colNum += 1 {
				colChunkMetadata, err := rowGroupMetadata.ColumnChunk(colNum)
				if err != nil {
					return nil, fmt.Errorf("failed to get column chunk metadata for column %d", colNum)
				}
				compression := colChunkMetadata.Compression()
				if compression != compress.Codecs.Uncompressed {
					colPath := colChunkMetadata.PathInSchema()
					writerProperties = append(writerProperties, parquet.WithCompressionPath(colPath, compression))
				}
			}
		}
	}

	if config.RowGroupLength > 0 {
		writerProperties = append(writerProperties, parquet.WithMaxRowGroupLength(int64(config.RowGroupLength)))
	}

	return parquet.NewWriterProperties(writerProperties...), nil
}

func TransformByColumn(config *TransformConfig) error {
	if config.Reader == nil {
		return errors.New("reader is required")
	}
	if config.Writer == nil {
		return errors.New("writer is required")
	}

	fileReader, fileReaderErr := file.NewParquetReader(config.Reader)
	if fileReaderErr != nil {
		return fileReaderErr
	}
	defer fileReader.Close()

	outputSchema := fileReader.MetaData().Schema
	if config.TransformSchema != nil {
		schema, err := config.TransformSchema(fileReader)
		if err != nil {
			return err
		}
		outputSchema = schema
	}

	arrowReadProperties := pqarrow.ArrowReadProperties{}

	arrowReader, arrowError := pqarrow.NewFileReader(fileReader, arrowReadProperties, memory.DefaultAllocator)
	if arrowError != nil {
		return arrowError
	}
	inputManifest := arrowReader.Manifest

	outputManifest, manifestErr := pqarrow.NewSchemaManifest(outputSchema, fileReader.MetaData().KeyValueMetadata(), &arrowReadProperties)
	if manifestErr != nil {
		return manifestErr
	}

	numFields := len(outputManifest.Fields)
	if numFields != len(inputManifest.Fields) {
		return fmt.Errorf("unexpected number of fields in the output schema, got %d, expected %d", numFields, len(inputManifest.Fields))
	}

	writerProperties, propErr := getWriterProperties(config, fileReader)
	if propErr != nil {
		return propErr
	}

	arrowSchema, arrowSchemaErr := pqarrow.FromParquet(outputSchema, &arrowReadProperties, fileReader.MetaData().KeyValueMetadata())
	if arrowSchemaErr != nil {
		return arrowSchemaErr
	}

	fileWriter, fileWriterErr := pqarrow.NewFileWriter(arrowSchema, config.Writer, writerProperties, pqarrow.DefaultWriterProps())
	if fileWriterErr != nil {
		return fileWriterErr
	}

	ctx := pqarrow.NewArrowWriteContext(context.Background(), nil)

	if config.RowGroupLength > 0 {
		columnReaders := make([]*pqarrow.ColumnReader, numFields)
		for fieldNum := 0; fieldNum < numFields; fieldNum += 1 {
			colReader, err := arrowReader.GetColumn(ctx, fieldNum)
			if err != nil {
				return err
			}
			columnReaders[fieldNum] = colReader
		}

		numRows := fileReader.NumRows()
		numRowsWritten := int64(0)
		for {
			fileWriter.NewRowGroup()
			numRowsInGroup := 0
			for fieldNum := 0; fieldNum < numFields; fieldNum += 1 {
				colReader := columnReaders[fieldNum]
				arr, readErr := colReader.NextBatch(int64(config.RowGroupLength))
				if readErr != nil {
					return readErr
				}
				if config.TransformColumn != nil {
					inputField := inputManifest.Fields[fieldNum].Field
					outputField := outputManifest.Fields[fieldNum].Field
					transformed, err := config.TransformColumn(inputField, outputField, arr)
					if err != nil {
						return err
					}
					if transformed.DataType() != outputField.Type {
						return fmt.Errorf("transform generated an unexpected type, got %s, expected %s", transformed.DataType().Name(), outputField.Type.Name())
					}
					arr = transformed
				}
				if numRowsInGroup == 0 {
					// TODO: propose fileWriter.RowGroupNumRows()
					numRowsInGroup = arr.Len()
				}
				if err := fileWriter.WriteColumnChunked(arr, 0, int64(arr.Len())); err != nil {
					return err
				}
			}
			numRowsWritten += int64(numRowsInGroup)
			if numRowsWritten >= numRows {
				break
			}
		}
	} else {
		numRowGroups := fileReader.NumRowGroups()
		for rowGroupIndex := 0; rowGroupIndex < numRowGroups; rowGroupIndex += 1 {
			rowGroupReader := arrowReader.RowGroup(rowGroupIndex)
			fileWriter.NewRowGroup()
			for fieldNum := 0; fieldNum < numFields; fieldNum += 1 {
				arr, readErr := rowGroupReader.Column(fieldNum).Read(ctx)
				if readErr != nil {
					return readErr
				}
				if config.TransformColumn != nil {
					inputField := inputManifest.Fields[fieldNum].Field
					outputField := outputManifest.Fields[fieldNum].Field
					transformed, err := config.TransformColumn(inputField, outputField, arr)
					if err != nil {
						return err
					}
					arr = transformed
				}
				if err := fileWriter.WriteColumnChunked(arr, 0, int64(arr.Len())); err != nil {
					return err
				}
			}
		}
	}

	if config.BeforeClose != nil {
		if err := config.BeforeClose(fileReader, fileWriter); err != nil {
			return err
		}
	}
	return fileWriter.Close()
}
