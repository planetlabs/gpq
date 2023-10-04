package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/apache/arrow/go/v14/parquet/schema"
	"github.com/planetlabs/gpq/internal/geojson"
	"github.com/planetlabs/gpq/internal/pqutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ParquetFromJSON(t *testing.T, data string, writerProperties *parquet.WriterProperties) []byte {
	if writerProperties == nil {
		writerProperties = parquet.NewWriterProperties()
	}
	var rows []map[string]any
	require.NoError(t, json.Unmarshal([]byte(data), &rows))

	builder := pqutil.NewArrowSchemaBuilder()
	for _, row := range rows {
		require.NoError(t, builder.Add(row))
	}
	if !builder.Ready() {
		assert.Fail(t, "could not derive schema from rows")
	}

	schema, err := builder.Schema()
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(string(data)))
	require.NoError(t, err)

	output := &bytes.Buffer{}

	writer, err := pqarrow.NewFileWriter(schema, output, writerProperties, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, writer.WriteBuffered(rec))
	require.NoError(t, writer.Close())

	return output.Bytes()
}

func ParquetToJSON(t *testing.T, input parquet.ReaderAtSeeker) string {
	fileReader, err := file.NewParquetReader(input)
	require.NoError(t, err)

	arrowReader, err := pqarrow.NewFileReader(fileReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	require.NoError(t, err)

	recordReader, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
	require.NoError(t, err)

	rows := []map[string]any{}

	for {
		record, err := recordReader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		schema := record.Schema()
		arr := array.RecordToStructArray(record)
		defer arr.Release()

		for rowNum := 0; rowNum < arr.Len(); rowNum += 1 {
			row := map[string]any{}
			for fieldNum := 0; fieldNum < arr.NumField(); fieldNum += 1 {
				name := schema.Field(fieldNum).Name
				value := arr.Field(fieldNum).GetOneForMarshal(rowNum)
				row[name] = value
			}
			rows = append(rows, row)
		}
	}

	data, err := json.Marshal(rows)
	require.NoError(t, err)
	return string(data)
}

func GeoParquetFromJSON(t *testing.T, data string) []byte {
	input := strings.NewReader(data)
	output := &bytes.Buffer{}
	require.NoError(t, geojson.ToParquet(input, output, nil))
	return output.Bytes()
}

func ParquetFromStructs[T any](t *testing.T, rows []T) parquet.ReaderAtSeeker {
	parquetSchema, err := schema.NewSchemaFromStruct(rows[0])
	require.NoError(t, err)

	arrowSchema, err := pqarrow.FromParquet(parquetSchema, nil, nil)
	require.NoError(t, err)

	data, err := json.Marshal(rows)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(string(data)))
	require.NoError(t, err)

	output := &bytes.Buffer{}

	writer, err := pqarrow.NewFileWriter(arrowSchema, output, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, writer.WriteBuffered(rec))
	require.NoError(t, writer.Close())

	return bytes.NewReader(output.Bytes())
}

func AssertArrowSchemaMatches(t *testing.T, expected string, schema *arrow.Schema) {
	parquetSchema, err := pqarrow.ToParquet(schema, nil, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	assert.Equal(t, Tab2Space(Dedent(expected)), pqutil.ParquetSchemaString(parquetSchema))
}

func Dedent(block string) string {
	newline := "\n"
	whitespace := " \t"

	lines := strings.Split(block, newline)
	prefixLen := -1

	if len(lines) == 0 {
		return block
	}

	if len(strings.TrimLeft(lines[0], whitespace)) == 0 {
		lines = lines[1:]
	}
	if len(strings.TrimLeft(lines[len(lines)-1], whitespace)) == 0 {
		lines = lines[:len(lines)-1]
	}

	dedentedLines := []string{}
	for _, line := range lines {
		if prefixLen < 0 {
			trimmedLine := strings.TrimLeft(line, whitespace)
			prefixLen = len(line) - len(trimmedLine)
			dedentedLines = append(dedentedLines, trimmedLine)
			continue
		}
		if prefixLen > len(line)-1 {
			dedentedLines = append(dedentedLines, strings.TrimLeft(line, whitespace))
			continue
		}
		dedentedLines = append(dedentedLines, line[prefixLen:])
	}
	return strings.Join(dedentedLines, newline) + newline
}

func Tab2Space(str string) string {
	return strings.ReplaceAll(str, "\t", "  ")
}
