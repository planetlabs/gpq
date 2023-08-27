package pqutil_test

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/schema"
	"github.com/planetlabs/gpq/internal/pqutil"
	"github.com/planetlabs/gpq/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformByColumn(t *testing.T) {
	cases := []struct {
		name   string
		data   string
		config *pqutil.TransformConfig
	}{
		{
			name: "basics",
			data: `[
				{
					"product": "soup",
					"cost": 1.29
				},
				{
					"product": "747",
					"cost": 100000000
				}
			]`,
		},
		{
			name: "repeated values",
			data: `[
				{
					"name": "Taylor",
					"grades": ["A", "B", "C"]
				},
				{
					"name": "Kai",
					"grades": ["C", "B", "A"]
				}
			]`,
		},
		{
			name: "with snappy compression",
			data: `[
				{
					"number": 42
				},
				{
					"number": 3.14
				}
			]`,
			config: &pqutil.TransformConfig{
				Compression: &compress.Codecs.Snappy,
			},
		},
		{
			name: "with gzip compression",
			data: `[
				{
					"number": 42
				},
				{
					"number": 3.14
				}
			]`,
			config: &pqutil.TransformConfig{
				Compression: &compress.Codecs.Gzip,
			},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%s (case %d)", c.name, i), func(t *testing.T) {
			input := test.ParquetFromJSON(t, c.data)
			output := &bytes.Buffer{}
			config := c.config
			if config == nil {
				config = &pqutil.TransformConfig{}
			}
			config.Reader = input
			config.Writer = output

			require.NoError(t, pqutil.TransformByColumn(config))

			outputAsJSON := test.ParquetToJSON(t, bytes.NewReader(output.Bytes()))
			assert.JSONEq(t, c.data, outputAsJSON)

			if c.config == nil {
				return
			}

			fileReader, err := file.NewParquetReader(bytes.NewReader(output.Bytes()))
			require.NoError(t, err)
			defer fileReader.Close()

			if c.config.Compression != nil {
				expected := *c.config.Compression
				require.Greater(t, fileReader.NumRowGroups(), 0)
				rowGroupMetadata := fileReader.RowGroup(0).MetaData()
				numColumns := rowGroupMetadata.NumColumns()
				assert.Greater(t, numColumns, 0)
				for colNum := 0; colNum < numColumns; colNum += 1 {
					columnChunk, err := rowGroupMetadata.ColumnChunk(colNum)
					require.NoError(t, err)
					assert.Equal(t, expected, columnChunk.Compression())
				}
			}
		})
	}
}

func TestTransformColumn(t *testing.T) {
	data := `[
		{
			"product": "soup",
			"cost": "1.29"
		},
		{
			"product": "747",
			"cost": "100000000"
		}
	]`

	expected := `[
		{
			"product": "soup",
			"cost": 1.29
		},
		{
			"product": "747",
			"cost": 100000000
		}
	]`

	transformSchema := func(inputSchema *schema.Schema) (*schema.Schema, error) {
		inputRoot := inputSchema.Root()
		numFields := inputRoot.NumFields()

		fields := make([]schema.Node, numFields)
		for fieldNum := 0; fieldNum < numFields; fieldNum += 1 {
			inputField := inputRoot.Field(fieldNum)
			if inputField.Name() != "cost" {
				fields[fieldNum] = inputField
				continue
			}
			outputField, err := schema.NewPrimitiveNode(inputField.Name(), inputField.RepetitionType(), parquet.Types.Double, -1, -1)
			if err != nil {
				return nil, err
			}
			fields[fieldNum] = outputField
		}

		outputRoot, err := schema.NewGroupNode(inputRoot.Name(), inputRoot.RepetitionType(), fields, -1)
		if err != nil {
			return nil, err
		}
		return schema.NewSchema(outputRoot), nil
	}

	transformColumn := func(inputField *arrow.Field, outputField *arrow.Field, chunked *arrow.Chunked) (*arrow.Chunked, error) {
		if inputField.Name != "cost" {
			return chunked, nil
		}
		chunks := chunked.Chunks()
		transformed := make([]arrow.Array, len(chunks))
		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		defer builder.Release()
		for i, arr := range chunks {
			stringArray, ok := arr.(*array.String)
			if !ok {
				return nil, fmt.Errorf("expected a string array, got %v", arr)
			}
			for rowNum := 0; rowNum < stringArray.Len(); rowNum += 1 {
				if outputField.Nullable && stringArray.IsNull(rowNum) {
					builder.AppendNull()
					continue
				}
				str := stringArray.Value(rowNum)
				value, err := strconv.ParseFloat(str, 64)
				if err != nil {
					return nil, fmt.Errorf("trouble parsing %q as float: %w", str, err)
				}
				builder.Append(value)
			}
			transformed[i] = builder.NewArray()
		}
		return arrow.NewChunked(builder.Type(), transformed), nil
	}

	input := test.ParquetFromJSON(t, data)
	output := &bytes.Buffer{}
	config := &pqutil.TransformConfig{
		Reader:          input,
		TransformSchema: transformSchema,
		TransformColumn: transformColumn,
		Writer:          output,
	}
	require.NoError(t, pqutil.TransformByColumn(config))

	outputAsJSON := test.ParquetToJSON(t, bytes.NewReader(output.Bytes()))
	assert.JSONEq(t, expected, outputAsJSON)
}