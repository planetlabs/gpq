// Copyright 2023 Planet Labs PBC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package geoparquet_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMetadataV040(t *testing.T) {
	fixturePath := "../testdata/cases/example-v0.4.0.parquet"
	info, statErr := os.Stat(fixturePath)
	require.NoError(t, statErr)

	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	file, fileErr := parquet.OpenFile(input, info.Size())
	require.NoError(t, fileErr)

	metadata, geoErr := geoparquet.GetMetadata(file)
	require.NoError(t, geoErr)

	assert.Equal(t, "geometry", metadata.PrimaryColumn)
	assert.Equal(t, "0.4.0", metadata.Version)
	require.Len(t, metadata.Columns, 1)

	col := metadata.Columns[metadata.PrimaryColumn]
	assert.Equal(t, "WKB", col.Encoding)
	assert.Equal(t, "planar", col.Edges)
	assert.Equal(t, []float64{-180, -90, 180, 83.6451}, col.Bounds)
	assert.Equal(t, []string{"Polygon", "MultiPolygon"}, col.GetGeometryTypes())
}

func TestGetMetadataV100Beta1(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.0.0-beta.1.parquet"
	info, statErr := os.Stat(fixturePath)
	require.NoError(t, statErr)

	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	file, fileErr := parquet.OpenFile(input, info.Size())
	require.NoError(t, fileErr)

	metadata, geoErr := geoparquet.GetMetadata(file)
	require.NoError(t, geoErr)

	assert.Equal(t, "geometry", metadata.PrimaryColumn)
	assert.Equal(t, "1.0.0-beta.1", metadata.Version)
	require.Len(t, metadata.Columns, 1)

	col := metadata.Columns[metadata.PrimaryColumn]
	assert.Equal(t, "WKB", col.Encoding)
	assert.Equal(t, "planar", col.Edges)
	assert.Equal(t, []float64{-180, -90, 180, 83.6451}, col.Bounds)
	assert.Equal(t, []string{"Polygon", "MultiPolygon"}, col.GetGeometryTypes())

	require.NotNil(t, col.CRS)
	require.NotNil(t, col.CRS.Id)
	assert.Equal(t, "OGC", col.CRS.Id.Authority)
	assert.Equal(t, "CRS84", col.CRS.Id.Code)
	assert.Equal(t, "WGS 84 (CRS84)", col.CRS.Name)
}

func TestRowReaderV040(t *testing.T) {
	fixturePath := "../testdata/cases/example-v0.4.0.parquet"
	info, statErr := os.Stat(fixturePath)
	require.NoError(t, statErr)

	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	file, fileErr := parquet.OpenFile(input, info.Size())
	require.NoError(t, fileErr)

	reader := geoparquet.NewRowReader(file)
	rows := []parquet.Row{}
	for {
		row, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NotNil(t, row)
		rows = append(rows, row)
	}

	assert.Len(t, rows, int(file.NumRows()))

	schema := file.Schema()
	firstRow := rows[0]

	continentCol, ok := schema.Lookup("continent")
	require.True(t, ok)
	continent := firstRow[continentCol.ColumnIndex]
	assert.Equal(t, "Oceania", continent.String())

	nameCol, ok := schema.Lookup("name")
	require.True(t, ok)
	name := firstRow[nameCol.ColumnIndex]
	assert.Equal(t, "Fiji", name.String())
}

func TestRowReaderV100Beta1(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.0.0-beta.1.parquet"
	info, statErr := os.Stat(fixturePath)
	require.NoError(t, statErr)

	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	file, fileErr := parquet.OpenFile(input, info.Size())
	require.NoError(t, fileErr)

	reader := geoparquet.NewRowReader(file)
	rows := []parquet.Row{}
	for {
		row, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NotNil(t, row)
		rows = append(rows, row)
	}

	assert.Len(t, rows, int(file.NumRows()))

	schema := file.Schema()
	firstRow := rows[0]

	continentCol, ok := schema.Lookup("continent")
	require.True(t, ok)
	continent := firstRow[continentCol.ColumnIndex]
	assert.Equal(t, "Oceania", continent.String())

	nameCol, ok := schema.Lookup("name")
	require.True(t, ok)
	name := firstRow[nameCol.ColumnIndex]
	assert.Equal(t, "Fiji", name.String())
}

func makeParquet[T any](rows []T, metadata *geoparquet.Metadata) (*parquet.File, error) {
	data := &bytes.Buffer{}

	writer := parquet.NewGenericWriter[T](data)

	_, writeErr := writer.Write(rows)
	if writeErr != nil {
		return nil, fmt.Errorf("trouble writing rows: %w", writeErr)
	}

	if metadata != nil {
		jsonMetadata, jsonErr := json.Marshal(metadata)
		if jsonErr != nil {
			return nil, fmt.Errorf("trouble encoding metadata as json: %w", jsonErr)
		}

		writer.SetKeyValueMetadata(geoparquet.MetadataKey, string(jsonMetadata))
	}

	closeErr := writer.Close()
	if closeErr != nil {
		return nil, fmt.Errorf("trouble closing writer: %w", closeErr)
	}

	return parquet.OpenFile(bytes.NewReader(data.Bytes()), int64(data.Len()))
}

func TestFromParquetWithoutMetadata(t *testing.T) {
	type Row struct {
		Name     string `parquet:"name"`
		Geometry []byte `parquet:"geometry"`
	}

	point, pointErr := wkb.Marshal(orb.Point{1, 2})
	require.NoError(t, pointErr)

	rows := []*Row{
		{
			Name:     "test-point",
			Geometry: point,
		},
	}

	parquetFile, inputErr := makeParquet(rows, nil)
	require.NoError(t, inputErr)

	output := &bytes.Buffer{}
	convertErr := geoparquet.FromParquet(parquetFile, output, nil)
	require.NoError(t, convertErr)

	geoparquetInput := bytes.NewReader(output.Bytes())
	geoparquetFile, outputErr := parquet.OpenFile(geoparquetInput, geoparquetInput.Size())
	require.NoError(t, outputErr)

	metadata, geoErr := geoparquet.GetMetadata(geoparquetFile)
	require.NoError(t, geoErr)

	assert.Len(t, metadata.Columns, 1)

	primaryColumnMetadata := metadata.Columns[metadata.PrimaryColumn]

	geometryTypes := primaryColumnMetadata.GetGeometryTypes()
	assert.Len(t, geometryTypes, 1)
	assert.Contains(t, geometryTypes, "Point")

	bounds := primaryColumnMetadata.Bounds
	assert.Equal(t, []float64{1, 2, 1, 2}, bounds)

	assert.Equal(t, geoparquet.EncodingWKB, primaryColumnMetadata.Encoding)

	assert.Equal(t, int64(1), geoparquetFile.NumRows())
}

func TestMetadataClone(t *testing.T) {
	metadata := geoparquet.DefaultMetadata()
	clone := metadata.Clone()

	assert.Equal(t, metadata.PrimaryColumn, clone.PrimaryColumn)
	clone.PrimaryColumn = "modified"
	assert.NotEqual(t, metadata.PrimaryColumn, clone.PrimaryColumn)

	assert.Equal(t, len(metadata.Columns), len(clone.Columns))

	require.Contains(t, metadata.Columns, metadata.PrimaryColumn)
	require.Contains(t, clone.Columns, metadata.PrimaryColumn)

	originalColumn := metadata.Columns[metadata.PrimaryColumn]
	cloneColumn := clone.Columns[metadata.PrimaryColumn]

	assert.Equal(t, originalColumn.Encoding, cloneColumn.Encoding)
	cloneColumn.Encoding = "modified"
	assert.NotEqual(t, originalColumn.Encoding, cloneColumn.Encoding)
}

func TestFromParquetWithWKT(t *testing.T) {
	type Row struct {
		Name     string `parquet:"name"`
		Geometry string `parquet:"geometry"`
	}

	rows := []*Row{
		{
			Name:     "test-point-1",
			Geometry: string(wkt.Marshal(orb.Point{1, 2})),
		},
		{
			Name:     "test-point-2",
			Geometry: string(wkt.Marshal(orb.Point{3, 4})),
		},
	}

	parquetFile, inputErr := makeParquet(rows, nil)
	require.NoError(t, inputErr)

	output := &bytes.Buffer{}
	convertErr := geoparquet.FromParquet(parquetFile, output, nil)
	require.NoError(t, convertErr)

	geoparquetInput := bytes.NewReader(output.Bytes())
	geoparquetFile, outputErr := parquet.OpenFile(geoparquetInput, geoparquetInput.Size())
	require.NoError(t, outputErr)

	metadata, geoErr := geoparquet.GetMetadata(geoparquetFile)
	require.NoError(t, geoErr)

	assert.Len(t, metadata.Columns, 1)

	primaryColumnMetadata := metadata.Columns[metadata.PrimaryColumn]

	geometryTypes := primaryColumnMetadata.GetGeometryTypes()
	assert.Len(t, geometryTypes, 1)
	assert.Contains(t, geometryTypes, "Point")

	bounds := primaryColumnMetadata.Bounds
	assert.Equal(t, []float64{1, 2, 3, 4}, bounds)

	assert.Equal(t, geoparquet.EncodingWKB, primaryColumnMetadata.Encoding)

	assert.Equal(t, int64(2), geoparquetFile.NumRows())
}

func TestFromParquetWithAltPrimaryColumn(t *testing.T) {
	type Row struct {
		Name string `parquet:"name"`
		Geo  string `parquet:"geo"`
	}

	rows := []*Row{
		{
			Name: "test-point-1",
			Geo:  string(wkt.Marshal(orb.Point{1, 2})),
		},
		{
			Name: "test-point-2",
			Geo:  string(wkt.Marshal(orb.Point{3, 4})),
		},
	}

	parquetFile, inputErr := makeParquet(rows, nil)
	require.NoError(t, inputErr)

	output := &bytes.Buffer{}
	convertErr := geoparquet.FromParquet(parquetFile, output, &geoparquet.ConvertOptions{InputPrimaryColumn: "geo"})
	require.NoError(t, convertErr)

	geoparquetInput := bytes.NewReader(output.Bytes())
	geoparquetFile, outputErr := parquet.OpenFile(geoparquetInput, geoparquetInput.Size())
	require.NoError(t, outputErr)

	metadata, geoErr := geoparquet.GetMetadata(geoparquetFile)
	require.NoError(t, geoErr)

	assert.Len(t, metadata.Columns, 1)

	primaryColumnMetadata := metadata.Columns[metadata.PrimaryColumn]

	geometryTypes := primaryColumnMetadata.GetGeometryTypes()
	assert.Len(t, geometryTypes, 1)
	assert.Contains(t, geometryTypes, "Point")

	bounds := primaryColumnMetadata.Bounds
	assert.Equal(t, []float64{1, 2, 3, 4}, bounds)

	assert.Equal(t, geoparquet.EncodingWKB, primaryColumnMetadata.Encoding)

	assert.Equal(t, int64(2), geoparquetFile.NumRows())
}
