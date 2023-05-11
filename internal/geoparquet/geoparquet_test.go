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
	"io"
	"os"
	"testing"

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
