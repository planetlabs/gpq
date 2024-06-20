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
	"context"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/planetlabs/gpq/internal/geo"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/planetlabs/gpq/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newFileReader(filepath string) (*file.Reader, error) {
	f, fileErr := os.Open(filepath)
	if fileErr != nil {
		return nil, fileErr
	}
	return file.NewParquetReader(f)
}

func TestGetMetadataV040(t *testing.T) {
	reader, readerErr := newFileReader("../testdata/cases/example-v0.4.0.parquet")
	require.NoError(t, readerErr)
	defer reader.Close()

	metadata, metadataErr := geoparquet.GetMetadata(reader.MetaData().GetKeyValueMetadata())
	require.NoError(t, metadataErr)
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
	reader, readerErr := newFileReader("../testdata/cases/example-v1.0.0-beta.1.parquet")
	require.NoError(t, readerErr)
	defer reader.Close()

	metadata, metadataErr := geoparquet.GetMetadata(reader.MetaData().GetKeyValueMetadata())
	require.NoError(t, metadataErr)
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

func TestGetMetadataV1(t *testing.T) {
	reader, readerErr := newFileReader("../testdata/cases/example-v1.0.0.parquet")
	require.NoError(t, readerErr)
	defer reader.Close()

	metadata, metadataErr := geoparquet.GetMetadata(reader.MetaData().GetKeyValueMetadata())
	require.NoError(t, metadataErr)

	assert.Equal(t, "geometry", metadata.PrimaryColumn)
	assert.Equal(t, "1.0.0", metadata.Version)
	require.Len(t, metadata.Columns, 1)

	col := metadata.Columns[metadata.PrimaryColumn]
	assert.Equal(t, "WKB", col.Encoding)
	geomTypes := col.GetGeometryTypes()
	assert.Len(t, geomTypes, 2)
	assert.Contains(t, geomTypes, "Polygon")
	assert.Contains(t, geomTypes, "MultiPolygon")
}

func TestRecordReaderV040(t *testing.T) {
	fixturePath := "../testdata/cases/example-v0.4.0.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	reader, err := geoparquet.NewRecordReader(&geoparquet.ReaderConfig{
		Reader: input,
	})
	require.NoError(t, err)

	numRows := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		numRows += int(record.NumRows())
	}

	assert.Equal(t, 5, numRows)
}

func TestRowReaderV100Beta1(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.0.0-beta.1.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	reader, err := geoparquet.NewRecordReader(&geoparquet.ReaderConfig{
		Reader: input,
	})
	require.NoError(t, err)

	numRows := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		numRows += int(record.NumRows())
	}

	assert.Equal(t, 5, numRows)
}

func toWKB(t *testing.T, geometry orb.Geometry) []byte {
	data, err := wkb.Marshal(geometry)
	require.NoError(t, err)
	return data
}

func TestFromParquetWithoutMetadata(t *testing.T) {
	type Row struct {
		Name     string `parquet:"name=name, logical=String" json:"name"`
		Geometry []byte `parquet:"name=geometry" json:"geometry"`
	}

	rows := []*Row{
		{
			Name:     "test-point",
			Geometry: toWKB(t, orb.Point{1, 2}),
		},
	}

	input := test.ParquetFromStructs(t, rows)

	output := &bytes.Buffer{}
	convertErr := geoparquet.FromParquet(input, output, nil)
	require.NoError(t, convertErr)

	geoparquetInput := bytes.NewReader(output.Bytes())

	reader, err := file.NewParquetReader(geoparquetInput)
	require.NoError(t, err)
	defer reader.Close()

	metadata, err := geoparquet.GetMetadata(reader.MetaData().KeyValueMetadata())
	require.NoError(t, err)

	assert.Len(t, metadata.Columns, 1)

	primaryColumnMetadata := metadata.Columns[metadata.PrimaryColumn]

	assert.Equal(t, geo.EncodingWKB, primaryColumnMetadata.Encoding)

	assert.Equal(t, int64(1), reader.NumRows())
}

func TestFromParquetWithoutDefaultGeometryColumn(t *testing.T) {
	type Row struct {
		Name string `parquet:"name=name, logical=String" json:"name"`
		Geom []byte `parquet:"name=geom" json:"geom"`
	}

	rows := []*Row{
		{
			Name: "test-point",
			Geom: toWKB(t, orb.Point{1, 2}),
		},
	}

	input := test.ParquetFromStructs(t, rows)

	output := &bytes.Buffer{}
	convertErr := geoparquet.FromParquet(input, output, nil)
	require.ErrorContains(t, convertErr, "expected a geometry column named \"geometry\"")
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
		Name     string `parquet:"name=name, logical=String" json:"name"`
		Geometry string `parquet:"name=geometry, logical=String" json:"geometry"`
	}

	rows := []*Row{
		{
			Name:     "test-point-1",
			Geometry: "POINT (1 2)",
		},
		{
			Name:     "test-point-2",
			Geometry: "POINT (3 4)",
		},
	}

	input := test.ParquetFromStructs(t, rows)

	output := &bytes.Buffer{}
	convertErr := geoparquet.FromParquet(input, output, nil)
	require.NoError(t, convertErr)

	geoparquetInput := bytes.NewReader(output.Bytes())
	reader, err := file.NewParquetReader(geoparquetInput)
	require.NoError(t, err)
	defer reader.Close()

	metadata, err := geoparquet.GetMetadata(reader.MetaData().KeyValueMetadata())
	require.NoError(t, err)

	assert.Len(t, metadata.Columns, 1)

	primaryColumnMetadata := metadata.Columns[metadata.PrimaryColumn]

	geometryTypes := primaryColumnMetadata.GetGeometryTypes()
	assert.Len(t, geometryTypes, 1)
	assert.Contains(t, geometryTypes, "Point")

	bounds := primaryColumnMetadata.Bounds
	assert.Equal(t, []float64{1, 2, 3, 4}, bounds)

	assert.Equal(t, geo.EncodingWKB, primaryColumnMetadata.Encoding)

	assert.Equal(t, int64(2), reader.NumRows())
}

func TestFromParquetWithAltPrimaryColumn(t *testing.T) {
	type Row struct {
		Name string `parquet:"name=name, logical=String" json:"name"`
		Geo  []byte `parquet:"name=geo" json:"geo"`
	}

	rows := []*Row{
		{
			Name: "test-point-1",
			Geo:  toWKB(t, orb.Point{1, 2}),
		},
		{
			Name: "test-point-2",
			Geo:  toWKB(t, orb.Point{3, 4}),
		},
	}

	input := test.ParquetFromStructs(t, rows)

	primaryColumn := "geo"

	output := &bytes.Buffer{}
	convertErr := geoparquet.FromParquet(input, output, &geoparquet.ConvertOptions{InputPrimaryColumn: primaryColumn})
	require.NoError(t, convertErr)

	geoparquetInput := bytes.NewReader(output.Bytes())
	reader, err := file.NewParquetReader(geoparquetInput)
	require.NoError(t, err)
	defer reader.Close()

	metadata, err := geoparquet.GetMetadata(reader.MetaData().KeyValueMetadata())
	require.NoError(t, err)

	assert.Equal(t, primaryColumn, metadata.PrimaryColumn)
	assert.Len(t, metadata.Columns, 1)
	primaryColumnMetadata := metadata.Columns[metadata.PrimaryColumn]
	assert.Equal(t, geo.EncodingWKB, primaryColumnMetadata.Encoding)

	assert.Equal(t, int64(2), reader.NumRows())
}

func TestFromParquetWithAltPrimaryColumnWKT(t *testing.T) {
	type Row struct {
		Name string `parquet:"name=name, logical=String" json:"name"`
		Geo  string `parquet:"name=geo, logical=String" json:"geo"`
	}

	rows := []*Row{
		{
			Name: "test-point-1",
			Geo:  "POINT (1 2)",
		},
		{
			Name: "test-point-2",
			Geo:  "POINT (3 4)",
		},
	}

	input := test.ParquetFromStructs(t, rows)

	output := &bytes.Buffer{}
	convertErr := geoparquet.FromParquet(input, output, &geoparquet.ConvertOptions{InputPrimaryColumn: "geo"})
	require.NoError(t, convertErr)

	geoparquetInput := bytes.NewReader(output.Bytes())
	reader, err := file.NewParquetReader(geoparquetInput)
	require.NoError(t, err)
	defer reader.Close()

	metadata, err := geoparquet.GetMetadata(reader.MetaData().KeyValueMetadata())
	require.NoError(t, err)

	assert.Len(t, metadata.Columns, 1)

	primaryColumnMetadata := metadata.Columns[metadata.PrimaryColumn]

	geometryTypes := primaryColumnMetadata.GetGeometryTypes()
	assert.Len(t, geometryTypes, 1)
	assert.Contains(t, geometryTypes, "Point")

	bounds := primaryColumnMetadata.Bounds
	assert.Equal(t, []float64{1, 2, 3, 4}, bounds)

	assert.Equal(t, geo.EncodingWKB, primaryColumnMetadata.Encoding)

	assert.Equal(t, int64(2), reader.NumRows())
}

func TestRecordReading(t *testing.T) {
	f, fileErr := os.Open("../testdata/cases/example-v1.0.0-beta.1.parquet")
	require.NoError(t, fileErr)
	reader, readerErr := file.NewParquetReader(f)
	require.NoError(t, readerErr)
	defer reader.Close()

	pqReader, pqErr := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 10}, memory.DefaultAllocator)
	require.NoError(t, pqErr)

	recordReader, rrErr := pqReader.GetRecordReader(context.Background(), nil, nil)
	require.NoError(t, rrErr)

	numRows := 0
	for {
		rec, err := recordReader.Read()
		if err == io.EOF {
			assert.Nil(t, rec)
			break
		}
		assert.NoError(t, err)
		numRows += int(rec.NumRows())
	}

	assert.Equal(t, reader.NumRows(), int64(numRows))
}
