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

	reader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
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

	reader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		Reader: input,
	})
	require.NoError(t, err)

	numRows := 0
	var numCols int
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		numRows += int(record.NumRows())
		numCols = int(record.NumCols())
	}

	assert.Equal(t, 5, numRows)
	assert.Equal(t, 6, numCols)
}

func TestRecordReaderV100Columns(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.0.0.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	reader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		Reader:  input,
		Columns: []int{0, 1, 4, 5},
	})
	require.NoError(t, err)

	record, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, record.NumCols(), int64(4))

	fields := record.Schema().Fields()
	colNames := make([]string, len(fields))
	for idx, field := range fields {
		colNames[idx] = field.Name
	}

	assert.ElementsMatch(t, colNames, []string{"geometry", "pop_est", "iso_a3", "name"})
}

func TestRecordReaderV110Columns(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.0.0.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	reader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		Reader:  input,
		Columns: []int{0, 2, 3},
	})
	require.NoError(t, err)

	record, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, record.NumCols(), int64(3))

	fields := record.Schema().Fields()
	colNames := make([]string, len(fields))
	for idx, field := range fields {
		colNames[idx] = field.Name
	}

	assert.ElementsMatch(t, colNames, []string{"geometry", "continent", "gdp_md_est"})
}

func TestRecordReaderV110NoGeomColError(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.1.0.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	reader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		Reader:  input,
		Columns: []int{2, 3},
	})
	require.ErrorContains(t, err, "geometry column")
	require.Nil(t, reader)
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

func TestGetBboxColumnV100(t *testing.T) {
	f, fileErr := os.Open("../testdata/cases/example-v1.0.0.parquet")
	require.NoError(t, fileErr)
	reader, readerErr := file.NewParquetReader(f)
	require.NoError(t, readerErr)
	defer reader.Close()

	metadata, err := geoparquet.GetMetadata(reader.MetaData().KeyValueMetadata())
	require.NoError(t, err)

	// no bbox col in the file, we expect -1
	bboxCol := geoparquet.GetBboxColumn(reader.MetaData().Schema, metadata)
	assert.Equal(t, -1, bboxCol.Index)
	assert.Equal(t, "", bboxCol.Name)
	assert.Equal(t, 0, bboxCol.BaseColumn)
	assert.Equal(t, "", bboxCol.BaseColumnEncoding)
	assert.Equal(t, "xmin", bboxCol.BboxColumnFieldNames.Xmin)
	assert.Equal(t, "ymin", bboxCol.BboxColumnFieldNames.Ymin)
	assert.Equal(t, "xmax", bboxCol.BboxColumnFieldNames.Xmax)
	assert.Equal(t, "ymax", bboxCol.BboxColumnFieldNames.Ymax)
}

func TestGetBboxColumnV110(t *testing.T) {
	f, fileErr := os.Open("../testdata/cases/example-v1.1.0.parquet")
	require.NoError(t, fileErr)
	reader, readerErr := file.NewParquetReader(f)
	require.NoError(t, readerErr)
	defer reader.Close()

	metadata, err := geoparquet.GetMetadata(reader.MetaData().KeyValueMetadata())
	require.NoError(t, err)

	// there is a bbox col in the file, we expect index 6
	bboxCol := geoparquet.GetBboxColumn(reader.MetaData().Schema, metadata)
	assert.Equal(t, 6, bboxCol.Index)
	assert.Equal(t, "bbox", bboxCol.Name)
	assert.Equal(t, 5, bboxCol.BaseColumn)
	assert.Equal(t, "", bboxCol.BaseColumnEncoding)
	assert.Equal(t, "xmin", bboxCol.BboxColumnFieldNames.Xmin)
	assert.Equal(t, "ymin", bboxCol.BboxColumnFieldNames.Ymin)
	assert.Equal(t, "xmax", bboxCol.BboxColumnFieldNames.Xmax)
	assert.Equal(t, "ymax", bboxCol.BboxColumnFieldNames.Ymax)
}

func TestGetBboxColumnIdxV110NonStandardBboxCol(t *testing.T) {
	f, fileErr := os.Open("../testdata/cases/example-v1.1.0-covering.parquet")
	require.NoError(t, fileErr)
	reader, readerErr := file.NewParquetReader(f)
	require.NoError(t, readerErr)
	defer reader.Close()

	metadata, err := geoparquet.GetMetadata(reader.MetaData().KeyValueMetadata())
	require.NoError(t, err)

	// there is a bbox col in the file with the non-standard name "geometry_bbox",
	// we expect index 6
	bboxCol := geoparquet.GetBboxColumn(reader.MetaData().Schema, metadata)
	assert.Equal(t, 6, bboxCol.Index)
	assert.Equal(t, "geometry_bbox", bboxCol.Name)
	assert.Equal(t, 5, bboxCol.BaseColumn)
	assert.Equal(t, "", bboxCol.BaseColumnEncoding)
	assert.Equal(t, "xmin", bboxCol.BboxColumnFieldNames.Xmin)
	assert.Equal(t, "ymin", bboxCol.BboxColumnFieldNames.Ymin)
	assert.Equal(t, "xmax", bboxCol.BboxColumnFieldNames.Xmax)
	assert.Equal(t, "ymax", bboxCol.BboxColumnFieldNames.Ymax)
	assert.Equal(t, 6, reader.MetaData().Schema.Root().FieldIndexByName("geometry_bbox"))
}

func TestFilterRecordBatchByBboxV100(t *testing.T) {
	fileReader, fileErr := os.Open("../testdata/cases/example-v1.0.0.parquet")
	require.NoError(t, fileErr)
	defer fileReader.Close()

	recordReader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{Reader: fileReader})
	require.NoError(t, err)
	defer recordReader.Close()

	record, readErr := recordReader.Read()
	require.NoError(t, readErr)
	assert.Equal(t, int64(6), record.NumCols())
	assert.Equal(t, int64(5), record.NumRows())

	inputBbox := &geo.Bbox{Xmin: 34.0, Ymin: -7.0, Xmax: 36.0, Ymax: -6.0}

	filteredRecord, err := geoparquet.FilterRecordBatchByBbox(context.Background(), &record, inputBbox, &geoparquet.BboxColumn{
		Index:      -1,
		BaseColumn: 0,
		BboxColumnFieldNames: geoparquet.BboxColumnFieldNames{
			Xmin: "xmin",
			Ymin: "ymin",
			Xmax: "xmax",
			Ymax: "ymax",
		},
	})
	require.NoError(t, err)

	// we expect only one row, namely Tanzania
	assert.Equal(t, int64(6), (*filteredRecord).NumCols())
	assert.Equal(t, int64(1), (*filteredRecord).NumRows())

	country := (*filteredRecord).Column(recordReader.Schema().ColumnIndexByName("name")).ValueStr(0)
	assert.Equal(t, "Tanzania", country)
}

func TestFilterRecordBatchByBboxV110(t *testing.T) {
	fileReader, fileErr := os.Open("../testdata/cases/example-v1.1.0.parquet")
	require.NoError(t, fileErr)
	defer fileReader.Close()

	recordReader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{Reader: fileReader})
	require.NoError(t, err)
	defer recordReader.Close()

	record, readErr := recordReader.Read()
	require.NoError(t, readErr)
	assert.Equal(t, int64(7), record.NumCols())
	assert.Equal(t, int64(5), record.NumRows())

	inputBbox := &geo.Bbox{Xmin: 34.0, Ymin: -7.0, Xmax: 36.0, Ymax: -6.0}

	filteredRecord, err := geoparquet.FilterRecordBatchByBbox(context.Background(), &record, inputBbox, &geoparquet.BboxColumn{
		Index:              6,
		BaseColumn:         5,
		BaseColumnEncoding: "wkb",
		BboxColumnFieldNames: geoparquet.BboxColumnFieldNames{
			Xmin: "xmin",
			Ymin: "ymin",
			Xmax: "xmax",
			Ymax: "ymax",
		},
	})
	require.NoError(t, err)

	// we expect only one row, namely Tanzania
	assert.Equal(t, int64(7), (*filteredRecord).NumCols())
	assert.Equal(t, int64(1), (*filteredRecord).NumRows())

	country := (*filteredRecord).Column(recordReader.Schema().ColumnIndexByName("name")).ValueStr(0)
	assert.Equal(t, "Tanzania", country)
}

func TestFilterRecordBatchByBboxV110NonStandardBboxCol(t *testing.T) {
	fileReader, fileErr := os.Open("../testdata/cases/example-v1.1.0-covering.parquet")
	require.NoError(t, fileErr)
	defer fileReader.Close()

	recordReader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{Reader: fileReader})
	require.NoError(t, err)
	defer recordReader.Close()

	record, readErr := recordReader.Read()
	require.NoError(t, readErr)
	assert.Equal(t, int64(7), record.NumCols())
	assert.Equal(t, int64(5), record.NumRows())

	inputBbox := &geo.Bbox{Xmin: 34.0, Ymin: -7.0, Xmax: 36.0, Ymax: -6.0}

	filteredRecord, err := geoparquet.FilterRecordBatchByBbox(context.Background(), &record, inputBbox, &geoparquet.BboxColumn{
		Index:              6,
		BaseColumn:         5,
		BaseColumnEncoding: "wkb",
		BboxColumnFieldNames: geoparquet.BboxColumnFieldNames{
			Xmin: "xmin",
			Ymin: "ymin",
			Xmax: "xmax",
			Ymax: "ymax",
		},
	})
	require.NoError(t, err)

	// we expect only one row, namely Tanzania
	assert.Equal(t, int64(7), (*filteredRecord).NumCols())
	assert.Equal(t, int64(1), (*filteredRecord).NumRows())

	country := (*filteredRecord).Column(recordReader.Schema().ColumnIndexByName("name")).ValueStr(0)
	assert.Equal(t, "Tanzania", country)
}
