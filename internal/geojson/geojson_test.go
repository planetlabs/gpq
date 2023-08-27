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

package geojson_test

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/apache/arrow/go/v14/parquet/schema"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/planetlabs/gpq/internal/geo"
	"github.com/planetlabs/gpq/internal/geojson"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/planetlabs/gpq/internal/pqutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromParquetv040(t *testing.T) {
	input := "../testdata/cases/example-v0.4.0.parquet"
	reader, openErr := os.Open(input)
	require.NoError(t, openErr)

	buffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(reader, buffer)
	assert.NoError(t, convertErr)

	expected, err := os.ReadFile("testdata/example.geojson")
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), buffer.String())
}

func TestFromParquetv100Beta1(t *testing.T) {
	input := "../testdata/cases/example-v1.0.0-beta.1.parquet"
	reader, openErr := os.Open(input)
	require.NoError(t, openErr)

	buffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(reader, buffer)
	assert.NoError(t, convertErr)

	expected, err := os.ReadFile("testdata/example.geojson")
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), buffer.String())
}

func TestToParquet(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	metadata, geoErr := geoparquet.GetMetadata(fileReader.MetaData().KeyValueMetadata())
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Len(t, geometryTypes, 2)
	assert.Contains(t, geometryTypes, "MultiPolygon")
	assert.Contains(t, geometryTypes, "Polygon")

	assert.Nil(t, metadata.Columns[metadata.PrimaryColumn].GeometryType)

	gotBounds := metadata.Columns[metadata.PrimaryColumn].Bounds
	assert.Equal(t, []float64{-180, -18.28799, 180, 83.23324000000001}, gotBounds)

	assert.Equal(t, int64(5), fileReader.NumRows())

	geojsonBuffer := &bytes.Buffer{}
	fromParquetErr := geojson.FromParquet(parquetInput, geojsonBuffer)
	require.NoError(t, fromParquetErr)

	expected, err := os.ReadFile("testdata/example.geojson")
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), geojsonBuffer.String())
}

func TestToParquetMismatchedTypes(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/mismatched-types.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	assert.EqualError(t, toParquetErr, "expected \"stringProperty\" to be a string, got 42")
}

func TestToParquetRepeatedProps(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/repeated-props.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	sc := fileReader.MetaData().Schema

	numbers, ok := pqutil.LookupListElementNode(sc, "numbers")
	require.True(t, ok)

	assert.Equal(t, parquet.Repetitions.Optional, numbers.RepetitionType())
	assert.Equal(t, parquet.Types.Double, numbers.PhysicalType())

	strings, ok := pqutil.LookupListElementNode(sc, "strings")
	require.True(t, ok)

	assert.Equal(t, parquet.Repetitions.Optional, strings.RepetitionType())
	assert.Equal(t, parquet.Types.ByteArray, strings.PhysicalType())
	assert.Equal(t, schema.StringLogicalType{}, strings.LogicalType())
}

func TestToParquetNullGeometry(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/null-geom.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	sc := fileReader.MetaData().Schema

	place, ok := pqutil.LookupPrimitiveNode(sc, "place")
	require.True(t, ok)
	assert.Equal(t, parquet.Repetitions.Optional, place.RepetitionType())
	assert.Equal(t, schema.StringLogicalType{}, place.LogicalType())

	geometry, ok := pqutil.LookupPrimitiveNode(sc, "geometry")
	require.True(t, ok)
	assert.Equal(t, parquet.Repetitions.Optional, geometry.RepetitionType())
	assert.Equal(t, parquet.Types.ByteArray, geometry.PhysicalType())
}

func TestToParquetAllNullGeometry(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/all-null-geom.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	metadata, geoErr := geoparquet.GetMetadata(fileReader.MetaData().KeyValueMetadata())
	require.NoError(t, geoErr)

	assert.Len(t, metadata.Columns[metadata.PrimaryColumn].GeometryTypes, 0)
	assert.Nil(t, metadata.Columns[metadata.PrimaryColumn].GeometryType)
	assert.Len(t, metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes(), 0)

	sc := fileReader.MetaData().Schema

	place, ok := pqutil.LookupPrimitiveNode(sc, "place")
	require.True(t, ok)
	assert.Equal(t, parquet.Repetitions.Optional, place.RepetitionType())
	assert.Equal(t, schema.StringLogicalType{}, place.LogicalType())

	geometry, ok := pqutil.LookupPrimitiveNode(sc, "geometry")
	require.True(t, ok)
	assert.Equal(t, parquet.Repetitions.Optional, geometry.RepetitionType())
	assert.Equal(t, parquet.Types.ByteArray, geometry.PhysicalType())
}

func TestToParquetStringId(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/string-id.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	metadata, geoErr := geoparquet.GetMetadata(fileReader.MetaData().KeyValueMetadata())
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Equal(t, []string{"Point"}, geometryTypes)
}

func TestToParquetNumberId(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/number-id.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	metadata, geoErr := geoparquet.GetMetadata(fileReader.MetaData().KeyValueMetadata())
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Equal(t, []string{"Point"}, geometryTypes)
}

func TestToParquetBooleanId(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/boolean-id.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	assert.ErrorContains(t, toParquetErr, "expected id to be a string or number, got: true")
}

func TestToParquetArrayId(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/array-id.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	assert.ErrorContains(t, toParquetErr, "expected id to be a string or number, got: [")
}

func TestToParquetObjectId(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/object-id.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	assert.ErrorContains(t, toParquetErr, "expected id to be a string or number, got: {")
}

func TestToParquetWithCRS(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/with-crs.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	metadata, geoErr := geoparquet.GetMetadata(fileReader.MetaData().KeyValueMetadata())
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Equal(t, []string{"Polygon"}, geometryTypes)
}

func TestToParquetExtraArray(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/extra-array.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	metadata, geoErr := geoparquet.GetMetadata(fileReader.MetaData().KeyValueMetadata())
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Equal(t, []string{"Point"}, geometryTypes)

	sc := fileReader.MetaData().Schema

	place, ok := pqutil.LookupPrimitiveNode(sc, "name")
	require.True(t, ok)
	assert.Equal(t, parquet.Repetitions.Optional, place.RepetitionType())
	assert.Equal(t, schema.StringLogicalType{}, place.LogicalType())

	geometry, ok := pqutil.LookupPrimitiveNode(sc, "geometry")
	require.True(t, ok)
	assert.Equal(t, parquet.Repetitions.Optional, geometry.RepetitionType())
	assert.Equal(t, parquet.Types.ByteArray, geometry.PhysicalType())
}

func TestToParquetExtraObject(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/extra-object.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	metadata, geoErr := geoparquet.GetMetadata(fileReader.MetaData().KeyValueMetadata())
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Equal(t, []string{"Point"}, geometryTypes)

	sc := fileReader.MetaData().Schema

	place, ok := pqutil.LookupPrimitiveNode(sc, "name")
	require.True(t, ok)
	assert.Equal(t, parquet.Repetitions.Optional, place.RepetitionType())
	assert.Equal(t, schema.StringLogicalType{}, place.LogicalType())

	geometry, ok := pqutil.LookupPrimitiveNode(sc, "geometry")
	require.True(t, ok)
	assert.Equal(t, parquet.Repetitions.Optional, geometry.RepetitionType())
	assert.Equal(t, parquet.Types.ByteArray, geometry.PhysicalType())
}

func TestRoundTripRepeatedProps(t *testing.T) {
	inputPath := "testdata/repeated-props.geojson"
	inputData, readErr := os.ReadFile(inputPath)
	require.NoError(t, readErr)
	inputReader := bytes.NewReader(inputData)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(inputReader, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())

	jsonBuffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(parquetInput, jsonBuffer)
	require.NoError(t, convertErr)

	assert.JSONEq(t, string(inputData), jsonBuffer.String())
}

func TestRoundTripNestedProps(t *testing.T) {
	inputPath := "testdata/nested-props.geojson"
	inputData, readErr := os.ReadFile(inputPath)
	require.NoError(t, readErr)
	inputReader := bytes.NewReader(inputData)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(inputReader, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())

	jsonBuffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(parquetInput, jsonBuffer)
	require.NoError(t, convertErr)

	assert.JSONEq(t, string(inputData), jsonBuffer.String())
}

func TestRoundTripNullGeometry(t *testing.T) {
	inputPath := "testdata/null-geom.geojson"
	inputData, readErr := os.ReadFile(inputPath)
	require.NoError(t, readErr)
	inputReader := bytes.NewReader(inputData)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(inputReader, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())

	jsonBuffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(parquetInput, jsonBuffer)
	require.NoError(t, convertErr)

	assert.JSONEq(t, string(inputData), jsonBuffer.String())
}

func TestRoundTripSparseProperties(t *testing.T) {
	inputPath := "testdata/sparse-properties.geojson"
	inputData, readErr := os.ReadFile(inputPath)
	require.NoError(t, readErr)
	inputReader := bytes.NewReader(inputData)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(inputReader, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())

	jsonBuffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(parquetInput, jsonBuffer)
	require.NoError(t, convertErr)

	assert.JSONEq(t, string(inputData), jsonBuffer.String())
}

func makeGeoParquetReader[T any](rows []T, metadata *geoparquet.Metadata) (*bytes.Reader, error) {
	data, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}

	parquetSchema, err := schema.NewSchemaFromStruct(rows[0])
	if err != nil {
		return nil, err
	}

	arrowSchema, err := pqarrow.FromParquet(parquetSchema, nil, nil)
	if err != nil {
		return nil, err
	}

	output := &bytes.Buffer{}
	recordWriter, err := geoparquet.NewRecordWriter(&geoparquet.WriterConfig{
		Writer:      output,
		Metadata:    metadata,
		ArrowSchema: arrowSchema,
	})
	if err != nil {
		return nil, err
	}

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(string(data)))
	if err != nil {
		return nil, err
	}

	if err := recordWriter.Write(rec); err != nil {
		return nil, err
	}
	if err := recordWriter.Close(); err != nil {
		return nil, err
	}

	return bytes.NewReader(output.Bytes()), nil
}

func TestWKT(t *testing.T) {
	type Row struct {
		Name     string `parquet:"name=name, logical=String" json:"name"`
		Geometry string `parquet:"name=geometry, logical=String" json:"geometry"`
	}

	rows := []*Row{
		{
			Name:     "test-point",
			Geometry: "POINT (1 2)",
		},
		{
			Name:     "test-line",
			Geometry: "LINESTRING (30 10, 10 30, 40 40)",
		},
	}

	metadata := geoparquet.DefaultMetadata()
	metadata.Columns[metadata.PrimaryColumn].Encoding = geo.EncodingWKT

	reader, readerErr := makeGeoParquetReader(rows, metadata)
	require.NoError(t, readerErr)

	output := &bytes.Buffer{}
	convertErr := geojson.FromParquet(reader, output)
	require.NoError(t, convertErr)

	expected := `{
		"type": "FeatureCollection",
		"features": [
			{
				"type": "Feature",
				"properties": {
					"name": "test-point"
				},
				"geometry": {
					"type": "Point",
					"coordinates": [1, 2]
				}
			},
			{
				"type": "Feature",
				"properties": {
					"name": "test-line"
				},
				"geometry": {
					"type": "LineString",
					"coordinates": [[30, 10], [10, 30], [40, 40]]
				}
			}
		]
	}`

	assert.JSONEq(t, expected, output.String())
}

func TestWKTNoEncoding(t *testing.T) {
	type Row struct {
		Name     string `parquet:"name=name, logical=String" json:"name"`
		Geometry string `parquet:"name=geometry, logical=String" json:"geometry"`
	}

	rows := []*Row{
		{
			Name:     "test-point",
			Geometry: "POINT (1 2)",
		},
	}

	metadata := geoparquet.DefaultMetadata()
	metadata.Columns[metadata.PrimaryColumn].Encoding = ""

	reader, readerErr := makeGeoParquetReader(rows, metadata)
	require.NoError(t, readerErr)

	output := &bytes.Buffer{}
	convertErr := geojson.FromParquet(reader, output)
	require.NoError(t, convertErr)

	expected := `{
		"type": "FeatureCollection",
		"features": [
			{
				"type": "Feature",
				"properties": {
					"name": "test-point"
				},
				"geometry": {
					"type": "Point",
					"coordinates": [1, 2]
				}
			}
		]
	}`

	assert.JSONEq(t, expected, output.String())
}

func TestWKB(t *testing.T) {
	type Row struct {
		Name     string `parquet:"name=name, logical=String" json:"name"`
		Geometry []byte `parquet:"name=geometry" json:"geometry"`
	}

	point, pointErr := wkb.Marshal(orb.Point{1, 2})
	require.NoError(t, pointErr)

	rows := []*Row{
		{
			Name:     "test-point",
			Geometry: point,
		},
	}

	metadata := geoparquet.DefaultMetadata()

	reader, readerErr := makeGeoParquetReader(rows, metadata)
	require.NoError(t, readerErr)

	output := &bytes.Buffer{}
	convertErr := geojson.FromParquet(reader, output)
	require.NoError(t, convertErr)

	expected := `{
		"type": "FeatureCollection",
		"features": [
			{
				"type": "Feature",
				"properties": {
					"name": "test-point"
				},
				"geometry": {
					"type": "Point",
					"coordinates": [1, 2]
				}
			}
		]
	}`

	assert.JSONEq(t, expected, output.String())
}

func TestWKBNoEncoding(t *testing.T) {
	type Row struct {
		Name     string `parquet:"name=name, logical=String" json:"name"`
		Geometry []byte `parquet:"name=geometry" json:"geometry"`
	}

	point, pointErr := wkb.Marshal(orb.Point{1, 2})
	require.NoError(t, pointErr)

	rows := []*Row{
		{
			Name:     "test-point",
			Geometry: point,
		},
	}

	metadata := geoparquet.DefaultMetadata()
	metadata.Columns[metadata.PrimaryColumn].Encoding = ""

	reader, readerErr := makeGeoParquetReader(rows, metadata)
	require.NoError(t, readerErr)

	output := &bytes.Buffer{}
	convertErr := geojson.FromParquet(reader, output)
	require.NoError(t, convertErr)

	expected := `{
		"type": "FeatureCollection",
		"features": [
			{
				"type": "Feature",
				"properties": {
					"name": "test-point"
				},
				"geometry": {
					"type": "Point",
					"coordinates": [1, 2]
				}
			}
		]
	}`

	assert.JSONEq(t, expected, output.String())
}

func TestCodecUncompressed(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{Compression: "uncompressed"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	assert.Equal(t, compress.Codecs.Uncompressed, compress.Compression(fileReader.MetaData().RowGroups[0].Columns[0].MetaData.Codec))
}

func TestCodecSnappy(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{Compression: "snappy"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	assert.Equal(t, compress.Codecs.Snappy, compress.Compression(fileReader.MetaData().RowGroups[0].Columns[0].MetaData.Codec))
}

func TestCodecGzip(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{Compression: "gzip"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	assert.Equal(t, compress.Codecs.Gzip, compress.Compression(fileReader.MetaData().RowGroups[0].Columns[0].MetaData.Codec))
}

func TestCodecBrotli(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{Compression: "brotli"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	assert.Equal(t, compress.Codecs.Brotli, compress.Compression(fileReader.MetaData().RowGroups[0].Columns[0].MetaData.Codec))
}

func TestCodecZstd(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{Compression: "zstd"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	fileReader, fileErr := file.NewParquetReader(parquetInput)
	require.NoError(t, fileErr)

	assert.Equal(t, compress.Codecs.Zstd, compress.Compression(fileReader.MetaData().RowGroups[0].Columns[0].MetaData.Codec))
}

func TestCodecInvalid(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{Compression: "invalid"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.EqualError(t, toParquetErr, "invalid compression codec invalid")
}
