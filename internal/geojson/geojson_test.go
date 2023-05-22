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
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/planetlabs/gpq/internal/geojson"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromParquetv040(t *testing.T) {
	input := "../testdata/cases/example-v0.4.0.parquet"
	reader, openErr := os.Open(input)
	require.NoError(t, openErr)

	info, statErr := os.Stat(input)
	require.NoError(t, statErr)

	file, fileErr := parquet.OpenFile(reader, info.Size())
	require.NoError(t, fileErr)

	buffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(file, buffer)
	assert.NoError(t, convertErr)

	expected, err := os.ReadFile("testdata/example.geojson")
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), buffer.String())
}

func TestFromParquetv100Beta1(t *testing.T) {
	input := "../testdata/cases/example-v1.0.0-beta.1.parquet"
	reader, openErr := os.Open(input)
	require.NoError(t, openErr)

	info, statErr := os.Stat(input)
	require.NoError(t, statErr)

	file, fileErr := parquet.OpenFile(reader, info.Size())
	require.NoError(t, fileErr)

	buffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(file, buffer)
	assert.NoError(t, convertErr)

	expected, err := os.ReadFile("testdata/example.geojson")
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), buffer.String())
}

func TestFeatureReader(t *testing.T) {
	file, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	features := []*geojson.Feature{}
	for {
		feature, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		features = append(features, feature)
	}
	require.Len(t, features, 5)

	fiji := features[0]
	assert.NotNil(t, fiji.Geometry)
	assert.Equal(t, "Oceania", fiji.Properties["continent"])
	assert.Equal(t, float64(920938), fiji.Properties["pop_est"])

	usa := features[4]
	assert.NotNil(t, usa.Geometry)
	assert.Equal(t, "North America", usa.Properties["continent"])
	assert.Equal(t, float64(326625791), usa.Properties["pop_est"])
}

func TestFeatureReaderPointGeometry(t *testing.T) {
	file, openErr := os.Open("testdata/point-geometry.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	features := []*geojson.Feature{}
	for {
		feature, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		features = append(features, feature)
	}
	require.Len(t, features, 1)

	feature := features[0]
	require.NotNil(t, feature.Geometry)
	assert.Equal(t, "Point", feature.Geometry.GeoJSONType())
	point, ok := feature.Geometry.(orb.Point)
	require.True(t, ok)
	assert.True(t, point.Equal(orb.Point{1, 2}))
	assert.Len(t, feature.Properties, 0)
}

func TestFeatureReaderSingleFeature(t *testing.T) {
	file, openErr := os.Open("testdata/feature.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	features := []*geojson.Feature{}
	for {
		feature, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		features = append(features, feature)
	}
	require.Len(t, features, 1)

	feature := features[0]
	require.NotNil(t, feature.Geometry)
	assert.Equal(t, "Point", feature.Geometry.GeoJSONType())
	point, ok := feature.Geometry.(orb.Point)
	require.True(t, ok)
	assert.True(t, point.Equal(orb.Point{1, 2}))
	assert.Equal(t, map[string]any{"name": "test"}, feature.Properties)
}

func TestFeatureReaderEmptyFeatureCollection(t *testing.T) {
	file, openErr := os.Open("testdata/empty-collection.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	feature, err := reader.Next()
	assert.Nil(t, feature)
	assert.Equal(t, io.EOF, err)
}

func TestFeatureReaderBadCollection(t *testing.T) {
	file, openErr := os.Open("testdata/bad-collection.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	feature, noErr := reader.Next()
	assert.NotNil(t, feature)
	assert.NoError(t, noErr)

	noFeature, err := reader.Next()
	require.Nil(t, noFeature)
	require.EqualError(t, err, "geojson: invalid geometry")
}

func TestFeatureReaderNotGeoJSON(t *testing.T) {
	file, openErr := os.Open("testdata/not-geojson.json")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	feature, err := reader.Next()
	assert.Nil(t, feature)
	assert.EqualError(t, err, "expected a FeatureCollection, a Feature, or a Geometry object")
}

func TestFeatureReaderNotGeoJSONArray(t *testing.T) {
	file, openErr := os.Open("testdata/array.json")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	feature, err := reader.Next()
	assert.Nil(t, feature)
	assert.EqualError(t, err, "expected a JSON object, got [")
}

func TestToParquet(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	metadata, geoErr := geoparquet.GetMetadata(parquetFile)
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Len(t, geometryTypes, 2)
	assert.Contains(t, geometryTypes, "MultiPolygon")
	assert.Contains(t, geometryTypes, "Polygon")

	assert.Nil(t, metadata.Columns[metadata.PrimaryColumn].GeometryType)

	gotBounds := metadata.Columns[metadata.PrimaryColumn].Bounds
	assert.Equal(t, []float64{-180, -18.28799, 180, 83.23324000000001}, gotBounds)

	assert.Equal(t, int64(5), parquetFile.NumRows())

	geojsonBuffer := &bytes.Buffer{}
	fromParquetErr := geojson.FromParquet(parquetFile, geojsonBuffer)
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
	assert.EqualError(t, toParquetErr, "mixed types for \"stringProperty\", expected string, but got float64")
}

func TestToParquetRepeatedProps(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/repeated-props.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	schema := parquetFile.Schema()

	numbers, ok := schema.Lookup("numbers")
	require.True(t, ok)
	assert.True(t, numbers.Node.Repeated())
	assert.Equal(t, parquet.DoubleType, numbers.Node.Type())

	strings, ok := schema.Lookup("strings")
	require.True(t, ok)
	assert.True(t, strings.Node.Repeated())
	assert.Equal(t, parquet.String().Type(), strings.Node.Type())
}

func TestToParquetNullGeometry(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/null-geom.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	schema := parquetFile.Schema()

	place, ok := schema.Lookup("place")
	require.True(t, ok)
	assert.True(t, place.Node.Optional())
	assert.Equal(t, parquet.String().Type(), place.Node.Type())

	geometry, ok := schema.Lookup("geometry")
	require.True(t, ok)
	assert.True(t, geometry.Node.Optional())
	assert.Equal(t, parquet.ByteArrayType, geometry.Node.Type())
}

func TestToParquetAllNullGeometry(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/all-null-geom.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	metadata, geoErr := geoparquet.GetMetadata(parquetFile)
	require.NoError(t, geoErr)

	assert.Len(t, metadata.Columns[metadata.PrimaryColumn].GeometryTypes, 0)
	assert.Nil(t, metadata.Columns[metadata.PrimaryColumn].GeometryType)
	assert.Len(t, metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes(), 0)

	schema := parquetFile.Schema()

	place, ok := schema.Lookup("place")
	require.True(t, ok)
	assert.True(t, place.Node.Optional())
	assert.Equal(t, parquet.String().Type(), place.Node.Type())

	geometry, ok := schema.Lookup("geometry")
	require.True(t, ok)
	assert.True(t, geometry.Node.Optional())
	assert.Equal(t, parquet.ByteArrayType, geometry.Node.Type())
}

func TestToParqueStringId(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/string-id.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	metadata, geoErr := geoparquet.GetMetadata(parquetFile)
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Equal(t, []string{"Point"}, geometryTypes)
}

func TestToParqueNumberId(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/number-id.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	metadata, geoErr := geoparquet.GetMetadata(parquetFile)
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Equal(t, []string{"Point"}, geometryTypes)
}

func TestToParqueBooleanId(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/boolean-id.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	assert.ErrorContains(t, toParquetErr, "expected id to be a string or number, got: true")
}

func TestToParqueArrayId(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/array-id.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	assert.ErrorContains(t, toParquetErr, "expected id to be a string or number, got: [")
}

func TestToParqueObjectId(t *testing.T) {
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
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	metadata, geoErr := geoparquet.GetMetadata(parquetFile)
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
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	metadata, geoErr := geoparquet.GetMetadata(parquetFile)
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Equal(t, []string{"Point"}, geometryTypes)

	schema := parquetFile.Schema()

	place, ok := schema.Lookup("name")
	require.True(t, ok)
	assert.True(t, place.Node.Optional())
	assert.Equal(t, parquet.String().Type(), place.Node.Type())

	geometry, ok := schema.Lookup("geometry")
	require.True(t, ok)
	assert.True(t, geometry.Node.Optional())
	assert.Equal(t, parquet.ByteArrayType, geometry.Node.Type())
}

func TestToParquetExtraObject(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/extra-object.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, nil)
	require.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	metadata, geoErr := geoparquet.GetMetadata(parquetFile)
	require.NoError(t, geoErr)

	geometryTypes := metadata.Columns[metadata.PrimaryColumn].GetGeometryTypes()
	assert.Equal(t, []string{"Point"}, geometryTypes)

	schema := parquetFile.Schema()

	place, ok := schema.Lookup("name")
	require.True(t, ok)
	assert.True(t, place.Node.Optional())
	assert.Equal(t, parquet.String().Type(), place.Node.Type())

	geometry, ok := schema.Lookup("geometry")
	require.True(t, ok)
	assert.True(t, geometry.Node.Optional())
	assert.Equal(t, parquet.ByteArrayType, geometry.Node.Type())
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
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	jsonBuffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(parquetFile, jsonBuffer)
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
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	jsonBuffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(parquetFile, jsonBuffer)
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
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	jsonBuffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(parquetFile, jsonBuffer)
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
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	jsonBuffer := &bytes.Buffer{}
	convertErr := geojson.FromParquet(parquetFile, jsonBuffer)
	require.NoError(t, convertErr)

	assert.JSONEq(t, string(inputData), jsonBuffer.String())
}

func makeGeoParquet[T any](rows []T, metadata *geoparquet.Metadata) (*parquet.File, error) {
	data := &bytes.Buffer{}

	writer := parquet.NewGenericWriter[T](data)

	_, writeErr := writer.Write(rows)
	if writeErr != nil {
		return nil, fmt.Errorf("trouble writing rows: %w", writeErr)
	}

	jsonMetadata, jsonErr := json.Marshal(metadata)
	if jsonErr != nil {
		return nil, fmt.Errorf("trouble encoding metadata as json: %w", jsonErr)
	}

	writer.SetKeyValueMetadata(geoparquet.MetadataKey, string(jsonMetadata))
	closeErr := writer.Close()
	if closeErr != nil {
		return nil, fmt.Errorf("trouble closing writer: %w", closeErr)
	}

	return parquet.OpenFile(bytes.NewReader(data.Bytes()), int64(data.Len()))
}

func TestWKT(t *testing.T) {
	type RowType struct {
		Name     string `parquet:"name"`
		Geometry string `parquet:"geometry"`
	}

	rows := []*RowType{
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
	metadata.Columns[metadata.PrimaryColumn].Encoding = geoparquet.EncodingWKT

	file, fileErr := makeGeoParquet(rows, metadata)
	require.NoError(t, fileErr)

	output := &bytes.Buffer{}
	convertErr := geojson.FromParquet(file, output)
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
	type RowType struct {
		Name     string `parquet:"name"`
		Geometry string `parquet:"geometry"`
	}

	rows := []*RowType{
		{
			Name:     "test-point",
			Geometry: "POINT (1 2)",
		},
	}

	metadata := geoparquet.DefaultMetadata()
	metadata.Columns[metadata.PrimaryColumn].Encoding = ""

	file, fileErr := makeGeoParquet(rows, metadata)
	require.NoError(t, fileErr)

	output := &bytes.Buffer{}
	convertErr := geojson.FromParquet(file, output)
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
	type RowType struct {
		Name     string `parquet:"name"`
		Geometry []byte `parquet:"geometry"`
	}

	point, pointErr := wkb.Marshal(orb.Point{1, 2})
	require.NoError(t, pointErr)

	rows := []*RowType{
		{
			Name:     "test-point",
			Geometry: point,
		},
	}

	metadata := geoparquet.DefaultMetadata()

	file, fileErr := makeGeoParquet(rows, metadata)
	require.NoError(t, fileErr)

	output := &bytes.Buffer{}
	convertErr := geojson.FromParquet(file, output)
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
	type RowType struct {
		Name     string `parquet:"name"`
		Geometry []byte `parquet:"geometry"`
	}

	point, pointErr := wkb.Marshal(orb.Point{1, 2})
	require.NoError(t, pointErr)

	rows := []*RowType{
		{
			Name:     "test-point",
			Geometry: point,
		},
	}

	metadata := geoparquet.DefaultMetadata()
	metadata.Columns[metadata.PrimaryColumn].Encoding = ""

	file, fileErr := makeGeoParquet(rows, metadata)
	require.NoError(t, fileErr)

	output := &bytes.Buffer{}
	convertErr := geojson.FromParquet(file, output)
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
	convertOptions := &geojson.ConvertOptions{MinFeatures: 10, MaxFeatures: 100, Compression: "uncompressed"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	assert.Equal(t, parquet.Uncompressed.CompressionCodec(), parquetFile.Metadata().RowGroups[0].Columns[0].MetaData.Codec)
}

func TestCodecSnappy(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{MinFeatures: 10, MaxFeatures: 100, Compression: "snappy"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	assert.Equal(t, parquet.Snappy.CompressionCodec(), parquetFile.Metadata().RowGroups[0].Columns[0].MetaData.Codec)
}

func TestCodecGzip(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{MinFeatures: 10, MaxFeatures: 100, Compression: "gzip"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	assert.Equal(t, parquet.Gzip.CompressionCodec(), parquetFile.Metadata().RowGroups[0].Columns[0].MetaData.Codec)
}

func TestCodecBrotli(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{MinFeatures: 10, MaxFeatures: 100, Compression: "brotli"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	assert.Equal(t, parquet.Brotli.CompressionCodec(), parquetFile.Metadata().RowGroups[0].Columns[0].MetaData.Codec)
}

func TestCodecZstd(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{MinFeatures: 10, MaxFeatures: 100, Compression: "zstd"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	assert.Equal(t, parquet.Zstd.CompressionCodec(), parquetFile.Metadata().RowGroups[0].Columns[0].MetaData.Codec)
}

func TestCodecLz4raw(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{MinFeatures: 10, MaxFeatures: 100, Compression: "lz4raw"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.NoError(t, toParquetErr)

	parquetInput := bytes.NewReader(parquetBuffer.Bytes())
	parquetFile, openErr := parquet.OpenFile(parquetInput, parquetInput.Size())
	require.NoError(t, openErr)

	assert.Equal(t, parquet.Lz4Raw.CompressionCodec(), parquetFile.Metadata().RowGroups[0].Columns[0].MetaData.Codec)
}

func TestCodecInvalid(t *testing.T) {
	geojsonFile, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	parquetBuffer := &bytes.Buffer{}
	convertOptions := &geojson.ConvertOptions{MinFeatures: 10, MaxFeatures: 100, Compression: "invalid"}
	toParquetErr := geojson.ToParquet(geojsonFile, parquetBuffer, convertOptions)
	assert.EqualError(t, toParquetErr, "invalid compression codec invalid")
}
