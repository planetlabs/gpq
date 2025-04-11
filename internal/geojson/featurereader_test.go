package geojson_test

import (
	"io"
	"os"
	"testing"

	"github.com/paulmach/orb"
	"github.com/planetlabs/gpq/internal/geo"
	"github.com/planetlabs/gpq/internal/geojson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFeatureReader(t *testing.T) {
	file, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	features := []*geo.Feature{}
	for {
		feature, err := reader.Read()
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

	features := []*geo.Feature{}
	for {
		feature, err := reader.Read()
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

	features := []*geo.Feature{}
	for {
		feature, err := reader.Read()
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

func TestFeatureReaderNewLineDelimited(t *testing.T) {
	file, openErr := os.Open("testdata/new-line-delimited.ndgeojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	features := []*geo.Feature{}
	for {
		feature, err := reader.Read()
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

func TestFeatureReaderBbox(t *testing.T) {
	file, openErr := os.Open("testdata/bbox.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	features := []*geo.Feature{}
	for {
		feature, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		features = append(features, feature)
	}
	require.Len(t, features, 5)

	feature := features[0]
	require.NotNil(t, feature.Geometry)
	assert.Equal(t, "MultiPolygon", feature.Geometry.GeoJSONType())
	assert.Equal(t, map[string]any{
		"continent":  "Oceania",
		"gdp_md_est": 8374.0,
		"iso_a3":     "FJI",
		"name":       "Fiji",
		"pop_est":    920938.0}, feature.Properties)
	assert.Equal(t, -180.0, feature.Bbox.Min.X())
	assert.Equal(t, -18.28799, feature.Bbox.Min.Y())
	assert.Equal(t, 180.0, feature.Bbox.Max.X())
	assert.Equal(t, -16.020882256741224, feature.Bbox.Max.Y())
}

func TestFeatureReaderBboxInvalid(t *testing.T) {
	file, openErr := os.Open("testdata/bad-bbox.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	feature, err := reader.Read()
	require.ErrorContains(t, err, "invalid bbox")
	require.Nil(t, feature)
}

func TestFeatureReaderBadNewLineDelimited(t *testing.T) {
	file, openErr := os.Open("testdata/bad-new-line-delimited.ndgeojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	first, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Oceania", first.Properties["continent"])

	_, err = reader.Read()
	assert.ErrorContains(t, err, "unexpected end of JSON input")
}

func TestFeatureReaderEmptyFeatureCollection(t *testing.T) {
	file, openErr := os.Open("testdata/empty-collection.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	feature, err := reader.Read()
	assert.Nil(t, feature)
	assert.Equal(t, io.EOF, err)
}

func TestFeatureReaderBadCollection(t *testing.T) {
	file, openErr := os.Open("testdata/bad-collection.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	feature, noErr := reader.Read()
	assert.NotNil(t, feature)
	assert.NoError(t, noErr)

	noFeature, err := reader.Read()
	require.Nil(t, noFeature)
	require.EqualError(t, err, "geojson: invalid geometry")
}

func TestFeatureReaderNotGeoJSON(t *testing.T) {
	file, openErr := os.Open("testdata/not-geojson.json")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	feature, err := reader.Read()
	assert.Nil(t, feature)
	assert.EqualError(t, err, "expected a FeatureCollection, a Feature, or a Geometry object")
}

func TestFeatureReaderNotGeoJSONArray(t *testing.T) {
	file, openErr := os.Open("testdata/array.json")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(file)

	feature, err := reader.Read()
	assert.Nil(t, feature)
	assert.EqualError(t, err, "expected a JSON object, got [")
}
