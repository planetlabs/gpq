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
	"os"
	"testing"

	"github.com/paulmach/orb"
	"github.com/planetlabs/gpq/internal/geojson"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaOf(t *testing.T) {
	input, openErr := os.Open("testdata/example.geojson")
	require.NoError(t, openErr)

	reader := geojson.NewFeatureReader(input)
	feature, readErr := reader.Next()
	require.NoError(t, readErr)

	schema, schemaErr := geojson.SchemaOf(feature)
	require.NoError(t, schemaErr)
	require.Len(t, schema.Fields(), 6)

	continent, ok := schema.Lookup("continent")
	require.True(t, ok)
	assert.True(t, continent.Node.Optional())
	assert.Equal(t, parquet.String().Type(), continent.Node.Type())

	name, ok := schema.Lookup("name")
	require.True(t, ok)
	assert.True(t, name.Node.Optional())
	assert.Equal(t, parquet.String().Type(), name.Node.Type())

	iso, ok := schema.Lookup("iso_a3")
	require.True(t, ok)
	assert.True(t, iso.Node.Optional())
	assert.Equal(t, parquet.String().Type(), iso.Node.Type())

	gdp, ok := schema.Lookup("gdp_md_est")
	require.True(t, ok)
	assert.True(t, gdp.Node.Optional())
	assert.Equal(t, parquet.DoubleType, gdp.Node.Type())

	pop, ok := schema.Lookup("pop_est")
	require.True(t, ok)
	assert.True(t, pop.Node.Optional())
	assert.Equal(t, parquet.DoubleType, pop.Node.Type())

	geometry, ok := schema.Lookup("geometry")
	require.True(t, ok)
	assert.True(t, geometry.Node.Optional())
	assert.Equal(t, parquet.ByteArrayType, geometry.Node.Type())
}

func TestSchemaOfArrayOfStrings(t *testing.T) {
	path := "test"

	feature := &geojson.Feature{
		Properties: map[string]any{
			path: []any{"one", "two"},
		},
	}

	schema, schemaErr := geojson.SchemaOf(feature)
	require.NoError(t, schemaErr)

	column, ok := schema.Lookup(path)
	require.True(t, ok)

	assert.True(t, column.Node.Repeated())
	assert.Equal(t, parquet.String().Type(), column.Node.Type())
}

func TestSchemaOfArrayOfNumbers(t *testing.T) {
	path := "test"

	feature := &geojson.Feature{
		Properties: map[string]any{
			path: []any{float64(42), float64(21)},
		},
	}

	schema, schemaErr := geojson.SchemaOf(feature)
	require.NoError(t, schemaErr)

	column, ok := schema.Lookup(path)
	require.True(t, ok)

	assert.True(t, column.Node.Repeated())
	assert.Equal(t, parquet.DoubleType, column.Node.Type())
}

func converterFromFeature(feature *geojson.Feature) (*geojson.TypeConverter, error) {
	schemaBuilder := &geojson.SchemaBuilder{}
	schemaBuilder.Add(feature)
	return schemaBuilder.Converter()
}

func TestConverterSliceOfFloat(t *testing.T) {
	path := "test"

	feature := &geojson.Feature{
		Geometry: orb.Point{},
		Properties: map[string]any{
			path: []any{float64(42), float64(21)},
		},
	}

	converter, converterErr := converterFromFeature(feature)
	require.NoError(t, converterErr)

	_, convertErr := converter.Convert(feature)
	require.NoError(t, convertErr)
}

func TestConverterSliceOfString(t *testing.T) {
	path := "test"

	feature := &geojson.Feature{
		Geometry: orb.Point{},
		Properties: map[string]any{
			path: []any{"one", "two"},
		},
	}

	converter, converterErr := converterFromFeature(feature)
	require.NoError(t, converterErr)

	_, convertErr := converter.Convert(feature)
	require.NoError(t, convertErr)
}

func TestConverterSliceOfMixed(t *testing.T) {
	path := "test"

	feature := &geojson.Feature{
		Geometry: orb.Point{},
		Properties: map[string]any{
			path: []any{"one", "two"},
		},
	}

	converter, converterErr := converterFromFeature(feature)
	require.NoError(t, converterErr)

	mixed := &geojson.Feature{
		Geometry: orb.Point{},
		Properties: map[string]any{
			path: []any{"oops", 42},
		},
	}

	_, convertErr := converter.Convert(mixed)
	assert.EqualError(t, convertErr, "unable to convert value [oops 42] for \"test\": mixed array, expected string, but got int")
}

func TestConverterNilGeometry(t *testing.T) {
	path := "test"

	feature := &geojson.Feature{
		Geometry: orb.Point{},
		Properties: map[string]any{
			path: "has geom",
		},
	}

	converter, converterErr := converterFromFeature(feature)
	require.NoError(t, converterErr)

	null := &geojson.Feature{
		Properties: map[string]any{
			path: "hasn't geom",
		},
	}

	_, convertErr := converter.Convert(null)
	assert.NoError(t, convertErr)
}

func TestConverterNilSlice(t *testing.T) {
	path := "test"

	feature := &geojson.Feature{
		Properties: map[string]any{
			path: []any{"one", "two"},
		},
	}

	converter, converterErr := converterFromFeature(feature)
	require.NoError(t, converterErr)

	null := &geojson.Feature{
		Properties: map[string]any{},
	}

	_, convertErr := converter.Convert(null)
	assert.NoError(t, convertErr)
}

func TestSchemaBuilder(t *testing.T) {
	prop1 := "test-property-1"
	prop2 := "test-property-2"

	features := []*geojson.Feature{
		{
			Properties: map[string]any{
				prop1: "test-value-1",
				prop2: "test-value-2",
			},
		},
	}

	schemaBuilder := &geojson.SchemaBuilder{}
	complete := schemaBuilder.Add(features[0])
	assert.True(t, complete)

	schema, schemaErr := schemaBuilder.Schema()
	require.NoError(t, schemaErr)

	require.NoError(t, schemaErr)
	require.Len(t, schema.Fields(), 3)

	col1, ok := schema.Lookup(prop1)
	require.True(t, ok)
	assert.True(t, col1.Node.Optional())
	assert.Equal(t, parquet.String().Type(), col1.Node.Type())

	col2, ok := schema.Lookup(prop2)
	require.True(t, ok)
	assert.True(t, col2.Node.Optional())
	assert.Equal(t, parquet.String().Type(), col2.Node.Type())

	geom, ok := schema.Lookup("geometry")
	require.True(t, ok)
	assert.True(t, geom.Node.Optional())
	assert.Equal(t, parquet.ByteArrayType, geom.Node.Type())
}

func TestSchemaBuilderSparse(t *testing.T) {
	prop1 := "test-property-1"
	prop2 := "test-property-2"

	features := []*geojson.Feature{
		{
			Properties: map[string]any{
				prop1: "test-value-1",
			},
		},
		{
			Properties: map[string]any{
				prop2: "test-value-2",
			},
		},
	}

	schemaBuilder := &geojson.SchemaBuilder{}

	assert.True(t, schemaBuilder.Add(features[0]))
	assert.True(t, schemaBuilder.Add(features[1]))

	schema, schemaErr := schemaBuilder.Schema()
	require.NoError(t, schemaErr)

	require.NoError(t, schemaErr)
	require.Len(t, schema.Fields(), 3)

	col1, ok := schema.Lookup(prop1)
	require.True(t, ok)
	assert.True(t, col1.Node.Optional())
	assert.Equal(t, parquet.String().Type(), col1.Node.Type())

	col2, ok := schema.Lookup(prop2)
	require.True(t, ok)
	assert.True(t, col2.Node.Optional())
	assert.Equal(t, parquet.String().Type(), col2.Node.Type())

	geom, ok := schema.Lookup("geometry")
	require.True(t, ok)
	assert.True(t, geom.Node.Optional())
	assert.Equal(t, parquet.ByteArrayType, geom.Node.Type())
}

func TestSchemaBuilderSparseNulls(t *testing.T) {
	prop1 := "test-property-1"
	prop2 := "test-property-2"

	features := []*geojson.Feature{
		{
			Properties: map[string]any{
				prop1: "test-value-1",
				prop2: nil,
			},
		},
		{
			Properties: map[string]any{
				prop1: nil,
				prop2: nil,
			},
		},
		{
			Properties: map[string]any{
				prop1: nil,
				prop2: "test-value-2",
			},
		},
	}

	schemaBuilder := &geojson.SchemaBuilder{}

	assert.False(t, schemaBuilder.Add(features[0]))
	assert.False(t, schemaBuilder.Add(features[1]))
	assert.True(t, schemaBuilder.Add(features[2]))

	schema, schemaErr := schemaBuilder.Schema()
	require.NoError(t, schemaErr)

	require.NoError(t, schemaErr)
	require.Len(t, schema.Fields(), 3)

	col1, ok := schema.Lookup(prop1)
	require.True(t, ok)
	assert.True(t, col1.Node.Optional())
	assert.Equal(t, parquet.String().Type(), col1.Node.Type())

	col2, ok := schema.Lookup(prop2)
	require.True(t, ok)
	assert.True(t, col2.Node.Optional())
	assert.Equal(t, parquet.String().Type(), col2.Node.Type())

	geom, ok := schema.Lookup("geometry")
	require.True(t, ok)
	assert.True(t, geom.Node.Optional())
	assert.Equal(t, parquet.ByteArrayType, geom.Node.Type())
}
