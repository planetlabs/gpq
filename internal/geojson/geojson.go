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

package geojson

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/encoding/wkt"
	orbjson "github.com/paulmach/orb/geojson"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/segmentio/parquet-go"
)

type FeatureWriter struct {
	writer   io.Writer
	schema   *parquet.Schema
	metadata *geoparquet.Metadata
	writing  bool
}

func NewFeatureWriter(writer io.Writer, metadata *geoparquet.Metadata, schema *parquet.Schema) (*FeatureWriter, error) {
	featureWriter := &FeatureWriter{
		writer:   writer,
		schema:   schema,
		metadata: metadata,
	}
	return featureWriter, nil
}

var stringType = parquet.String().Type()

func toFeature(row parquet.Row, schema *parquet.Schema, metadata *geoparquet.Metadata) (*Feature, error) {
	properties := map[string]any{}
	if err := schema.Reconstruct(&properties, row); err != nil {
		return nil, err
	}

	var primaryGeometry orb.Geometry

	for geometryName := range metadata.Columns {
		geometryInterface, ok := properties[geometryName]
		if !ok {
			return nil, fmt.Errorf("missing geometry column: %s", geometryName)
		}

		if geometryName == metadata.PrimaryColumn {
			delete(properties, geometryName)
		}
		if geometryInterface == nil {
			continue
		}

		geometryString, ok := geometryInterface.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected geometry type: %t", geometryInterface)
		}

		encoding := metadata.Columns[geometryName].Encoding
		if encoding == "" {
			column, ok := schema.Lookup(geometryName)
			if !ok {
				return nil, fmt.Errorf("missing column: %s", geometryName)
			}
			nodeType := column.Node.Type()
			if nodeType == stringType {
				encoding = geoparquet.EncodingWKT
			} else if nodeType == parquet.ByteArrayType {
				encoding = geoparquet.EncodingWKB
			} else {
				return nil, fmt.Errorf("unsupported geometry type: %s", nodeType)
			}
		}

		var geometry orb.Geometry

		switch strings.ToUpper(encoding) {
		case geoparquet.EncodingWKB:
			g, err := wkb.Unmarshal([]byte(geometryString))
			if err != nil {
				return nil, fmt.Errorf("trouble reading geometry: %w", err)
			}
			geometry = g
		case geoparquet.EncodingWKT:
			g, err := wkt.Unmarshal(geometryString)
			if err != nil {
				return nil, fmt.Errorf("trouble reading geometry: %w", err)
			}
			geometry = g
		default:
			return nil, fmt.Errorf("unsupported encoding: %s", encoding)
		}

		if geometryName == metadata.PrimaryColumn {
			primaryGeometry = geometry
		} else {
			properties[geometryName] = geometry
		}
	}

	feature := &Feature{
		Properties: properties,
		Geometry:   primaryGeometry,
	}
	return feature, nil
}

func (w *FeatureWriter) Write(row parquet.Row) error {
	if !w.writing {
		_, err := io.WriteString(w.writer, `{"type":"FeatureCollection","features":[`)
		if err != nil {
			return err
		}
		w.writing = true
	} else {
		_, err := io.WriteString(w.writer, ",")
		if err != nil {
			return err
		}
	}

	feature, err := toFeature(row, w.schema, w.metadata)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(w.writer)
	return encoder.Encode(feature)
}

func (w *FeatureWriter) Close() error {
	if w.writing {
		_, err := io.WriteString(w.writer, "]}")
		if err != nil {
			return err
		}
		w.writing = false
	} else {
		_, err := io.WriteString(w.writer, `{"type":"FeatureCollection","features":[]}`)
		if err != nil {
			return err
		}
	}
	return nil
}

type Feature struct {
	Id         any            `json:"id,omitempty"`
	Type       string         `json:"type"`
	Geometry   orb.Geometry   `json:"geometry"`
	Properties map[string]any `json:"properties"`
}

func (f *Feature) MarshalJSON() ([]byte, error) {
	m := map[string]any{
		"type":       "Feature",
		"geometry":   orbjson.NewGeometry(f.Geometry),
		"properties": f.Properties,
	}
	if f.Id != nil {
		m["id"] = f.Id
	}
	return json.Marshal(m)
}

type jsonFeature struct {
	Id         any             `json:"id,omitempty"`
	Type       string          `json:"type"`
	Geometry   json.RawMessage `json:"geometry"`
	Properties map[string]any  `json:"properties"`
}

var rawNull = json.RawMessage{'n', 'u', 'l', 'l'}

func isRawNull(raw json.RawMessage) bool {
	if len(raw) != len(rawNull) {
		return false
	}
	for i, c := range raw {
		if c != rawNull[i] {
			return false
		}
	}
	return true
}

func (f *Feature) UnmarshalJSON(data []byte) error {
	jf := &jsonFeature{}
	if err := json.Unmarshal(data, jf); err != nil {
		return err
	}

	f.Type = jf.Type
	f.Id = jf.Id
	f.Properties = jf.Properties

	if isRawNull(jf.Geometry) {
		return nil
	}
	geometry := &orbjson.Geometry{}
	if err := json.Unmarshal(jf.Geometry, geometry); err != nil {
		return err
	}

	f.Geometry = geometry.Geometry()
	return nil
}

func FromParquet(file *parquet.File, writer io.Writer) error {
	rowReader := geoparquet.NewRowReader(file)

	metadata, geoErr := geoparquet.GetMetadata(file)
	if geoErr != nil {
		return geoErr
	}

	featureWriter, writerErr := NewFeatureWriter(writer, metadata, file.Schema())
	if writerErr != nil {
		return writerErr
	}

	for {
		row, readErr := rowReader.Next()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}

		writeErr := featureWriter.Write(row)
		if writeErr != nil {
			return writeErr
		}
	}

	return featureWriter.Close()
}

type FeatureReader struct {
	buffer     []*Feature
	collection bool
	decoder    *json.Decoder
}

func NewFeatureReader(input io.Reader) *FeatureReader {
	return &FeatureReader{
		decoder: json.NewDecoder(input),
	}
}

func (reader *FeatureReader) Converter(min int, max int) (*TypeConverter, error) {
	features := []*Feature{}
	schemaBuilder := &SchemaBuilder{}
	for attempts := 0; attempts < max-1; attempts += 1 {
		feature, readErr := reader.Next()
		if readErr == io.EOF {
			if attempts == 0 {
				return nil, errors.New("empty feature collection")
			}
			reader.buffer = features
			return schemaBuilder.Converter()
		}
		if readErr != nil {
			return nil, readErr
		}
		features = append(features, feature)

		if complete := schemaBuilder.Add(feature); complete && attempts >= min-1 {
			reader.buffer = features
			return schemaBuilder.Converter()
		}
	}
	return nil, fmt.Errorf("failed to generate converter from first %d features", max)
}

func (r *FeatureReader) Next() (*Feature, error) {
	if len(r.buffer) > 0 {
		feature := r.buffer[0]
		r.buffer = r.buffer[1:]
		return feature, nil
	}

	if r.decoder == nil {
		return nil, io.EOF
	}

	if r.collection {
		return r.readFeature()
	}

	defer func() {
		if !r.collection {
			r.decoder = nil
		}
	}()

	token, err := r.decoder.Token()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}

	delim, ok := token.(json.Delim)
	if !ok || delim != json.Delim('{') {
		return nil, fmt.Errorf("expected a JSON object, got %s", token)
	}

	var parsedType string
	var feature *Feature
	var coordinatesJSON json.RawMessage
	for {
		keyToken, keyErr := r.decoder.Token()
		if keyErr == io.EOF {
			if feature == nil {
				return nil, io.EOF
			}
			return feature, nil
		}
		if keyErr != nil {
			return nil, keyErr
		}

		delim, ok := keyToken.(json.Delim)
		if ok && delim == json.Delim('}') {
			if feature == nil {
				return nil, errors.New("expected a FeatureCollection, a Feature, or a Geometry object")
			}
			return feature, nil
		}

		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected token: %s", token)
		}

		if key == "geometry" {
			if feature == nil {
				feature = &Feature{}
			} else if feature.Geometry != nil {
				return nil, errors.New("found duplicate geometry")
			}
			geometry := &orbjson.Geometry{}
			if err := r.decoder.Decode(geometry); err != nil {
				return nil, fmt.Errorf("trouble parsing geometry: %w", err)
			}
			feature.Geometry = geometry.Geometry()
			continue
		}

		if key == "properties" {
			if feature == nil {
				feature = &Feature{}
			} else if feature.Properties != nil {
				return nil, errors.New("found duplicate properties")
			}
			properties := map[string]any{}
			if err := r.decoder.Decode(&properties); err != nil {
				return nil, fmt.Errorf("trouble parsing properties: %w", err)
			}
			feature.Properties = properties
			continue
		}

		if key == "coordinates" {
			if feature == nil {
				feature = &Feature{}
			} else if feature.Geometry != nil {
				return nil, errors.New("found unexpected coordinates")
			}
			if coordinatesJSON != nil {
				return nil, errors.New("found duplicate coordinates")
			}
			if err := r.decoder.Decode(&coordinatesJSON); err != nil {
				return nil, fmt.Errorf("trouble parsing coordinates")
			}
			if parsedType != "" {
				return r.featureFromCoordinates(parsedType, coordinatesJSON)
			}
			continue
		}

		valueToken, valueErr := r.decoder.Token()
		if valueErr != nil {
			return nil, valueErr
		}

		if key == "type" {
			if parsedType != "" {
				return nil, errors.New("found duplicate type")
			}
			value, ok := valueToken.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected type: %s", valueToken)
			}
			parsedType = value
			if coordinatesJSON != nil {
				return r.featureFromCoordinates(parsedType, coordinatesJSON)
			}
			continue
		}

		if key == "features" {
			if parsedType != "" && parsedType != "FeatureCollection" {
				return nil, fmt.Errorf("found features in unexpected %q type", parsedType)
			}
			delim, ok := valueToken.(json.Delim)
			if !ok || delim != json.Delim('[') {
				return nil, fmt.Errorf("expected an array of features, got %s", token)
			}
			r.collection = true
			return r.readFeature()
		}

		if key == "geometries" {
			if parsedType != "" && parsedType != "GeometryCollection" {
				return nil, fmt.Errorf("found geometries in unexpected %q type", parsedType)
			}
			delim, ok := valueToken.(json.Delim)
			if !ok || delim != json.Delim('[') {
				return nil, fmt.Errorf("expected an array of geometries, got %s", token)
			}
			return r.readGeometryCollection()
		}

		if key == "id" {
			if feature == nil {
				feature = &Feature{}
			} else if feature.Id != nil {
				return nil, errors.New("found duplicate id")
			}
			_, stringId := valueToken.(string)
			_, floatId := valueToken.(float64)
			if !(stringId || floatId) {
				return nil, fmt.Errorf("expected id to be a string or number, got: %v", valueToken)
			}
			feature.Id = valueToken
			continue
		}

		if delim, ok := valueToken.(json.Delim); ok {
			switch delim {
			case json.Delim('['):
				err := r.scanToMatching('[', ']')
				if err != nil {
					return nil, err
				}
			case json.Delim('{'):
				err := r.scanToMatching('{', '}')
				if err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("unexpected token: %s", delim)
			}
		}

	}
}

func (r *FeatureReader) scanToMatching(fromDelim json.Delim, toDelim json.Delim) error {
	depth := 1
	for {
		token, err := r.decoder.Token()
		if err != nil {
			return fmt.Errorf("unexpected token: %w", err)
		}
		delim, ok := token.(json.Delim)
		if !ok {
			continue
		}
		if delim == fromDelim {
			depth += 1
			continue
		}
		if delim == toDelim {
			depth -= 1
			if depth == 0 {
				return nil
			}
		}
	}
}

func (r *FeatureReader) featureFromCoordinates(geometryType string, coordinatesJSON json.RawMessage) (*Feature, error) {
	prefix := []byte(`{"type":"` + geometryType + `","coordinates":`)
	geometryData := append(prefix, coordinatesJSON...)
	geometryData = append(geometryData, "}"...)
	geometry := &orbjson.Geometry{}
	if err := json.Unmarshal(geometryData, geometry); err != nil {
		return nil, fmt.Errorf("trouble parsing geometry coordinates: %w", err)
	}
	feature := &Feature{
		Geometry:   geometry.Geometry(),
		Properties: map[string]any{},
	}
	return feature, nil
}

func (r *FeatureReader) readFeature() (*Feature, error) {
	if !r.decoder.More() {
		r.decoder = nil
		return nil, io.EOF
	}
	feature := &Feature{}
	if err := r.decoder.Decode(feature); err != nil {
		return nil, err
	}
	return feature, nil
}

func (r *FeatureReader) readGeometryCollection() (*Feature, error) {
	feature := &Feature{Properties: map[string]any{}}

	if !r.decoder.More() {
		return feature, nil
	}

	geometries := []orb.Geometry{}
	for r.decoder.More() {
		geometry := &orbjson.Geometry{}
		if err := r.decoder.Decode(geometry); err != nil {
			return nil, fmt.Errorf("trouble parsing geometry: %w", err)
		}
		geometries = append(geometries, geometry.Geometry())
	}

	feature.Geometry = orb.Collection(geometries)
	return feature, nil
}

type ConvertOptions struct {
	MinFeatures int
	MaxFeatures int
}

var defaultOptions = &ConvertOptions{
	MinFeatures: 1,
	MaxFeatures: 50,
}

func ToParquet(input io.Reader, output io.Writer, convertOptions *ConvertOptions) error {
	reader := NewFeatureReader(input)

	if convertOptions == nil {
		convertOptions = defaultOptions
	}
	converter, converterErr := reader.Converter(convertOptions.MinFeatures, convertOptions.MaxFeatures)
	if converterErr != nil {
		return converterErr
	}

	schema := parquet.SchemaOf(reflect.New(converter.Type).Elem().Interface())

	options := []parquet.WriterOption{
		parquet.Compression(&parquet.Zstd),
		schema,
	}

	writerConfig, configErr := parquet.NewWriterConfig(options...)
	if configErr != nil {
		return configErr
	}

	writer := parquet.NewGenericWriter[any](output, writerConfig)

	var bounds *orb.Bound
	geometryTypeLookup := map[string]bool{}

	for {
		feature, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if feature.Geometry != nil {
			b := feature.Geometry.Bound()
			if bounds == nil {
				bounds = &b
			} else {
				b = b.Union(*bounds)
				bounds = &b
			}
			geometryTypeLookup[feature.Geometry.GeoJSONType()] = true
		}
		row, err := converter.Convert(feature)
		if err != nil {
			return err
		}
		_, writeErr := writer.Write([]any{row})
		if writeErr != nil {
			return writeErr
		}
	}

	metadata := GetDefaultMetadata()
	if bounds != nil {
		metadata.Columns[metadata.PrimaryColumn].Bounds = []float64{
			bounds.Left(), bounds.Bottom(), bounds.Right(), bounds.Top(),
		}
	}

	geometryTypes := []string{}
	if len(geometryTypeLookup) > 0 {
		for geometryType := range geometryTypeLookup {
			geometryTypes = append(geometryTypes, geometryType)
		}
	}
	metadata.Columns[metadata.PrimaryColumn].GeometryTypes = geometryTypes

	metadataEncoded, jsonErr := json.Marshal(metadata)
	if jsonErr != nil {
		return fmt.Errorf("failed to serialize geo metadata: %w", jsonErr)
	}

	writer.SetKeyValueMetadata(geoparquet.MetadataKey, string(metadataEncoded))
	return writer.Close()
}
