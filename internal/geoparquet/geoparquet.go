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

package geoparquet

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/compress"
)

const (
	Version                     = "1.0.0-beta.1"
	MetadataKey                 = "geo"
	EncodingWKB                 = "WKB"
	EncodingWKT                 = "WKT"
	EdgesPlanar                 = "planar"
	EdgesSpherical              = "spherical"
	OrientationCounterClockwise = "counterclockwise"
	defaultGeometryColumn       = "geometry"
)

var GeometryTypes = []string{
	"Point",
	"LineString",
	"Polygon",
	"MultiPoint",
	"MultiLineString",
	"MultiPolygon",
	"GeometryCollection",
	"Point Z",
	"LineString Z",
	"Polygon Z",
	"MultiPoint Z",
	"MultiLineString Z",
	"MultiPolygon Z",
	"GeometryCollection Z",
}

type Metadata struct {
	Version       string                     `json:"version"`
	PrimaryColumn string                     `json:"primary_column"`
	Columns       map[string]*GeometryColumn `json:"columns"`
}

func (m *Metadata) Clone() *Metadata {
	clone := &Metadata{}
	*clone = *m
	clone.Columns = make(map[string]*GeometryColumn, len(m.Columns))
	for i, v := range m.Columns {
		clone.Columns[i] = v.clone()
	}
	return clone
}

type GeometryColumn struct {
	Encoding      string    `json:"encoding"`
	GeometryType  any       `json:"geometry_type,omitempty"`
	GeometryTypes any       `json:"geometry_types"`
	CRS           any       `json:"crs,omitempty"`
	Edges         string    `json:"edges,omitempty"`
	Orientation   string    `json:"orientation,omitempty"`
	Bounds        []float64 `json:"bbox,omitempty"`
	Epoch         float64   `json:"epoch,omitempty"`
}

func (g *GeometryColumn) clone() *GeometryColumn {
	clone := &GeometryColumn{}
	*clone = *g
	clone.Bounds = make([]float64, len(g.Bounds))
	copy(clone.Bounds, g.Bounds)
	return clone
}

func (col *GeometryColumn) GetGeometryTypes() []string {
	if multiType, ok := col.GeometryTypes.([]any); ok {
		types := make([]string, len(multiType))
		for i, value := range multiType {
			geometryType, ok := value.(string)
			if !ok {
				return nil
			}
			types[i] = geometryType
		}
		return types
	}

	if singleType, ok := col.GeometryType.(string); ok {
		return []string{singleType}
	}

	values, ok := col.GeometryType.([]any)
	if !ok {
		return nil
	}

	types := make([]string, len(values))
	for i, value := range values {
		geometryType, ok := value.(string)
		if !ok {
			return nil
		}
		types[i] = geometryType
	}

	return types
}

func getDefaultGeometryColumn() *GeometryColumn {
	return &GeometryColumn{
		Encoding:      EncodingWKB,
		GeometryTypes: []string{},
	}
}

func DefaultMetadata() *Metadata {
	return &Metadata{
		Version:       Version,
		PrimaryColumn: defaultGeometryColumn,
		Columns: map[string]*GeometryColumn{
			defaultGeometryColumn: getDefaultGeometryColumn(),
		},
	}
}

var ErrNoMetadata = fmt.Errorf("missing %s metadata key", MetadataKey)

func GetMetadataValue(file *parquet.File) (string, error) {
	value, ok := file.Lookup(MetadataKey)
	if !ok {
		return "", ErrNoMetadata
	}
	return value, nil
}

func GetMetadata(file *parquet.File) (*Metadata, error) {
	value, valueErr := GetMetadataValue(file)
	if valueErr != nil {
		return nil, valueErr
	}
	geoFileMetadata := &Metadata{}
	jsonErr := json.Unmarshal([]byte(value), geoFileMetadata)
	if jsonErr != nil {
		return nil, fmt.Errorf("unable to parse geo metadata: %w", jsonErr)
	}
	return geoFileMetadata, nil
}

const defaultBatchSize = 128

type RowReader struct {
	file       *parquet.File
	groups     []parquet.RowGroup
	groupIndex int
	rowIndex   int
	rowBuffer  []parquet.Row
	rowsRead   int
	reader     parquet.Rows
}

func NewRowReader(file *parquet.File) *RowReader {
	groups := file.RowGroups()

	return &RowReader{
		file:      file,
		groups:    groups,
		rowBuffer: make([]parquet.Row, defaultBatchSize),
	}
}

func (r *RowReader) closeReader() error {
	if r.reader == nil {
		return nil
	}
	err := r.reader.Close()
	r.reader = nil
	return err
}

func (r *RowReader) Next() (parquet.Row, error) {
	if r.groupIndex >= len(r.groups) {
		return nil, io.EOF
	}

	if r.rowIndex == 0 {
		if r.reader == nil {
			group := r.groups[r.groupIndex]
			r.reader = group.Rows()
		}
		rowsRead, readErr := r.reader.ReadRows(r.rowBuffer)
		r.rowsRead = rowsRead
		if readErr != nil {
			closeErr := r.closeReader()
			if readErr != io.EOF {
				return nil, readErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
		}
	}

	if r.rowIndex >= r.rowsRead {
		r.rowIndex = 0
		if r.rowsRead < len(r.rowBuffer) {
			if err := r.closeReader(); err != nil {
				return nil, err
			}
			r.groupIndex += 1
		}
		return r.Next()
	}

	row := r.rowBuffer[r.rowIndex]
	r.rowIndex += 1
	return row, nil
}

func (r *RowReader) Close() error {
	return r.closeReader()
}

type GenericWriter[T any] struct {
	writer   *parquet.GenericWriter[T]
	metadata *Metadata
}

func NewGenericWriter[T any](output io.Writer, metadata *Metadata, options ...parquet.WriterOption) *GenericWriter[T] {
	return &GenericWriter[T]{
		writer:   parquet.NewGenericWriter[T](output, options...),
		metadata: metadata,
	}
}

func (w *GenericWriter[T]) Write(rows []T) (int, error) {
	return w.writer.Write(rows)
}

func (w *GenericWriter[T]) Close() error {
	jsonMetadata, jsonErr := json.Marshal(w.metadata)
	if jsonErr != nil {
		return fmt.Errorf("trouble encoding metadata as json: %w", jsonErr)
	}

	w.writer.SetKeyValueMetadata(MetadataKey, string(jsonMetadata))
	return w.writer.Close()
}

var stringType = parquet.String().Type()

func Geometry(value any, name string, metadata *Metadata, schema *parquet.Schema) (orb.Geometry, string, error) {
	geometryString, ok := value.(string)
	if !ok {
		return nil, "", fmt.Errorf("unexpected geometry type: %t", value)
	}

	encoding := metadata.Columns[name].Encoding
	if encoding == "" {
		column, ok := schema.Lookup(name)
		if !ok {
			return nil, "", fmt.Errorf("missing column: %s", name)
		}
		nodeType := column.Node.Type()
		if nodeType == stringType {
			encoding = EncodingWKT
		} else if nodeType == parquet.ByteArrayType {
			encoding = EncodingWKB
		} else {
			return nil, "", fmt.Errorf("unsupported geometry type: %s", nodeType)
		}
	}

	var geometry orb.Geometry

	switch strings.ToUpper(encoding) {
	case EncodingWKB:
		g, err := wkb.Unmarshal([]byte(geometryString))
		if err != nil {
			return nil, "", fmt.Errorf("trouble reading geometry as WKB: %w", err)
		}
		geometry = g
	case EncodingWKT:
		g, err := wkt.Unmarshal(geometryString)
		if err != nil {
			return nil, "", fmt.Errorf("trouble reading geometry as WKT: %w", err)
		}
		geometry = g
	default:
		return nil, "", fmt.Errorf("unsupported encoding: %s", encoding)
	}

	return geometry, strings.ToUpper(encoding), nil
}

func GetCodec(codec string) (compress.Codec, error) {
	switch codec {
	case "uncompressed":
		return &parquet.Uncompressed, nil
	case "snappy":
		return &parquet.Snappy, nil
	case "gzip":
		return &parquet.Gzip, nil
	case "brotli":
		return &parquet.Brotli, nil
	case "zstd":
		return &parquet.Zstd, nil
	case "lz4raw":
		return &parquet.Lz4Raw, nil
	default:
		return nil, fmt.Errorf("invalid compression codec %s", codec)
	}
}

type ConvertOptions struct {
	InputPrimaryColumn string
	Compression        string
}

func FromParquet(file *parquet.File, output io.Writer, convertOptions *ConvertOptions) error {
	if convertOptions == nil {
		convertOptions = &ConvertOptions{}
	}
	reader := NewRowReader(file)

	schema := file.Schema()

	codec := schema.Compression()
	if convertOptions.Compression != "" {
		candidate, codecErr := GetCodec(convertOptions.Compression)
		if codecErr != nil {
			return codecErr
		}
		codec = candidate
	}

	options := []parquet.WriterOption{
		parquet.Compression(codec),
		schema,
	}

	writerConfig, configErr := parquet.NewWriterConfig(options...)
	if configErr != nil {
		return configErr
	}

	writer := parquet.NewGenericWriter[any](output, writerConfig)

	boundsLookup := map[string]*orb.Bound{}
	geometryTypeLookup := map[string]map[string]bool{}

	inputMetadata, metadataErr := GetMetadata(file)
	if metadataErr != nil {
		primaryColumn := defaultGeometryColumn
		if convertOptions.InputPrimaryColumn != "" {
			primaryColumn = convertOptions.InputPrimaryColumn
		}
		inputMetadata = &Metadata{
			PrimaryColumn: primaryColumn,
			Columns: map[string]*GeometryColumn{
				primaryColumn: {},
			},
		}
	}
	outputMetadata := inputMetadata.Clone()

	for {
		row, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		properties := map[string]any{}
		if err := schema.Reconstruct(&properties, row); err != nil {
			return err
		}

		for name, inputColumn := range inputMetadata.Columns {
			value, ok := properties[name]
			if !ok {
				return fmt.Errorf("missing geometry column: %s", name)
			}
			geometry, encoding, err := Geometry(value, name, inputMetadata, schema)
			if err != nil {
				return err
			}

			if encoding != EncodingWKB {
				column, ok := schema.Lookup(name)
				if !ok {
					return fmt.Errorf("missing geometry column: %s", name)
				}
				geomBytes, wkbErr := wkb.Marshal(geometry)
				if wkbErr != nil {
					return fmt.Errorf("failed to encode %q geometry as wkb: %w", name, wkbErr)
				}
				row[column.ColumnIndex] = parquet.ValueOf(geomBytes)
			}

			if inputColumn.Encoding != EncodingWKB {
				outputMetadata.Columns[name].Encoding = EncodingWKB
			}

			bounds := geometry.Bound()
			if boundsLookup[name] != nil {
				bounds = bounds.Union(*boundsLookup[name])
			}
			boundsLookup[name] = &bounds

			if geometryTypeLookup[name] == nil {
				geometryTypeLookup[name] = map[string]bool{}
			}
			geometryTypeLookup[name][geometry.GeoJSONType()] = true
		}

		_, writeErr := writer.WriteRows([]parquet.Row{row})
		if writeErr != nil {
			return writeErr
		}
	}

	for name, bounds := range boundsLookup {
		if bounds != nil {
			if inputMetadata.Columns[name] == nil {
				outputMetadata.Columns[name] = getDefaultGeometryColumn()
			}
			outputMetadata.Columns[name].Bounds = []float64{
				bounds.Left(), bounds.Bottom(), bounds.Right(), bounds.Top(),
			}
		}
	}

	for name, types := range geometryTypeLookup {
		geometryTypes := []string{}
		if len(types) > 0 {
			for geometryType := range types {
				geometryTypes = append(geometryTypes, geometryType)
			}
		}
		if inputMetadata.Columns[name] == nil {
			outputMetadata.Columns[name] = getDefaultGeometryColumn()
		}
		outputMetadata.Columns[name].GeometryTypes = geometryTypes
	}

	metadataBytes, jsonErr := json.Marshal(outputMetadata)
	if jsonErr != nil {
		return fmt.Errorf("failed to serialize geo metadata: %w", jsonErr)
	}
	writer.SetKeyValueMetadata(MetadataKey, string(metadataBytes))
	return writer.Close()
}
