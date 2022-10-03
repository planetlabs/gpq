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

	"github.com/segmentio/parquet-go"
)

const GeoMetadataKey = "geo"

const Version = "1.0.0-beta.1"

type GeoMetadata struct {
	Version       string                     `json:"version"`
	PrimaryColumn string                     `json:"primary_column"`
	Columns       map[string]*GeometryColumn `json:"columns"`
}

type GeometryColumn struct {
	Encoding      string    `json:"encoding"`
	GeometryType  any       `json:"geometry_type,omitempty"`
	GeometryTypes any       `json:"geometry_types"`
	CRS           any       `json:"crs,omitempty"`
	Edges         string    `json:"edges,omitempty"`
	Orientation   string    `json:"orientation,omitempty"`
	Bounds        []float64 `json:"bbox,omitempty"`
	Epoch         int       `json:"epoch,omitempty"`
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

func GetGeoMetadataValue(file *parquet.File) (string, error) {
	value, ok := file.Lookup(GeoMetadataKey)
	if !ok {
		return "", fmt.Errorf("missing %s metadata key", GeoMetadataKey)
	}
	return value, nil
}

func GetGeoMetadata(file *parquet.File) (*GeoMetadata, error) {
	value, valueErr := GetGeoMetadataValue(file)
	if valueErr != nil {
		return nil, valueErr
	}
	geoFileMetadata := &GeoMetadata{}
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
