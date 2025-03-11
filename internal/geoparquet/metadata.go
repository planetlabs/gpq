package geoparquet

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v16/parquet/metadata"
	"github.com/planetlabs/gpq/internal/geo"
)

const (
	Version                     = "1.0.0"
	MetadataKey                 = "geo"
	EdgesPlanar                 = "planar"
	EdgesSpherical              = "spherical"
	OrientationCounterClockwise = "counterclockwise"
	DefaultGeometryColumn       = "geometry"
	DefaultGeometryEncoding     = geo.EncodingWKB
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

type ProjId struct {
	Authority string `json:"authority"`
	Code      any    `json:"code"`
}

type Proj struct {
	Name string  `json:"name"`
	Id   *ProjId `json:"id"`
}

func (p *Proj) String() string {
	id := ""
	if p.Id != nil {
		if code, ok := p.Id.Code.(string); ok {
			id = p.Id.Authority + ":" + code
		} else if code, ok := p.Id.Code.(float64); ok {
			id = fmt.Sprintf("%s:%g", p.Id.Authority, code)
		}
	}
	if p.Name != "" {
		return p.Name
	}
	if id == "" {
		return "Unknown"
	}
	return id
}

type coveringBbox struct {
	Xmin []string
	Ymin []string
	Xmax []string
	Ymax []string
}

type Covering struct {
	Bbox coveringBbox
}

type GeometryColumn struct {
	Encoding      string    `json:"encoding"`
	GeometryType  any       `json:"geometry_type,omitempty"`
	GeometryTypes any       `json:"geometry_types"`
	CRS           *Proj     `json:"crs,omitempty"`
	Edges         string    `json:"edges,omitempty"`
	Orientation   string    `json:"orientation,omitempty"`
	Bounds        []float64 `json:"bbox,omitempty"`
	Epoch         float64   `json:"epoch,omitempty"`
	Covering      *Covering `json:"covering,omitempty"`
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
		Encoding:      DefaultGeometryEncoding,
		GeometryTypes: []string{},
	}
}

func DefaultMetadata() *Metadata {
	return &Metadata{
		Version:       Version,
		PrimaryColumn: DefaultGeometryColumn,
		Columns: map[string]*GeometryColumn{
			DefaultGeometryColumn: getDefaultGeometryColumn(),
		},
	}
}

var ErrNoMetadata = fmt.Errorf("missing %s metadata key", MetadataKey)
var ErrDuplicateMetadata = fmt.Errorf("found more than one %s metadata key", MetadataKey)

func GetMetadata(keyValueMetadata metadata.KeyValueMetadata) (*Metadata, error) {
	value, err := GetMetadataValue(keyValueMetadata)
	if err != nil {
		return nil, err
	}
	geoFileMetadata := &Metadata{}
	jsonErr := json.Unmarshal([]byte(value), geoFileMetadata)
	if jsonErr != nil {
		return nil, fmt.Errorf("unable to parse %s metadata: %w", MetadataKey, jsonErr)
	}
	return geoFileMetadata, nil
}

func GetMetadataValue(keyValueMetadata metadata.KeyValueMetadata) (string, error) {
	var value *string
	for _, kv := range keyValueMetadata {
		if kv.Key == MetadataKey {
			if value != nil {
				return "", ErrDuplicateMetadata
			}
			value = kv.Value
		}
	}
	if value == nil {
		return "", ErrNoMetadata
	}
	return *value, nil
}
