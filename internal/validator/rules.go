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

package validator

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/paulmach/orb"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/segmentio/parquet-go"
)

type MetadataMap map[string]any

type ColumnMetdataMap map[string]map[string]any

type FileInfo struct {
	File     *parquet.File
	Metadata *geoparquet.Metadata
}

type RuleData interface {
	*parquet.File | MetadataMap | ColumnMetdataMap | *FileInfo
}

type EncodedGeometryMap map[string]any
type DecodedGeometryMap map[string]orb.Geometry

type RowData interface {
	EncodedGeometryMap | DecodedGeometryMap
}

type Rule interface {
	Title() string
	Validate() error
}

type errFatal string

var ErrFatal = errFatal("fatal error")

func (e errFatal) Error() string {
	return string(e)
}

func (e errFatal) Is(target error) bool {
	_, ok := target.(errFatal)
	return ok
}

func fatal(format string, a ...any) errFatal {
	return errFatal(fmt.Sprintf(format, a...))
}

type GenericRule[T RuleData] struct {
	title    string
	value    T
	validate func(T) error
}

var _ Rule = (*GenericRule[*parquet.File])(nil)

func (r *GenericRule[T]) Title() string {
	return r.title
}

func (r *GenericRule[T]) Init(value T) {
	r.value = value
}

func (r *GenericRule[T]) Validate() error {
	return r.validate(r.value)
}

type RowRule[T RowData] struct {
	title string
	row   func(*FileInfo, T) error
	info  *FileInfo
	err   error
}

var _ Rule = (*RowRule[EncodedGeometryMap])(nil)

func (r *RowRule[T]) Title() string {
	return r.title
}

func (r *RowRule[T]) Init(info *FileInfo) {
	r.info = info
}

func (r *RowRule[T]) Row(data T) error {
	if r.err == nil {
		r.err = r.row(r.info, data)
	}
	return r.err
}

func (r *RowRule[T]) Validate() error {
	return r.err
}

func asJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("<unable to encode as JSON: %s>", err)
	}
	return string(data)
}

func RequiredGeoKey() Rule {
	return &GenericRule[*parquet.File]{
		title: fmt.Sprintf("file must include a %q metadata key", geoparquet.MetadataKey),
		validate: func(file *parquet.File) error {
			_, ok := file.Lookup(geoparquet.MetadataKey)
			if !ok {
				return fatal("missing %q metadata key", geoparquet.MetadataKey)
			}
			return nil
		},
	}
}

func RequiredMetadataType() Rule {
	return &GenericRule[*parquet.File]{
		title: "metadata must be a JSON object",
		validate: func(file *parquet.File) error {
			value, geoErr := geoparquet.GetMetadataValue(file)
			if geoErr != nil {
				return fatal(geoErr.Error())
			}

			metadataMap := map[string]any{}
			jsonErr := json.Unmarshal([]byte(value), &metadataMap)
			if jsonErr != nil {
				return fatal("failed to parse file metadata as a JSON object")
			}
			return nil
		},
	}
}

func RequiredVersion() Rule {
	return &GenericRule[MetadataMap]{
		title: `metadata must include a "version" string`,
		validate: func(metadata MetadataMap) error {
			value, ok := metadata["version"]
			if !ok {
				return errors.New(`missing "version" in metadata`)
			}
			version, ok := value.(string)
			if !ok {
				return fmt.Errorf(`expected "version" to be a string, got %s`, asJSON(value))
			}
			if version == "" {
				return errors.New(`expected "version" to be a non-empty string`)
			}
			return nil
		},
	}
}

func RequiredPrimaryColumn() Rule {
	return &GenericRule[MetadataMap]{
		title: `metadata must include a "primary_column" string`,
		validate: func(metadata MetadataMap) error {
			name, ok := metadata["primary_column"]
			if !ok {
				return errors.New(`missing "primary_column" in metadata`)
			}
			_, ok = name.(string)
			if !ok {
				return fmt.Errorf(`expected "primary_column" to be a string, got %s`, asJSON(name))
			}
			return nil
		},
	}
}

func RequiredColumns() Rule {
	return &GenericRule[MetadataMap]{
		title: `metadata must include a "columns" object`,
		validate: func(metadata MetadataMap) error {
			columnsAny, ok := metadata["columns"]
			if !ok {
				return fatal(`missing "columns" in metadata`)
			}
			columnsMap, ok := columnsAny.(map[string]any)
			if !ok {
				return fatal(`expected "columns" to be an object, got %s`, asJSON(columnsAny))
			}
			for name, meta := range columnsMap {
				_, ok := meta.(map[string]any)
				if !ok {
					return fatal(`expected column %q to be an object, got %s`, name, asJSON(meta))
				}
			}
			return nil
		},
	}
}

func RequiredColumnEncoding() Rule {
	return &GenericRule[ColumnMetdataMap]{
		title: `column metadata must include a valid "encoding" string`,
		validate: func(columnMetadata ColumnMetdataMap) error {
			for name, meta := range columnMetadata {
				_, ok := meta["encoding"]
				if !ok {
					return fmt.Errorf(`missing "encoding" for column %q`, name)
				}
				encoding, ok := meta["encoding"].(string)
				if !ok {
					return fmt.Errorf(`expected "encoding" for column %q to be a string, got %s`, name, asJSON(meta["encoding"]))
				}
				if encoding != geoparquet.EncodingWKB {
					return fmt.Errorf(`unsupported encoding %q for column %q`, encoding, name)
				}
			}
			return nil
		},
	}
}

func isValidGeometryType(geometryType string) bool {
	for _, validGeometryType := range geoparquet.GeometryTypes {
		if geometryType == validGeometryType {
			return true
		}
	}
	return false
}

func RequiredGeometryTypes() Rule {
	return &GenericRule[ColumnMetdataMap]{
		title: `column metadata must include a "geometry_types" list`,
		validate: func(columnMetadata ColumnMetdataMap) error {
			for name, meta := range columnMetadata {
				_, ok := meta["geometry_types"]
				if !ok {
					return fmt.Errorf(`missing "geometry_types" for column %q`, name)
				}
				geometryTypes, ok := meta["geometry_types"].([]any)
				if !ok {
					return fmt.Errorf(`expected "geometry_types" for column %q to be a list, got %s`, name, asJSON(meta["geometry_types"]))
				}
				for _, value := range geometryTypes {
					geometryType, ok := value.(string)
					if !ok {
						return fmt.Errorf(`expected "geometry_types" for column %q to be a list of strings, got %s`, name, asJSON(geometryTypes))
					}
					if !isValidGeometryType(geometryType) {
						return fmt.Errorf(`unsupported geometry type %q for column %q`, geometryType, name)
					}
				}
			}
			return nil
		},
	}
}

func projJSONSchemaUrl(version string) string {
	return fmt.Sprintf("https://proj.org/schemas/v%s/projjson.schema.json", version)
}

func simplifiedValidationMessage(err *jsonschema.ValidationError) string {
	leaf := err
	for len(leaf.Causes) > 0 {
		leaf = leaf.Causes[0]
	}
	location := leaf.InstanceLocation
	if location == "" {
		location = "input"
	}
	return fmt.Sprintf("%s is invalid: %s", location, leaf.Message)
}

func OptionalCRS() Rule {
	return &GenericRule[ColumnMetdataMap]{
		title: `optional "crs" must be null or a PROJJSON object`,
		validate: func(columnMetadata ColumnMetdataMap) error {
			for name, meta := range columnMetadata {
				if meta["crs"] == nil {
					return nil
				}
				crs, ok := meta["crs"].(map[string]any)
				if !ok {
					return fmt.Errorf(`expected "crs" for column %q to be an object, got %s`, name, asJSON(meta["crs"]))
				}
				schemaUrl, ok := crs["$schema"].(string)
				if !ok {
					schemaUrl = projJSONSchemaUrl("0.6")
				}
				compiler := jsonschema.NewCompiler()
				schema, schemaErr := compiler.Compile(schemaUrl)
				if schemaErr != nil {
					return fmt.Errorf("failed to compile PROJJSON schema: %w", schemaErr)
				}
				err := schema.Validate(crs)
				if err == nil {
					continue
				}
				validationErr, ok := err.(*jsonschema.ValidationError)
				if !ok {
					return err
				}
				return fmt.Errorf("validation failed against %s: %s", schemaUrl, simplifiedValidationMessage(validationErr))
			}
			return nil
		},
	}
}

func OptionalOrientation() Rule {
	return &GenericRule[ColumnMetdataMap]{
		title: `optional "orientation" must be a valid string`,
		validate: func(columnMetadata ColumnMetdataMap) error {
			for name, meta := range columnMetadata {
				_, ok := meta["orientation"]
				if !ok {
					return nil
				}
				orientation, ok := meta["orientation"].(string)
				if !ok {
					return fmt.Errorf(`expected "orientation" for column %q to be a string, got %s`, name, asJSON(meta["orientation"]))
				}
				if orientation != geoparquet.OrientationCounterClockwise {
					return fmt.Errorf(`unsupported orientation %q for column %q, expected %q`, orientation, name, geoparquet.OrientationCounterClockwise)
				}
			}
			return nil
		},
	}
}

func OptionalEdges() Rule {
	return &GenericRule[ColumnMetdataMap]{
		title: `optional "edges" must be a valid string`,
		validate: func(columnMetadata ColumnMetdataMap) error {
			for name, meta := range columnMetadata {
				_, ok := meta["edges"]
				if !ok {
					return nil
				}
				edges, ok := meta["edges"].(string)
				if !ok {
					return fmt.Errorf(`expected "edges" for column %q to be a string, got %s`, name, asJSON(meta["edges"]))
				}
				if edges != geoparquet.EdgesPlanar && edges != geoparquet.EdgesSpherical {
					return fmt.Errorf(`unsupported edges %q for column %q, expected %q or %q`, edges, name, geoparquet.EdgesPlanar, geoparquet.EdgesSpherical)
				}
			}
			return nil
		},
	}
}

func OptionalBbox() Rule {
	return &GenericRule[ColumnMetdataMap]{
		title: `optional "bbox" must be an array of 4 or 6 numbers`,
		validate: func(columnMetadata ColumnMetdataMap) error {
			for name, meta := range columnMetadata {
				_, ok := meta["bbox"]
				if !ok {
					return nil
				}
				bbox, ok := meta["bbox"].([]any)
				if !ok {
					return fmt.Errorf(`expected "bbox" for column %q to be a list, got %s`, name, asJSON(meta["bbox"]))
				}
				if len(bbox) != 4 && len(bbox) != 6 {
					return fmt.Errorf(`expected "bbox" for column %q to be a list of 4 or 6 numbers, got %s`, name, asJSON(bbox))
				}
				for _, value := range bbox {
					_, ok := value.(float64)
					if !ok {
						return fatal(`expected "bbox" for column %q to be a list of numbers, got %s`, name, asJSON(bbox))
					}
				}
			}
			return nil
		},
	}
}

func OptionalEpoch() Rule {
	return &GenericRule[ColumnMetdataMap]{
		title: `optional "epoch" must be a number`,
		validate: func(columnMetadata ColumnMetdataMap) error {
			for name, meta := range columnMetadata {
				_, ok := meta["epoch"]
				if !ok {
					return nil
				}
				_, ok = meta["epoch"].(float64)
				if !ok {
					return fatal(`expected "epoch" for column %q to be a number, got %s`, name, asJSON(meta["epoch"]))
				}
			}
			return nil
		},
	}
}

func PrimaryColumnInLookup() Rule {
	return &GenericRule[*FileInfo]{
		title: `column metadata must include the "primary_column" name`,
		validate: func(info *FileInfo) error {
			name := info.Metadata.PrimaryColumn
			_, ok := info.Metadata.Columns[name]
			if !ok {
				return fmt.Errorf("the %q column is not included in the column metadata", name)
			}
			return nil
		},
	}
}

func GeometryDataType() Rule {
	return &GenericRule[*FileInfo]{
		title: "geometry columns must be stored using the BYTE_ARRAY parquet type",
		validate: func(info *FileInfo) error {
			metadata := info.Metadata
			schema := info.File.Schema()
			for name := range metadata.Columns {
				column, ok := schema.Lookup(name)
				if !ok {
					return fatal("missing geometry column %q", name)
				}
				if column.Node.Type() != parquet.ByteArrayType {
					return fatal("unexpected type for column %q, got %s", name, column.Node.Type())
				}
			}

			return nil
		},
	}
}

func GeometryUngrouped() Rule {
	return &GenericRule[*FileInfo]{
		title: "geometry columns must not be grouped",
		validate: func(info *FileInfo) error {
			metadata := info.Metadata
			schema := info.File.Schema()
			for name := range metadata.Columns {
				column, ok := schema.Lookup(name)
				if !ok {
					return fatal("missing geometry column %q", name)
				}
				if !column.Node.Leaf() {
					return fmt.Errorf("column %q must not be a group", name)
				}
			}

			return nil
		},
	}
}

func GeometryRepetition() Rule {
	return &GenericRule[*FileInfo]{
		title: "geometry columns must be required or optional, not repeated",
		validate: func(info *FileInfo) error {
			metadata := info.Metadata
			schema := info.File.Schema()
			for name := range metadata.Columns {
				column, ok := schema.Lookup(name)
				if !ok {
					return fatal("missing geometry column %q", name)
				}
				if column.Node.Repeated() {
					return fmt.Errorf("column %q must not be repeated", name)
				}
				if !column.Node.Required() && !column.Node.Optional() {
					return fmt.Errorf("column %q must be required or optional", name)
				}
			}

			return nil
		},
	}
}

func GeometryEncoding() Rule {
	return &RowRule[EncodedGeometryMap]{
		title: `all geometry values match the "encoding" metadata`,
		row: func(info *FileInfo, geometries EncodedGeometryMap) error {
			schema := info.File.Schema()
			metadata := info.Metadata

			for name, encoded := range geometries {
				_, err := geoparquet.Geometry(encoded, name, metadata, schema)
				if err != nil {
					return fatal("invalid geometry in column %q: %s", name, err)
				}
			}

			return nil
		},
	}
}

func GeometryTypes() Rule {
	return &RowRule[DecodedGeometryMap]{
		title: `all geometry types must be included in the "geometry_types" metadata (if not empty)`,
		row: func(info *FileInfo, geometries DecodedGeometryMap) error {
			metadata := info.Metadata

			for name, geometry := range geometries {
				meta, ok := metadata.Columns[name]
				if !ok {
					return fatal("missing metadata for column %q", name)
				}
				geometryTypes := meta.GetGeometryTypes()
				if len(geometryTypes) == 0 {
					continue
				}
				actualType := geometry.GeoJSONType()
				included := false
				for _, expectedType := range geometryTypes {
					if actualType == expectedType || actualType+" Z" == expectedType {
						included = true
						break
					}
				}
				if !included {
					return fmt.Errorf("unexpected geometry type %q for column %q", actualType, name)
				}
			}

			return nil
		},
	}
}

func GeometryOrientation() Rule {
	return &RowRule[DecodedGeometryMap]{
		title: `all polygon geometries must follow the "orientation" metadata (if present)`,
		row: func(info *FileInfo, geometries DecodedGeometryMap) error {
			metadata := info.Metadata

			for name, geometry := range geometries {
				meta, ok := metadata.Columns[name]
				if !ok {
					return fatal("missing metadata for column %q", name)
				}
				if meta.Orientation == "" {
					continue
				}
				if meta.Orientation != geoparquet.OrientationCounterClockwise {
					return fmt.Errorf("unsupported orientation %q for column %q", meta.Orientation, name)
				}
				polygon, ok := geometry.(orb.Polygon)
				if !ok {
					continue
				}

				expectedExterior := orb.CCW
				expectedInterior := orb.CW

				for i, ring := range polygon {
					orientation := ring.Orientation()
					if i == 0 {
						if orientation != expectedExterior {
							return fmt.Errorf("invalid orientation for exterior ring in column %q", name)
						}
						continue
					}
					if orientation != expectedInterior {
						return fmt.Errorf("invalid orientation for interior ring in column %q", name)
					}
				}
			}

			return nil
		},
	}
}

func GeometryBounds() Rule {
	return &RowRule[DecodedGeometryMap]{
		title: `all geometries must fall within the "bbox" metadata (if present)`,
		row: func(info *FileInfo, geometries DecodedGeometryMap) error {
			metadata := info.Metadata

			for name, geometry := range geometries {
				meta, ok := metadata.Columns[name]
				if !ok {
					return fatal("missing metadata for column %q", name)
				}
				bbox := meta.Bounds
				length := len(bbox)
				if length == 0 {
					continue
				}
				var x0 float64
				var x1 float64
				var y0 float64
				var y1 float64
				if length == 4 {
					x0 = bbox[0]
					y0 = bbox[1]
					x1 = bbox[2]
					y1 = bbox[3]
				} else if length == 6 {
					x0 = bbox[0]
					y0 = bbox[1]
					x1 = bbox[3]
					y1 = bbox[4]
				} else {
					return fmt.Errorf("invalid bbox length for column %q", name)
				}

				bound := geometry.Bound()
				if x0 <= x1 {
					// bbox does not cross the antimeridian
					if bound.Min.X() < x0 {
						return fmt.Errorf("geometry in column %q extends to %f, west of the bbox", name, bound.Min.X())
					}
					if bound.Max.X() > x1 {
						return fmt.Errorf("geometry in column %q extends to %f, east of the bbox", name, bound.Max.X())
					}
				} else {
					// bbox crosses the antimeridian
					if bound.Max.X() > x1 && bound.Max.X() < x0 {
						return fmt.Errorf("geometry in column %q extends to %f, outside of the bbox", name, bound.Max.X())
					}
					if bound.Min.X() < x0 && bound.Min.X() > x1 {
						return fmt.Errorf("geometry in column %q extends to %f, outside of the bbox", name, bound.Min.X())
					}
				}
				if bound.Min.Y() < y0 {
					return fmt.Errorf("geometry in column %q extends to %f, south of the bbox", name, bound.Min.Y())
				}
				if bound.Max.Y() > y1 {
					return fmt.Errorf("geometry in column %q extends to %f, north of the bbox", name, bound.Max.Y())
				}
			}

			return nil
		},
	}
}
