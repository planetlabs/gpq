package geoparquet

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/planetlabs/gpq/internal/geo"
)

type FeatureWriter struct {
	geoMetadata        *Metadata
	maxRowGroupLength  int64
	bufferedLength     int64
	fileWriter         *pqarrow.FileWriter
	recordBuilder      *array.RecordBuilder
	geometryTypeLookup map[string]map[string]bool
	boundsLookup       map[string]*orb.Bound
}

func NewFeatureWriter(config *WriterConfig) (*FeatureWriter, error) {
	parquetProps := config.ParquetWriterProps
	if parquetProps == nil {
		parquetProps = parquet.NewWriterProperties()
	}

	arrowProps := config.ArrowWriterProps
	if arrowProps == nil {
		defaults := pqarrow.DefaultWriterProps()
		arrowProps = &defaults
	}

	geoMetadata := config.Metadata
	if geoMetadata == nil {
		geoMetadata = DefaultMetadata()
	}

	if config.ArrowSchema == nil {
		return nil, errors.New("schema is required")
	}

	if config.Writer == nil {
		return nil, errors.New("writer is required")
	}
	fileWriter, fileErr := pqarrow.NewFileWriter(config.ArrowSchema, config.Writer, parquetProps, *arrowProps)
	if fileErr != nil {
		return nil, fileErr
	}

	writer := &FeatureWriter{
		geoMetadata:        geoMetadata,
		fileWriter:         fileWriter,
		maxRowGroupLength:  parquetProps.MaxRowGroupLength(),
		bufferedLength:     0,
		recordBuilder:      array.NewRecordBuilder(parquetProps.Allocator(), config.ArrowSchema),
		geometryTypeLookup: map[string]map[string]bool{},
		boundsLookup:       map[string]*orb.Bound{},
	}

	return writer, nil
}

func (w *FeatureWriter) Write(feature *geo.Feature) error {
	arrowSchema := w.recordBuilder.Schema()
	numFields := arrowSchema.NumFields()
	for i := 0; i < numFields; i++ {
		field := arrowSchema.Field(i)
		builder := w.recordBuilder.Field(i)
		if err := w.append(feature, field, builder); err != nil {
			return err
		}
	}
	w.bufferedLength += 1
	if w.bufferedLength >= w.maxRowGroupLength {
		return w.writeBuffered()
	}
	return nil
}

func (w *FeatureWriter) writeBuffered() error {
	record := w.recordBuilder.NewRecord()
	defer record.Release()
	if err := w.fileWriter.WriteBuffered(record); err != nil {
		return err
	}
	w.bufferedLength = 0
	return nil
}

func (w *FeatureWriter) append(feature *geo.Feature, field arrow.Field, builder array.Builder) error {
	name := field.Name
	if w.geoMetadata.Columns[name] != nil {
		return w.appendGeometry(feature, field, builder)
	}

	value, ok := feature.Properties[name]
	if !ok || value == nil {
		if !field.Nullable {
			return fmt.Errorf("field %q is required, but the property is missing in the feature", name)
		}
		builder.AppendNull()
		return nil
	}

	return w.appendValue(name, value, builder)
}

func (w *FeatureWriter) appendValue(name string, value any, builder array.Builder) error {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		v, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected %q to be a boolean, got %v", name, value)
		}
		b.Append(v)
	case *array.StringBuilder:
		v, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected %q to be a string, got %v", name, value)
		}
		b.Append(v)
	case *array.Float64Builder:
		v, ok := value.(float64)
		if !ok {
			return fmt.Errorf("expected %q to be a float64, got %v", name, value)
		}
		b.Append(v)
	case *array.ListBuilder:
		b.Append(true)
		valueBuilder := b.ValueBuilder()
		switch vb := valueBuilder.(type) {
		case *array.BooleanBuilder:
			v, ok := toUniformSlice[bool](value)
			if !ok {
				return fmt.Errorf("expected %q to be []bool, got %v", name, value)
			}
			vb.AppendValues(v, nil)
		case *array.StringBuilder:
			v, ok := toUniformSlice[string](value)
			if !ok {
				return fmt.Errorf("expected %q to be []string, got %v", name, value)
			}
			vb.AppendValues(v, nil)
		case *array.Float64Builder:
			v, ok := toUniformSlice[float64](value)
			if !ok {
				return fmt.Errorf("expected %q to be []float64, got %v", name, value)
			}
			vb.AppendValues(v, nil)
		case *array.StructBuilder:
			v, ok := value.([]any)
			if !ok {
				return fmt.Errorf("expected %q to be []any, got %v", name, value)
			}
			for _, item := range v {
				if err := w.appendValue(name, item, vb); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unsupported list element builder type %#v", vb)
		}
	case *array.StructBuilder:
		v, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("expected %q to be map[string]any, got %v", name, value)
		}
		t, ok := b.Type().(*arrow.StructType)
		if !ok {
			return fmt.Errorf("expected builder for %q to have a struct type, got %v", name, b.Type())
		}
		b.Append(true)
		for i := 0; i < b.NumField(); i += 1 {
			field := t.Field(i)
			name := field.Name
			fieldValue, ok := v[name]
			fieldBuilder := b.FieldBuilder(i)
			if !ok || fieldValue == nil {
				if !field.Nullable {
					return fmt.Errorf("field %q is required, but the property is missing", name)
				}
				fieldBuilder.AppendNull()
				continue
			}
			if err := w.appendValue(name, fieldValue, fieldBuilder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported builder type %#v", b)
	}

	return nil
}

func toUniformSlice[T any](value any) ([]T, bool) {
	if values, ok := value.([]T); ok {
		return values, true
	}
	slice, ok := value.([]any)
	if !ok {
		return nil, false
	}
	values := make([]T, len(slice))
	for i, v := range slice {
		t, ok := v.(T)
		if !ok {
			return nil, false
		}
		values[i] = t
	}
	return values, true
}

func (w *FeatureWriter) appendGeometry(feature *geo.Feature, field arrow.Field, builder array.Builder) error {
	name := field.Name
	geomColumn := w.geoMetadata.Columns[name]

	binaryBuilder, ok := builder.(*array.BinaryBuilder)
	if !ok {
		return fmt.Errorf("expected column %q to have a binary type, got %s", name, builder.Type().Name())
	}
	var geometry orb.Geometry
	if name == w.geoMetadata.PrimaryColumn {
		geometry = feature.Geometry
	} else {
		if value, ok := feature.Properties[name]; ok {
			g, ok := value.(orb.Geometry)
			if !ok {
				return fmt.Errorf("expected %q to be a geometry, got %v", name, value)
			}
			geometry = g
		}
	}
	if geometry == nil {
		if !field.Nullable {
			return fmt.Errorf("feature missing required %q geometry", name)
		}
		binaryBuilder.AppendNull()
		return nil
	}

	if w.geometryTypeLookup[name] == nil {
		w.geometryTypeLookup[name] = map[string]bool{}
	}
	w.geometryTypeLookup[name][geometry.GeoJSONType()] = true

	bounds := geometry.Bound()
	if w.boundsLookup[name] != nil {
		bounds = bounds.Union(*w.boundsLookup[name])
	}
	w.boundsLookup[name] = &bounds

	switch geomColumn.Encoding {
	case geo.EncodingWKB:
		data, err := wkb.Marshal(geometry)
		if err != nil {
			return fmt.Errorf("failed to encode %q as WKB: %w", name, err)
		}
		binaryBuilder.Append(data)
		return nil
	case geo.EncodingWKT:
		binaryBuilder.Append(wkt.Marshal(geometry))
		return nil
	default:
		return fmt.Errorf("unsupported geometry encoding: %s", geomColumn.Encoding)
	}
}

func (w *FeatureWriter) Close() error {
	defer w.recordBuilder.Release()
	if w.bufferedLength > 0 {
		if err := w.writeBuffered(); err != nil {
			return err
		}
	}

	geoMetadata := w.geoMetadata.Clone()
	for name, bounds := range w.boundsLookup {
		if bounds != nil {
			if geoMetadata.Columns[name] == nil {
				geoMetadata.Columns[name] = getDefaultGeometryColumn()
			}
			geoMetadata.Columns[name].Bounds = []float64{
				bounds.Left(), bounds.Bottom(), bounds.Right(), bounds.Top(),
			}
		}
	}
	for name, types := range w.geometryTypeLookup {
		geometryTypes := []string{}
		if len(types) > 0 {
			for geometryType := range types {
				geometryTypes = append(geometryTypes, geometryType)
			}
		}
		if geoMetadata.Columns[name] == nil {
			geoMetadata.Columns[name] = getDefaultGeometryColumn()
		}
		geoMetadata.Columns[name].GeometryTypes = geometryTypes
	}

	data, err := json.Marshal(geoMetadata)
	if err != nil {
		return fmt.Errorf("failed to encode %s file metadata", MetadataKey)
	}
	if err := w.fileWriter.AppendKeyValueMetadata(MetadataKey, string(data)); err != nil {
		return fmt.Errorf("failed to append %s file metadata", MetadataKey)
	}
	return w.fileWriter.Close()
}
