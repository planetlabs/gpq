package pqutil

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/planetlabs/gpq/internal/geo"
)

type ArrowSchemaBuilder struct {
	fields map[string]*arrow.Field
}

func NewArrowSchemaBuilder() *ArrowSchemaBuilder {
	return &ArrowSchemaBuilder{
		fields: map[string]*arrow.Field{},
	}
}

func (b *ArrowSchemaBuilder) Has(name string) bool {
	_, has := b.fields[name]
	return has
}

func (b *ArrowSchemaBuilder) AddGeometry(name string, encoding string) error {
	var dataType arrow.DataType
	switch encoding {
	case geo.EncodingWKB:
		dataType = arrow.BinaryTypes.Binary
	case geo.EncodingWKT:
		dataType = arrow.BinaryTypes.String
	default:
		return fmt.Errorf("unsupported geometry encoding: %s", encoding)
	}
	b.fields[name] = &arrow.Field{Name: name, Type: dataType, Nullable: true}
	return nil
}

func (b *ArrowSchemaBuilder) AddBbox(name string) {
	bboxFields := []arrow.Field{
		{Name: "xmin", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ymin", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "xmax", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ymax", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	}
	dataType := arrow.StructOf(bboxFields...)
	b.fields[name] = &arrow.Field{Name: name, Type: dataType, Nullable: true}
}

func (b *ArrowSchemaBuilder) Add(record map[string]any) error {
	for name, value := range record {
		if b.fields[name] != nil {
			continue
		}
		if value == nil {
			b.fields[name] = nil
			continue
		}
		if values, ok := value.([]any); ok {
			if len(values) == 0 {
				b.fields[name] = nil
				continue

			}
		}
		field, err := fieldFromValue(name, value, true)
		if err != nil {
			return fmt.Errorf("error converting value for %s: %w", name, err)
		}
		b.fields[name] = field
	}
	return nil
}

func fieldFromValue(name string, value any, nullable bool) (*arrow.Field, error) {
	switch v := value.(type) {
	case bool:
		return &arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean, Nullable: nullable}, nil
	case int, int64:
		return &arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: nullable}, nil
	case int32:
		return &arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32, Nullable: nullable}, nil
	case float32:
		return &arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float32, Nullable: nullable}, nil
	case float64:
		return &arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: nullable}, nil
	case []byte:
		return &arrow.Field{Name: name, Type: arrow.BinaryTypes.Binary, Nullable: nullable}, nil
	case string:
		return &arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: nullable}, nil
	case []any:
		if len(v) == 0 {
			return nil, nil
		}
		if err := assertUniformType(v); err != nil {
			return nil, err
		}
		field, err := fieldFromValue(name, v[0], nullable)
		if err != nil {
			return nil, err
		}
		return &arrow.Field{Name: name, Type: arrow.ListOf(field.Type), Nullable: nullable}, nil
	case map[string]any:
		if len(v) == 0 {
			return nil, nil
		}
		return fieldFromMap(name, v, nullable)
	default:
		return nil, fmt.Errorf("cannot convert value: %v", v)
	}
}

func fieldFromMap(name string, value map[string]any, nullable bool) (*arrow.Field, error) {
	keys := sortedKeys(value)
	length := len(keys)
	fields := make([]arrow.Field, length)
	for i, key := range keys {
		field, err := fieldFromValue(key, value[key], nullable)
		if err != nil {
			return nil, fmt.Errorf("trouble generating schema for field %q: %w", key, err)
		}
		if field == nil {
			return nil, nil
		}
		fields[i] = *field
	}
	return &arrow.Field{Name: name, Type: arrow.StructOf(fields...), Nullable: nullable}, nil
}

func assertUniformType(values []any) error {
	length := len(values)
	if length == 0 {
		return errors.New("cannot determine type from zero length slice")
	}
	mixedTypeErr := errors.New("slices must be of all the same type")
	switch v := values[0].(type) {
	case bool:
		for i := 1; i < length; i += 1 {
			if _, ok := values[i].(bool); !ok {
				return mixedTypeErr
			}
		}
	case float64:
		for i := 1; i < length; i += 1 {
			if _, ok := values[i].(float64); !ok {
				return mixedTypeErr
			}
		}
	case string:
		for i := 1; i < length; i += 1 {
			if _, ok := values[i].(string); !ok {
				return mixedTypeErr
			}
		}
	default:
		t := reflect.TypeOf(v)
		for i := 1; i < length; i += 1 {
			if reflect.TypeOf(values[i]) != t {
				return mixedTypeErr
			}
		}
	}
	return nil
}

func (b *ArrowSchemaBuilder) Ready() bool {
	for _, field := range b.fields {
		if field == nil {
			return false
		}
	}
	return true
}

func (b *ArrowSchemaBuilder) Schema() (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(b.fields))
	for i, name := range sortedKeys(b.fields) {
		field := b.fields[name]
		if field == nil {
			return nil, fmt.Errorf("could not derive type for field: %s", name)
		}
		fields[i] = *field
	}
	return arrow.NewSchema(fields, nil), nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i += 1
	}
	sort.Strings(keys)
	return keys
}
