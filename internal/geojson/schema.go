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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/segmentio/parquet-go"
)

type ConvertFn func(any) (any, error)

var identity ConvertFn = func(v any) (any, error) {
	return v, nil
}

type TypeConverter struct {
	Type    reflect.Type
	Convert ConvertFn
}

func typeConverterFromAny(v any) (*TypeConverter, error) {
	if v == nil {
		return nil, errors.New("cannot determine type from null")
	}

	switch value := v.(type) {
	case bool, float64, string:
		converter := &TypeConverter{
			Type:    reflect.TypeOf(value),
			Convert: identity,
		}
		return converter, nil
	case map[string]any:
		return typeConverterFromMap(value)
	case []any:
		return typeConverterFromSlice(value)
	default:
		return nil, fmt.Errorf("unsupported type: %t", value)
	}
}

func typeConverterFromSlice(data []any) (*TypeConverter, error) {
	if len(data) == 0 {
		return nil, errors.New("cannot determine type from empty array")
	}

	itemConverter, err := typeConverterFromAny(data[0])
	if err != nil {
		return nil, err
	}

	itemType := itemConverter.Type

	for _, v := range data[1:] {
		_, err := itemConverter.Convert(v)
		if err != nil {
			return nil, fmt.Errorf("unsupported array of mixed type: %w", err)
		}
	}

	converter := &TypeConverter{
		Type: reflect.SliceOf(itemType),
		Convert: func(v any) (any, error) {
			data, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf("expected []any, got %t", v)
			}
			slice := reflect.MakeSlice(reflect.SliceOf(itemType), len(data), len(data))
			for i, d := range data {
				value, err := itemConverter.Convert(d)
				if err != nil {
					return nil, err
				}
				itemValue := reflect.ValueOf(value)
				if itemValue.Type() != itemType {
					return nil, fmt.Errorf("mixed array, expected %s, but got %s", itemType, itemValue.Type())
				}
				slice.Index(i).Set(itemValue)
			}
			return slice.Interface(), nil
		},
	}

	return converter, nil
}

type FieldConverter struct {
	Field   reflect.StructField
	Convert ConvertFn
}

func typeConverterFromMap(data map[string]any) (*TypeConverter, error) {
	fieldConverters, err := fieldConvertersFromMap(data)
	if err != nil {
		return nil, err
	}
	return structConverter(fieldConverters)
}

func structConverter(fieldConverters map[string]*FieldConverter) (*TypeConverter, error) {
	convertLookup := map[string]ConvertFn{}
	nameLookup := map[string]string{}

	fields := []reflect.StructField{}
	for key, fieldConverter := range fieldConverters {
		fields = append(fields, fieldConverter.Field)
		convertLookup[key] = fieldConverter.Convert
		nameLookup[key] = fieldConverter.Field.Name
	}

	structType := reflect.StructOf(fields)

	converter := &TypeConverter{
		Type: structType,
		Convert: func(d any) (any, error) {
			data, ok := d.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected map[string]any, got %t", d)
			}

			structValue := reflect.New(structType).Elem()
			for k, v := range data {
				convert, ok := convertLookup[k]
				if !ok {
					return nil, fmt.Errorf("unexpected property name %q", k)
				}
				name, ok := nameLookup[k]
				if !ok {
					return nil, fmt.Errorf("unexpected property name %q", k)
				}
				if v == nil {
					continue
				}

				value, err := convert(v)
				if err != nil {
					return nil, fmt.Errorf("unable to convert value %v for %q: %w", v, k, err)
				}

				fieldValue := structValue.FieldByName(name)
				if fieldValue.Type() != reflect.TypeOf(value) {
					return nil, fmt.Errorf("mixed types for %q, expected %s, but got %s", k, fieldValue.Type(), reflect.TypeOf(value))
				}
				fieldValue.Set(reflect.ValueOf(value))
			}
			return structValue.Interface(), nil
		},
	}
	return converter, nil
}

func fieldName(key string, offset int) string {
	letters := []rune("GPQ_")
	for _, r := range strings.ToUpper(key) {
		if !(unicode.IsLetter(r) || unicode.IsNumber(r)) {
			r = '_'
		}
		letters = append(letters, r)
	}
	return fmt.Sprintf("%s_%d", string(letters), offset)
}

func fieldConverterFromAny(key string, offset int, value any) (*FieldConverter, error) {
	typeConverter, err := typeConverterFromAny(value)
	if err != nil {
		return nil, err
	}

	repetition := "optional"
	if typeConverter.Type.Kind() == reflect.Slice {
		repetition = ""
	}

	fieldConverter := &FieldConverter{
		Field: reflect.StructField{
			Name: fieldName(key, offset),
			Type: typeConverter.Type,
			Tag:  makeStructTag("parquet", key, repetition),
		},
		Convert: typeConverter.Convert,
	}

	return fieldConverter, nil
}

func fieldConvertersFromMap(data map[string]any) (map[string]*FieldConverter, error) {
	fieldConverters := map[string]*FieldConverter{}
	for key, v := range data {
		fieldConverter, err := fieldConverterFromAny(key, len(fieldConverters), v)
		if err != nil {
			return nil, err
		}

		fieldConverters[key] = fieldConverter
	}
	return fieldConverters, nil
}

func makeStructTag(name string, values ...string) reflect.StructTag {
	nonEmptyValues := []string{}
	for _, value := range values {
		if value == "" {
			continue
		}
		nonEmptyValues = append(nonEmptyValues, value)
	}
	return reflect.StructTag(fmt.Sprintf("%s:%q", name, strings.Join(nonEmptyValues, ",")))
}

type SchemaBuilder struct {
	fieldConverters map[string]*FieldConverter
	lastError       error
}

func (sb *SchemaBuilder) Error() error {
	return sb.lastError
}

func (sb *SchemaBuilder) isComplete() bool {
	if sb.fieldConverters == nil {
		return false
	}
	for _, v := range sb.fieldConverters {
		if v == nil {
			return false
		}
	}
	return true
}

func (sb *SchemaBuilder) Add(feature *Feature) bool {
	if sb.fieldConverters == nil {
		sb.fieldConverters = map[string]*FieldConverter{}
	}

	fieldConverters := sb.fieldConverters
	for key, value := range feature.Properties {
		if value == nil {
			if _, ok := fieldConverters[key]; !ok {
				if sb.lastError == nil {
					sb.lastError = fmt.Errorf("null value for %q", key)
				}
				fieldConverters[key] = nil
			}
			continue
		}

		if fieldConverters[key] != nil {
			continue
		}

		fieldConverter, err := fieldConverterFromAny(key, len(fieldConverters), value)
		if err != nil {
			sb.lastError = err
			fieldConverters[key] = nil
			continue
		}

		fieldConverters[key] = fieldConverter
	}

	if fieldConverters[primaryColumn] != nil {
		return sb.isComplete()
	}

	geometryData, wkbErr := wkb.Marshal(feature.Geometry)
	if wkbErr != nil {
		fieldConverters[primaryColumn] = nil
		sb.lastError = fmt.Errorf("failed to encode geometry: %w", wkbErr)
		return false
	}

	fieldConverters[primaryColumn] = &FieldConverter{
		Field: reflect.StructField{
			Name: fieldName(primaryColumn, len(fieldConverters)),
			Type: reflect.TypeOf(geometryData),
			Tag:  makeStructTag("parquet", primaryColumn, "optional"),
		},
		Convert: func(v any) (any, error) {
			geometry, ok := v.(orb.Geometry)
			if !ok {
				return nil, fmt.Errorf("expected geometry, got %t", v)
			}
			return wkb.Marshal(geometry)
		},
	}

	return sb.isComplete()
}

func (sb *SchemaBuilder) Converter() (*TypeConverter, error) {
	if !sb.isComplete() {
		if err := sb.Error(); err != nil {
			return nil, err
		}

		return nil, errors.New("not enough features have been added to build a schema")
	}

	converter, converterErr := structConverter(sb.fieldConverters)
	if converterErr != nil {
		return nil, converterErr
	}

	featureConverter := &TypeConverter{
		Type: converter.Type,
		Convert: func(f any) (any, error) {
			feature, ok := f.(*Feature)
			if !ok {
				return nil, fmt.Errorf("expected feature, got %t", f)
			}
			data := map[string]any{}
			for k, v := range feature.Properties {
				data[k] = v
			}
			data[primaryColumn] = feature.Geometry
			return converter.Convert(data)
		},
	}
	return featureConverter, nil
}

func (sb *SchemaBuilder) Schema() (*parquet.Schema, error) {
	converter, err := sb.Converter()
	if err != nil {
		return nil, err
	}

	schema := parquet.SchemaOf(reflect.New(converter.Type).Elem().Interface())
	return schema, nil
}

func SchemaOf(feature *Feature) (*parquet.Schema, error) {
	schemaBuilder := &SchemaBuilder{}
	schemaBuilder.Add(feature)
	return schemaBuilder.Schema()
}
