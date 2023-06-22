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
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/compress"
	"github.com/segmentio/parquet-go/encoding"
)

type rootNode struct {
	fields []parquet.Field
}

var _ parquet.Node = (*rootNode)(nil)

func (r *rootNode) Optional() bool { return false }

func (r *rootNode) Repeated() bool { return false }

func (r *rootNode) Required() bool { return true }

func (r *rootNode) Leaf() bool { return false }

func (r *rootNode) Encoding() encoding.Encoding { return nil }

func (r *rootNode) Compression() compress.Codec { return nil }

func (r *rootNode) String() string {
	s := new(strings.Builder)
	_ = parquet.PrintSchema(s, "", r)
	return s.String()
}

func (r *rootNode) Type() parquet.Type {
	g := parquet.Group{}
	return g.Type()
}

func (r *rootNode) GoType() reflect.Type {
	fields := r.fields
	structFields := make([]reflect.StructField, len(fields))
	for i, field := range fields {
		name := field.Name()
		firstRune, size := utf8.DecodeRuneInString(name)
		structFields[i].Name = string([]rune{unicode.ToUpper(firstRune)}) + name[size:]
		structFields[i].Type = field.GoType()
	}
	return reflect.StructOf(structFields)
}

func (r *rootNode) Fields() []parquet.Field {
	return r.fields
}

type groupField struct {
	parquet.Node
	name string
}

var _ parquet.Field = (*groupField)(nil)

func (f *groupField) Name() string {
	return f.name
}

func (f *groupField) Value(base reflect.Value) reflect.Value {
	return base.MapIndex(reflect.ValueOf(&f.name).Elem())
}

type fieldTransform func(parquet.Field) (parquet.Field, error)

func TransformSchema(schema *parquet.Schema, transform fieldTransform) (*parquet.Schema, error) {
	inputFields := schema.Fields()
	outputFields := make([]parquet.Field, len(inputFields))
	for i, inputField := range inputFields {
		outputField, err := transform(inputField)
		if err != nil {
			return nil, fmt.Errorf("trouble transforming field %s: %w", inputField.Name(), err)
		}
		outputFields[i] = outputField
	}

	root := &rootNode{fields: outputFields}

	return parquet.NewSchema(schema.Name(), root), nil
}

func stringToBinaryFieldTransform(metadata *Metadata) fieldTransform {
	return func(inputField parquet.Field) (parquet.Field, error) {
		name := inputField.Name()
		if _, ok := metadata.Columns[name]; !ok {
			return inputField, nil
		}

		if inputField.Type() != stringType {
			return inputField, nil
		}

		node := parquet.Leaf(parquet.ByteArrayType)
		if inputField.Optional() {
			node = parquet.Optional(node)
		}
		if inputField.Repeated() {
			node = parquet.Repeated(node)
		}
		if inputField.Compression() != nil {
			node = parquet.Compressed(node, inputField.Compression())
		}

		return &groupField{Node: node, name: name}, nil
	}
}
