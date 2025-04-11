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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/paulmach/orb"
	"github.com/planetlabs/gpq/internal/geo"
	"github.com/planetlabs/gpq/internal/geoparquet"
	_ "github.com/santhosh-tekuri/jsonschema/v5/httploader"
)

type Validator struct {
	rules        []Rule
	metadataOnly bool
}

func MetadataOnlyRules() []Rule {
	return []Rule{
		RequiredGeoKey(),
		RequiredMetadataType(),
		RequiredVersion(),
		RequiredPrimaryColumn(),
		RequiredColumns(),
		PrimaryColumnInLookup(),
		RequiredColumnEncoding(),
		RequiredGeometryTypes(),
		OptionalCRS(),
		OptionalOrientation(),
		OptionalEdges(),
		OptionalBbox(),
		OptionalEpoch(),
		GeometryUngrouped(),
		GeometryDataType(),
		GeometryRepetition(),
	}
}

func DataScanningRules() []Rule {
	return []Rule{
		GeometryEncoding(),
		GeometryTypes(),
		GeometryOrientation(),
		GeometryBounds(),
	}
}

// New creates a new Validator.
func New(metadataOnly bool) *Validator {
	rules := MetadataOnlyRules()
	if !metadataOnly {
		rules = append(rules, DataScanningRules()...)
	}

	v := &Validator{
		rules:        rules,
		metadataOnly: metadataOnly,
	}

	return v
}

type Report struct {
	Checks       []*Check `json:"checks"`
	MetadataOnly bool     `json:"metadataOnly"`
}

type Check struct {
	Title   string `json:"title"`
	Run     bool   `json:"run"`
	Passed  bool   `json:"passed"`
	Message string `json:"message,omitempty"`
}

// Validate opens and validates a GeoParquet file.
func (v *Validator) Validate(ctx context.Context, input parquet.ReaderAtSeeker, name string) (*Report, error) {
	reader, readerErr := file.NewParquetReader(input)
	if readerErr != nil {
		return nil, fmt.Errorf("failed to create parquet reader from %q: %w", name, readerErr)
	}
	defer reader.Close()

	return v.Report(ctx, reader)
}

// Report generates a validation report for a GeoParquet file.
func (v *Validator) Report(ctx context.Context, file *file.Reader) (*Report, error) {
	checks := make([]*Check, len(v.rules))
	for i, rule := range v.rules {
		checks[i] = &Check{
			Title: rule.Title(),
		}
	}

	report := &Report{Checks: checks, MetadataOnly: v.metadataOnly}

	// run all file rules
	if err := run(v, checks, file); err != nil {
		return report, nil
	}

	// run all metadata rules
	metadataValue, metadataErr := geoparquet.GetMetadataValue(file.MetaData().KeyValueMetadata())
	if metadataErr != nil {
		return nil, metadataErr
	}

	metadataMap := MetadataMap{}
	if err := json.Unmarshal([]byte(metadataValue), &metadataMap); err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}

	if err := run(v, checks, metadataMap); err != nil {
		return report, nil
	}

	// run all column metadata rules
	columnMetadataMap := ColumnMetdataMap{}
	columnMetadataAny, ok := metadataMap["columns"].(map[string]any)
	if !ok {
		return nil, errors.New("columns metadata is not an object")
	}

	for k, v := range columnMetadataAny {
		col, ok := v.(map[string]any)
		if !ok {
			return nil, errors.New("column metadata is not an object")
		}
		columnMetadataMap[k] = col
	}

	if err := run(v, checks, columnMetadataMap); err != nil {
		return report, nil
	}

	// run all rules that need the file and parsed metadata
	metadata, err := geoparquet.GetMetadata(file.MetaData().KeyValueMetadata())
	if err != nil {
		return nil, err
	}

	info := &FileInfo{Metadata: metadata, File: file}
	if err := run(v, checks, info); err != nil {
		return report, nil
	}

	if v.metadataOnly {
		return report, nil
	}

	// run all the data scanning rules
	recordReader, rrErr := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		File:    file,
		Context: ctx,
	})
	if rrErr != nil {
		return nil, rrErr
	}
	defer recordReader.Close()

	encodedGeometryRules := []*ColumnValueRule[any]{}
	encodedGeometryChecks := []*Check{}
	for i, r := range v.rules {
		rule, ok := r.(*ColumnValueRule[any])
		if ok {
			rule.Init(info)
			encodedGeometryRules = append(encodedGeometryRules, rule)
			encodedGeometryChecks = append(encodedGeometryChecks, checks[i])
		}
	}

	decodedGeometryRules := []*ColumnValueRule[orb.Geometry]{}
	decodedGeometryChecks := []*Check{}
	for i, r := range v.rules {
		rule, ok := r.(*ColumnValueRule[orb.Geometry])
		if ok {
			rule.Init(info)
			decodedGeometryRules = append(decodedGeometryRules, rule)
			decodedGeometryChecks = append(decodedGeometryChecks, checks[i])
		}
	}

	for {
		record, recordErr := recordReader.Read()
		if recordErr == io.EOF {
			break
		}
		if recordErr != nil {
			return nil, fmt.Errorf("failed to read record: %w", recordErr)
		}
		defer record.Release()

		schema := record.Schema()

		arr := array.RecordToStructArray(record)
		defer arr.Release()

		for colNum := 0; colNum < arr.NumField(); colNum += 1 {
			field := schema.Field(colNum)
			geomColumn := metadata.Columns[field.Name]
			if geomColumn == nil {
				continue
			}
			values := arr.Field(colNum)
			for rowNum := 0; rowNum < arr.Len(); rowNum += 1 {
				value := values.GetOneForMarshal(rowNum)
				for i, rule := range encodedGeometryRules {
					check := encodedGeometryChecks[i]
					if err := rule.Value(field.Name, value); errors.Is(err, ErrFatal) {
						check.Message = err.Error()
						check.Run = true
						return report, nil
					}
				}

				geometry, err := geo.DecodeGeometry(value, geomColumn.Encoding)
				if err != nil {
					return nil, fmt.Errorf("failed to decode geometry for %q: %w", field.Name, err)
				}
				if geometry == nil {
					continue
				}
				for i, rule := range decodedGeometryRules {
					check := decodedGeometryChecks[i]
					if err := rule.Value(field.Name, geometry.Geometry()); errors.Is(err, ErrFatal) {
						check.Message = err.Error()
						check.Run = true
						return report, nil
					}
				}
			}
		}

	}

	for i, rule := range encodedGeometryRules {
		check := encodedGeometryChecks[i]
		check.Run = true
		if err := rule.Validate(); err != nil {
			check.Message = err.Error()
			if errors.Is(err, ErrFatal) {
				return report, nil
			}
			continue
		}
		check.Passed = true
	}

	for i, rule := range decodedGeometryRules {
		check := decodedGeometryChecks[i]
		check.Run = true
		if err := rule.Validate(); err != nil {
			check.Message = err.Error()
			if errors.Is(err, ErrFatal) {
				return report, nil
			}
			continue
		}
		check.Passed = true
	}

	return report, nil
}

func run[T RuleData](v *Validator, checks []*Check, data T) error {
	for i, r := range v.rules {
		check := checks[i]
		rule, ok := r.(*GenericRule[T])
		if !ok {
			continue
		}
		rule.Init(data)
		check.Run = true
		if err := rule.Validate(); err != nil {
			check.Message = err.Error()
			if errors.Is(err, ErrFatal) {
				return err
			}
			continue
		}
		check.Passed = true
	}
	return nil
}
