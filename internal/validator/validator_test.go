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

package validator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/planetlabs/gpq/internal/geojson"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/planetlabs/gpq/internal/pqutil"
	"github.com/planetlabs/gpq/internal/validator"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/stretchr/testify/suite"
)

func loadSchema(schemaURL string) (io.ReadCloser, error) {
	u, err := url.Parse(schemaURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema url: %w", err)
	}
	schemaPath := path.Join("../testdata", "schema", u.Host, u.Path)
	file, openErr := os.Open(schemaPath)
	if openErr != nil {
		return nil, fmt.Errorf("failed to open schema file: %w", openErr)
	}
	return file, nil
}

type Suite struct {
	suite.Suite
	originalHttpLoader  func(string) (io.ReadCloser, error)
	originalHttpsLoader func(string) (io.ReadCloser, error)
}

func (s *Suite) readExpected(name string) []byte {
	expected, err := os.ReadFile(path.Join("testdata", name, "expected.json"))
	s.Require().NoError(err)
	return expected
}

func (s *Suite) writeActual(name string, data []byte) {
	err := os.WriteFile(path.Join("testdata", name, "actual.json"), data, 0644)
	s.Require().NoError(err)
}

type Spec struct {
	Metadata json.RawMessage `json:"metadata"`
	Data     json.RawMessage `json:"data"`
}

func (s *Suite) readSpec(name string) *Spec {
	data, err := os.ReadFile(path.Join("testdata", name, "input.json"))
	s.Require().NoError(err)
	input := &Spec{}
	s.Require().NoError(json.Unmarshal(data, input))
	return input
}

func (s *Suite) copyWithMetadata(input parquet.ReaderAtSeeker, output io.Writer, metadata string) {
	config := &pqutil.TransformConfig{
		Reader: input,
		Writer: output,
		BeforeClose: func(fileWriter *file.Writer) error {
			return fileWriter.AppendKeyValueMetadata(geoparquet.MetadataKey, metadata)
		},
	}
	s.Require().NoError(pqutil.TransformByColumn(config))
}

func (s *Suite) generateGeoParquet(name string) *file.Reader {
	spec := s.readSpec(name)

	initialOutput := &bytes.Buffer{}

	options := &geojson.ConvertOptions{
		Metadata: string(spec.Metadata),
	}
	s.Require().NoError(geojson.ToParquet(bytes.NewReader(spec.Data), initialOutput, options))

	input := bytes.NewReader(initialOutput.Bytes())
	output := &bytes.Buffer{}
	s.copyWithMetadata(input, output, string(spec.Metadata))

	fileReader, err := file.NewParquetReader(bytes.NewReader(output.Bytes()))
	s.Require().NoError(err)

	return fileReader
}

func (s *Suite) assertExpectedReport(name string, report *validator.Report) {
	actual, err := json.MarshalIndent(report, "", "  ")
	s.Require().NoError(err)
	s.writeActual(name, actual)
	expected := s.readExpected(name)
	s.Assert().JSONEq(string(expected), string(actual))
}

func (s *Suite) SetupSuite() {
	s.originalHttpLoader = jsonschema.Loaders["http"]
	s.originalHttpsLoader = jsonschema.Loaders["https"]
	jsonschema.Loaders["http"] = loadSchema
	jsonschema.Loaders["https"] = loadSchema
}

func (s *Suite) TearDownSuite() {
	jsonschema.Loaders["http"] = s.originalHttpLoader
	jsonschema.Loaders["https"] = s.originalHttpsLoader
}

func (s *Suite) TestValidCases() {
	cases := []string{
		"example-v1.0.0-beta.1.parquet",
	}

	validatorAll := validator.New(false)
	validatorMeta := validator.New(true)

	ctx := context.Background()

	for _, c := range cases {
		s.Run(c, func() {
			resourcePath := path.Join("../testdata/cases", c)

			allReport, allErr := validatorAll.Validate(ctx, resourcePath)
			s.Require().NoError(allErr)
			s.assertExpectedReport("all-pass", allReport)

			metaReport, metaErr := validatorMeta.Validate(ctx, resourcePath)
			s.Require().NoError(metaErr)
			s.assertExpectedReport("all-pass-meta", metaReport)
		})
	}
}

func (s *Suite) TestReport() {
	cases := []string{
		"all-pass",
		"all-pass-meta",
		"all-pass-minimal",
		"bad-metadata-type",
		"missing-version",
		"missing-primary-column",
		"missing-columns",
		"bad-primary-column",
		"missing-encoding",
		"bad-encoding",
		"missing-geometry-types",
		"bad-geometry-types",
		"bad-crs",
		"bad-orientation",
		"bad-edges",
		"bad-bbox-type",
		"bad-bbox-length",
		"bad-epoch",
		"geometry-type-not-in-list",
		"geometry-correctly-oriented",
		"geometry-incorrectly-oriented",
		"geometry-outside-bbox",
		"geometry-inside-antimeridian-spanning-bbox",
		"geometry-outside-antimeridian-spanning-bbox",
	}

	ctx := context.Background()
	for _, c := range cases {
		s.Run(c, func() {
			metadataOnly := strings.HasSuffix(c, "-meta")
			v := validator.New(metadataOnly)

			report, err := v.Report(ctx, s.generateGeoParquet(c))
			s.Require().NoError(err)

			s.assertExpectedReport(c, report)
		})
	}
}

func TestSuite(t *testing.T) {
	suite.Run(t, &Suite{})
}
