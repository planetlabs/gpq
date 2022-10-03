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
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"testing"

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
		"example-v0.4.0.parquet",
		"example-v1.0.0-beta.1.parquet",
	}

	v := validator.New()
	ctx := context.Background()
	for _, c := range cases {
		s.Run(c, func() {
			resourcePath := path.Join("../testdata/cases", c)
			err := v.Validate(ctx, resourcePath)
			s.Assert().NoError(err)
		})
	}
}

func TestSuite(t *testing.T) {
	suite.Run(t, &Suite{})
}
