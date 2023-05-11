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
	"os"

	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/santhosh-tekuri/jsonschema/v5"
	_ "github.com/santhosh-tekuri/jsonschema/v5/httploader"
	"github.com/segmentio/parquet-go"
)

type Validator struct {
	compiler  *jsonschema.Compiler
	schemaMap map[string]string
}

// Options for the Validator.
type Options struct {
	// A lookup of substitute schema locations.  The key is the original schema location
	// and the value is the substitute location.
	SchemaMap map[string]string
}

func (v *Validator) apply(options *Options) {
	if options.SchemaMap != nil {
		v.schemaMap = options.SchemaMap
	}
}

func schemaUrl(version string) string {
	return fmt.Sprintf("https://geoparquet.org/releases/v%s/schema.json", version)
}

// New creates a new Validator.
func New(options ...*Options) *Validator {
	v := &Validator{
		compiler: jsonschema.NewCompiler(),
	}
	for _, opt := range options {
		v.apply(opt)
	}

	return v
}

// Validate validates a GeoParquet file.
func (v *Validator) Validate(ctx context.Context, resource string) error {

	stat, statError := os.Stat(resource)
	if statError != nil {
		return fmt.Errorf("failed to get size of %q: %w", resource, statError)
	}

	input, readErr := os.Open(resource)
	if readErr != nil {
		return fmt.Errorf("failed to read from %q: %w", resource, readErr)
	}
	defer input.Close()

	file, fileErr := parquet.OpenFile(input, stat.Size())
	if fileErr != nil {
		return fileErr
	}

	value, geoErr := geoparquet.GetMetadataValue(file)
	if geoErr != nil {
		return geoErr
	}

	geoFileMetadata := map[string]any{}
	jsonErr := json.Unmarshal([]byte(value), &geoFileMetadata)
	if jsonErr != nil {
		return fmt.Errorf("failed to parse geo metadata: %w", jsonErr)
	}

	versionData, ok := geoFileMetadata["version"]
	if !ok {
		return errors.New("missing version in geo metadata")
	}
	version, ok := versionData.(string)
	if !ok {
		return fmt.Errorf("expected version to be a string, got %v", versionData)
	}

	schema, schemaErr := v.compiler.Compile(schemaUrl(version))
	if schemaErr != nil {
		return fmt.Errorf("failed to compile schema: %w", schemaErr)
	}

	return schema.Validate(geoFileMetadata)
}
