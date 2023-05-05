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

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/planetlabs/gpq/internal/geojson"
	"github.com/segmentio/parquet-go"
)

type ConvertCmd struct {
	Input  string `arg:"" name:"input" help:"Input file." type:"existingfile"`
	From   string `help:"Input file format.  Possible values: ${enum}." enum:"auto, geojson, geoparquet" default:"auto"`
	Output string `arg:"" name:"output" help:"Output file." type:"path"`
	To     string `help:"Output file format.  Possible values: ${enum}." enum:"auto, geojson, geoparquet" default:"auto"`
	Min    int    `help:"Minimum number of features to consider when building a schema." default:"10"`
	Max    int    `help:"Maximum number of features to consider when building a schema." default:"100"`
}

type FormatType string

const (
	AutoType       FormatType = "auto"
	GeoParquetType FormatType = "geoparquet"
	GeoJSONType    FormatType = "geojson"
	UnknownType    FormatType = "unknown"
)

var validTypes = map[FormatType]bool{
	AutoType:       true,
	GeoParquetType: true,
	GeoJSONType:    true,
}

func parseFormatType(format string) FormatType {
	ft := FormatType(strings.ToLower(format))
	if !validTypes[ft] {
		return UnknownType
	}
	return ft
}

func getFormatType(filename string) FormatType {
	if strings.HasSuffix(filename, ".json") || strings.HasSuffix(filename, ".geojson") {
		return GeoJSONType
	}
	if strings.HasSuffix(filename, ".pq") || strings.HasSuffix(filename, ".parquet") || strings.HasSuffix(filename, ".geoparquet") {
		return GeoParquetType
	}
	return UnknownType
}

func (c *ConvertCmd) Run() error {
	outputFormat := parseFormatType(c.To)
	if outputFormat == AutoType {
		outputFormat = getFormatType(c.Output)
	}
	if outputFormat == UnknownType {
		return fmt.Errorf("could not determine output format for %s", c.Output)
	}

	inputFormat := parseFormatType(c.From)
	if inputFormat == AutoType {
		inputFormat = getFormatType(c.Input)
	}
	if inputFormat == UnknownType {
		return fmt.Errorf("could not determine input format for %s", c.Input)
	}

	if inputFormat == outputFormat {
		return fmt.Errorf("input and output are both the same type: %s", inputFormat)
	}

	input, readErr := os.Open(c.Input)
	if readErr != nil {
		return fmt.Errorf("failed to read from %q: %w", c.Input, readErr)
	}
	defer input.Close()

	output, createErr := os.Create(c.Output)
	if createErr != nil {
		return fmt.Errorf("failed to open %q for writing: %w", c.Output, createErr)
	}
	defer output.Close()

	if inputFormat == GeoParquetType {
		stat, statErr := os.Stat(c.Input)
		if statErr != nil {
			return fmt.Errorf("failed to get size of %q: %w", c.Input, statErr)
		}

		file, fileErr := parquet.OpenFile(input, stat.Size())
		if fileErr != nil {
			return fileErr
		}

		return geojson.FromParquet(file, output)
	}

	convertOptions := &geojson.ConvertOptions{MinFeatures: c.Min, MaxFeatures: c.Max}
	return geojson.ToParquet(input, output, convertOptions)
}
