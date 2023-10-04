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

package command

import (
	"bytes"
	"io"
	"os"
	"strings"

	"github.com/planetlabs/gpq/internal/geojson"
	"github.com/planetlabs/gpq/internal/geoparquet"
)

type ConvertCmd struct {
	Input              string `arg:"" optional:"" name:"input" help:"Input file.  If not provided, input is read from stdin." type:"path"`
	From               string `help:"Input file format.  Possible values: ${enum}." enum:"auto, geojson, geoparquet, parquet" default:"auto"`
	Output             string `arg:"" optional:"" name:"output" help:"Output file.  If not provided, output is written to stdout." type:"path"`
	To                 string `help:"Output file format.  Possible values: ${enum}." enum:"auto, geojson, geoparquet" default:"auto"`
	Min                int    `help:"Minimum number of features to consider when building a schema." default:"10"`
	Max                int    `help:"Maximum number of features to consider when building a schema." default:"100"`
	InputPrimaryColumn string `help:"Primary geometry column name when reading Parquet withtout metadata." default:"geometry"`
	Compression        string `help:"Parquet compression to use.  Possible values: ${enum}." enum:"uncompressed, snappy, gzip, brotli, zstd" default:"zstd"`
	RowGroupLength     int    `help:"Maximum number of rows per group when writing Parquet."`
}

type FormatType string

const (
	AutoType       FormatType = "auto"
	GeoParquetType FormatType = "geoparquet"
	ParquetType    FormatType = "parquet"
	GeoJSONType    FormatType = "geojson"
	UnknownType    FormatType = "unknown"
)

var validTypes = map[FormatType]bool{
	AutoType:       true,
	GeoParquetType: true,
	ParquetType:    true,
	GeoJSONType:    true,
}

func parseFormatType(format string) FormatType {
	if format == "" {
		return AutoType
	}
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
	if strings.HasSuffix(filename, ".gpq") || strings.HasSuffix(filename, ".geoparquet") {
		return GeoParquetType
	}
	if strings.HasSuffix(filename, ".pq") || strings.HasSuffix(filename, ".parquet") {
		return ParquetType
	}
	return UnknownType
}

func hasStdin() bool {
	stats, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return stats.Size() > 0
}

func (c *ConvertCmd) Run() error {
	inputSource := c.Input
	outputSource := c.Output

	if outputSource == "" && hasStdin() {
		outputSource = inputSource
		inputSource = ""
	}

	outputFormat := parseFormatType(c.To)
	if outputFormat == AutoType {
		if outputSource == "" {
			return NewCommandError("when writing to stdout, the --to option must be provided to determine the output format")
		}
		outputFormat = getFormatType(outputSource)
	}
	if outputFormat == UnknownType {
		return NewCommandError("could not determine output format for %s", outputSource)
	}

	inputFormat := parseFormatType(c.From)
	if inputFormat == AutoType {
		if inputSource == "" {
			return NewCommandError("when reading from stdin, the --from option must be provided to determine the input format")
		}
		inputFormat = getFormatType(inputSource)
	}
	if inputFormat == UnknownType {
		return NewCommandError("could not determine input format for %s", inputSource)
	}

	var input ReaderAtSeeker
	if inputSource == "" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return NewCommandError("trouble reading from stdin: %w", err)
		}
		input = bytes.NewReader(data)
	} else {
		i, readErr := os.Open(inputSource)
		if readErr != nil {
			return NewCommandError("failed to read from %q: %w", inputSource, readErr)
		}
		defer i.Close()
		input = i
	}

	var output *os.File
	if outputSource == "" {
		output = os.Stdout
	} else {
		o, createErr := os.Create(outputSource)
		if createErr != nil {
			return NewCommandError("failed to open %q for writing: %w", outputSource, createErr)
		}
		defer o.Close()
		output = o
	}

	if inputFormat == GeoJSONType {
		if outputFormat != ParquetType && outputFormat != GeoParquetType {
			return NewCommandError("GeoJSON input can only be converted to GeoParquet")
		}
		convertOptions := &geojson.ConvertOptions{
			MinFeatures:    c.Min,
			MaxFeatures:    c.Max,
			Compression:    c.Compression,
			RowGroupLength: c.RowGroupLength,
		}
		if err := geojson.ToParquet(input, output, convertOptions); err != nil {
			return NewCommandError("%w", err)
		}
		return nil
	}

	if outputFormat == GeoJSONType {
		if err := geojson.FromParquet(input, output); err != nil {
			return NewCommandError("%w", err)
		}
		return nil
	}

	convertOptions := &geoparquet.ConvertOptions{
		InputPrimaryColumn: c.InputPrimaryColumn,
		Compression:        c.Compression,
		RowGroupLength:     c.RowGroupLength,
	}

	if err := geoparquet.FromParquet(input, output, convertOptions); err != nil {
		return NewCommandError("%w", err)
	}
	return nil
}
