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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/segmentio/parquet-go"
	"golang.org/x/term"
)

type DescribeCmd struct {
	Input    string `arg:"" name:"input" help:"Path to a GeoParquet file." type:"existingfile"`
	Format   string `help:"Report format.  Possible values: ${enum}." enum:"text, json" default:"text"`
	Unpretty bool   `help:"No newlines or indentation in the JSON output."`
}

const (
	ColName          = "Column"
	ColType          = "Type"
	ColAnnotation    = "Annotation"
	ColRepetition    = "Repetition"
	ColEncoding      = "Encoding"
	ColGeometryTypes = "Geometry Types"
	ColBounds        = "Bounds"
	ColDetail        = "Detail"
)

func (c *DescribeCmd) Run() error {
	input, readErr := os.Open(c.Input)
	if readErr != nil {
		return fmt.Errorf("failed to read from %q: %w", c.Input, readErr)
	}
	defer input.Close()

	stat, statErr := os.Stat(c.Input)
	if statErr != nil {
		return fmt.Errorf("failed to get size of %q: %w", c.Input, statErr)
	}

	file, fileErr := parquet.OpenFile(input, stat.Size())
	if fileErr != nil {
		return fileErr
	}

	metadata, geoErr := geoparquet.GetMetadata(file)
	if geoErr != nil {
		if !errors.Is(geoErr, geoparquet.ErrNoMetadata) {
			return geoErr
		}
	}

	schema := buildSchema("", file.Schema())

	info := &Info{
		Schema:   schema,
		Metadata: metadata,
		NumRows:  file.NumRows(),
	}

	if c.Format == "json" {
		return c.formatJSON(info)
	}
	return c.formatText(info)
}

func (c *DescribeCmd) formatText(info *Info) error {
	metadata := info.Metadata

	header := table.Row{ColName, ColType, ColAnnotation, ColRepetition}
	columnConfigs := []table.ColumnConfig{}
	if metadata != nil {
		header = append(header, ColEncoding, ColGeometryTypes, ColBounds, ColDetail)
		columnConfigs = append(columnConfigs, table.ColumnConfig{
			Name:             ColGeometryTypes,
			WidthMax:         50,
			WidthMaxEnforcer: text.WrapSoft,
		}, table.ColumnConfig{
			Name:             ColBounds,
			WidthMax:         50,
			WidthMaxEnforcer: text.WrapSoft,
		})
	}

	out := os.Stdout
	tbl := table.NewWriter()
	if term.IsTerminal(int(out.Fd())) {
		width, _, err := term.GetSize(int(out.Fd()))
		if err == nil {
			tbl.SetAllowedRowLength(width)
		}
	}

	tbl.SetColumnConfigs(columnConfigs)
	tbl.AppendHeader(header)

	for _, field := range info.Schema.Fields {
		name := field.Name
		if metadata != nil && metadata.PrimaryColumn == name {
			name = text.Bold.Sprint(name)
		}
		repetition := "1"
		if field.Repeated {
			repetition = "0..*"
		} else if field.Optional {
			repetition = "0..1"
		}
		row := table.Row{name, field.Type, field.Annotation, repetition}
		if metadata != nil {
			geoColumn, ok := metadata.Columns[field.Name]
			if !ok {
				row = append(row, "")
			} else {
				types := strings.Join(geoColumn.GetGeometryTypes(), ", ")
				bounds := ""
				if geoColumn.Bounds != nil {
					values := make([]string, len(geoColumn.Bounds))
					for i, v := range geoColumn.Bounds {
						values[i] = strconv.FormatFloat(v, 'f', -1, 64)
					}
					bounds = fmt.Sprintf("[%s]", strings.Join(values, ", "))
				}
				details := table.NewWriter()
				details.SetStyle(table.StyleLight)
				details.Style().Options.DrawBorder = false
				if geoColumn.Orientation != "" {
					details.AppendRow(table.Row{"orientation", geoColumn.Orientation})
				}
				if geoColumn.Edges != "" {
					details.AppendRow(table.Row{"edges", geoColumn.Edges})
				}
				if geoColumn.CRS != nil {
					details.AppendRow(table.Row{"crs", geoColumn.CRS})
				}
				row = append(row, geoColumn.Encoding, types, bounds, details.Render())
			}
		}

		tbl.AppendRow(row)
	}

	tbl.AppendFooter(makeFooter("Rows", info.NumRows, header), table.RowConfig{AutoMerge: true})
	if metadata != nil {
		version := metadata.Version
		if version == "" {
			version = "missing"
		}
		tbl.AppendFooter(makeFooter("Version", version, header), table.RowConfig{AutoMerge: true, AutoMergeAlign: text.AlignLeft})
	}

	tbl.SetStyle(table.StyleRounded)
	tbl.SetOutputMirror(out)
	tbl.Render()

	return nil
}

func makeFooter(key string, value any, header table.Row) table.Row {
	row := table.Row{key, value}
	for i := len(row); i < len(header); i += 1 {
		row = append(row, "")
	}
	return row
}

func (c *DescribeCmd) formatJSON(info *Info) error {
	encoder := json.NewEncoder(os.Stdout)
	if !c.Unpretty {
		encoder.SetIndent("", "  ")
		encoder.SetEscapeHTML(false)
	}
	if err := encoder.Encode(info); err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	return nil
}

type Info struct {
	Schema   *Schema              `json:"schema"`
	Metadata *geoparquet.Metadata `json:"metadata"`
	NumRows  int64                `json:"rows"`
}

type Schema struct {
	Name       string    `json:"name,omitempty"`
	Optional   bool      `json:"optional,omitempty"`
	Repeated   bool      `json:"repeated,omitempty"`
	Type       string    `json:"type,omitempty"`
	Annotation string    `json:"annotation,omitempty"`
	Fields     []*Schema `json:"fields,omitempty"`
}

func buildSchema(name string, node parquet.Node) *Schema {
	nodeType := node.Type()
	annotation := ""
	if logicalType := nodeType.LogicalType(); logicalType != nil {
		annotation = logicalType.String()
	}

	field := &Schema{
		Name:       name,
		Optional:   node.Optional(),
		Repeated:   node.Repeated(),
		Annotation: annotation,
	}

	if node.Leaf() {
		switch nodeType.Kind() {
		case parquet.Boolean:
			field.Type = "boolean"
		case parquet.Int32:
			field.Type = "int32"
		case parquet.Int64:
			field.Type = "int64"
		case parquet.Int96:
			field.Type = "int96"
		case parquet.Float:
			field.Type = "float"
		case parquet.Double:
			field.Type = "double"
		case parquet.ByteArray:
			field.Type = "binary"
		case parquet.FixedLenByteArray:
			field.Type = fmt.Sprintf("fixed_len_byte_array(%d)", nodeType.Length())
		default:
			field.Type = "unknown"
		}
		return field
	}

	field.Fields = make([]*Schema, len(node.Fields()))
	for i, groupField := range node.Fields() {
		field.Fields[i] = buildSchema(groupField.Name(), groupField)
	}
	return field
}
