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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/schema"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"golang.org/x/term"
)

type DescribeCmd struct {
	Input        string `arg:"" optional:"" name:"input" help:"Path or URL for a GeoParquet file.  If not provided, input is read from stdin."`
	Format       string `help:"Report format.  Possible values: ${enum}." enum:"text, json" default:"text"`
	MetadataOnly bool   `help:"Print the unformatted geo metadata only (other arguments will be ignored)."`
	Unpretty     bool   `help:"No newlines or indentation in the JSON output."`
}

const (
	ColName          = "Column"
	ColType          = "Type"
	ColAnnotation    = "Annotation"
	ColRepetition    = "Repetition"
	ColCompression   = "Compression"
	ColEncoding      = "Encoding"
	ColGeometryTypes = "Geometry Types"
	ColBounds        = "Bounds"
	ColDetail        = "Detail"
)

func (c *DescribeCmd) Run() error {
	input, inputErr := readerFromInput(c.Input)
	if inputErr != nil {
		return NewCommandError("trouble getting a reader from %q: %w", c.Input, inputErr)
	}

	fileReader, fileErr := file.NewParquetReader(input)
	if fileErr != nil {
		return fmt.Errorf("failed to read %q as parquet: %w", c.Input, fileErr)
	}
	defer func() { _ = fileReader.Close() }()

	if c.MetadataOnly {
		value, err := geoparquet.GetMetadataValue(fileReader.MetaData().KeyValueMetadata())
		if err != nil {
			if errors.Is(err, geoparquet.ErrNoMetadata) {
				return NewCommandError("missing %q metadata key", geoparquet.MetadataKey)
			}
			return err
		}
		fmt.Println(value)
		return nil
	}

	fileMetadata := fileReader.MetaData()

	info := &DescribeInfo{
		Schema:       buildSchema(fileReader, "", fileMetadata.Schema.Root()),
		NumRows:      fileMetadata.NumRows,
		NumRowGroups: int64(len(fileMetadata.RowGroups)),
	}

	metadata, geoErr := geoparquet.GetMetadata(fileMetadata.KeyValueMetadata())
	if geoErr != nil {
		if errors.Is(geoErr, geoparquet.ErrNoMetadata) {
			message := fmt.Sprintf(
				"Not a valid GeoParquet file (missing the %q metadata key)."+
					" Run convert to try to convert it to GeoParquet.",
				geoparquet.MetadataKey,
			)
			info.Issues = append(info.Issues, message)
		} else {
			message := fmt.Sprintf(
				"Not a valid GeoParquet file (invalid %q metadata)."+
					" Run describe with the --metadata-only flag to see the %q metadata value."+
					" Run validate for more detail on validation issues.",
				geoparquet.MetadataKey,
				geoparquet.MetadataKey,
			)
			info.Issues = append(info.Issues, message)
		}
	} else {
		info.Metadata = metadata
	}

	if c.Format == "json" {
		err := c.formatJSON(info)
		if err != nil {
			return NewCommandError("failed to format report as json: %w", err)
		}
		return nil
	}

	if err := c.formatText(info); err != nil {
		return NewCommandError("failed to format report: %w", err)
	}

	return nil
}

func (c *DescribeCmd) formatText(info *DescribeInfo) error {
	metadata := info.Metadata

	header := table.Row{ColName, ColType, ColAnnotation, ColRepetition, ColCompression}
	columnConfigs := []table.ColumnConfig{}
	if metadata != nil {
		header = append(header, ColEncoding, ColGeometryTypes, ColBounds, ColDetail)
		columnConfigs = append(columnConfigs, table.ColumnConfig{
			Name:             ColEncoding,
			WidthMax:         40,
			WidthMaxEnforcer: text.WrapSoft,
		}, table.ColumnConfig{
			Name:             ColGeometryTypes,
			WidthMax:         40,
			WidthMaxEnforcer: text.WrapSoft,
		}, table.ColumnConfig{
			Name:             ColBounds,
			WidthMax:         40,
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
		row := table.Row{name, field.Type, field.Annotation, repetition, field.Compression}
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

	footerConfig := table.RowConfig{AutoMerge: true, AutoMergeAlign: text.AlignLeft}
	tbl.AppendFooter(makeFooter("Rows", info.NumRows, header), footerConfig)
	tbl.AppendFooter(makeFooter("Row Groups", info.NumRowGroups, header), footerConfig)
	if metadata != nil {
		version := metadata.Version
		if version == "" {
			version = "missing"
		}
		tbl.AppendFooter(makeFooter("GeoParquet Version", version, header), footerConfig)
	}

	style := table.StyleRounded
	style.Format.Footer = text.FormatDefault

	tbl.SetStyle(style)
	tbl.SetOutputMirror(out)
	tbl.Render()

	for _, issue := range info.Issues {
		fmt.Printf(" ⚠️  %s\n", issue)
	}

	return nil
}

func makeFooter(key string, value any, header table.Row) table.Row {
	row := table.Row{key, value}
	for i := len(row); i < len(header); i += 1 {
		row = append(row, value)
	}
	return row
}

func (c *DescribeCmd) formatJSON(info *DescribeInfo) error {
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

type DescribeInfo struct {
	Schema       *DescribeSchema      `json:"schema"`
	Metadata     *geoparquet.Metadata `json:"metadata"`
	NumRows      int64                `json:"rows"`
	NumRowGroups int64                `json:"groups"`
	Issues       []string             `json:"issues"`
}

type DescribeSchema struct {
	Name        string            `json:"name,omitempty"`
	Optional    bool              `json:"optional,omitempty"`
	Repeated    bool              `json:"repeated,omitempty"`
	Type        string            `json:"type,omitempty"`
	Annotation  string            `json:"annotation,omitempty"`
	Compression string            `json:"compression,omitempty"`
	Fields      []*DescribeSchema `json:"fields,omitempty"`
}

func getCompression(fileReader *file.Reader, node schema.Node) string {
	if _, ok := node.(*schema.GroupNode); ok {
		return ""
	}
	if fileReader.NumRowGroups() == 0 {
		return "unknown"
	}
	rowGroupReader := fileReader.RowGroup(0)
	colIndex := fileReader.MetaData().Schema.ColumnIndexByName(node.Path())
	if colIndex < 0 {
		return "unknown"
	}
	col, err := rowGroupReader.MetaData().ColumnChunk(colIndex)
	if err != nil {
		return "unknown"
	}
	return strings.ToLower(col.Compression().String())
}

func buildSchema(fileReader *file.Reader, name string, node schema.Node) *DescribeSchema {
	annotation := ""
	logicalType := node.LogicalType()
	if !logicalType.IsNone() {
		annotation = strings.ToLower(logicalType.String())
	} else if _, isGroup := node.(*schema.GroupNode); isGroup {
		annotation = "group"
	}

	repetition := node.RepetitionType()
	optional := false
	repeated := false
	switch repetition {
	case parquet.Repetitions.Optional:
		optional = true
	case parquet.Repetitions.Repeated:
		repeated = true
	}

	field := &DescribeSchema{
		Name:        name,
		Optional:    optional,
		Repeated:    repeated,
		Annotation:  annotation,
		Compression: getCompression(fileReader, node),
	}

	if leaf, ok := node.(*schema.PrimitiveNode); ok {
		switch leaf.PhysicalType() {
		case parquet.Types.Boolean:
			field.Type = "boolean"
		case parquet.Types.Int32:
			field.Type = "int32"
		case parquet.Types.Int64:
			field.Type = "int64"
		case parquet.Types.Int96:
			field.Type = "int96"
		case parquet.Types.Float:
			field.Type = "float"
		case parquet.Types.Double:
			field.Type = "double"
		case parquet.Types.ByteArray:
			field.Type = "binary"
		case parquet.Types.FixedLenByteArray:
			field.Type = fmt.Sprintf("fixed_len_byte_array(%d)", leaf.TypeLength())
		default:
			field.Type = leaf.PhysicalType().String()
		}
		return field
	}

	if group, ok := node.(*schema.GroupNode); ok {
		count := group.NumFields()
		field.Fields = make([]*DescribeSchema, count)
		for i := 0; i < count; i += 1 {
			groupField := group.Field(i)
			field.Fields[i] = buildSchema(fileReader, groupField.Name(), groupField)
		}
	}
	return field
}
