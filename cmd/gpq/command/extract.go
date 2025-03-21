package command

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/planetlabs/gpq/internal/geo"
	"github.com/planetlabs/gpq/internal/geoparquet"
)

type ExtractCmd struct {
	Input        string `arg:"" optional:"" name:"input" help:"Input file path or URL.  If not provided, input is read from stdin."`
	Output       string `arg:"" optional:"" name:"output" help:"Output file.  If not provided, output is written to stdout." type:"path"`
	Bbox         string `help:"Filter features by intersection of their bounding box with the provided bounding box (in x_min,y_min,x_max,y_max format)."`
	DropCols     string `help:"Drop the provided columns. Provide a comma-separated string of column names to be excluded. Do not use together with --keep-only-cols."`
	KeepOnlyCols string `help:"Keep only the provided columns. Provide a comma-separated string of columns to be kept. Do not use together with --drop-cols."`
}

func (c *ExtractCmd) Run() error {

	// validate and transform inputs

	inputSource := c.Input
	outputSource := c.Output

	if c.Input == "" && hasStdin() {
		outputSource = inputSource
		inputSource = ""
	}

	input, inputErr := readerFromInput(inputSource)
	if inputErr != nil {
		return NewCommandError("trouble getting a reader from %q: %w", c.Input, inputErr)
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

	// prepare input reader (ignore certain columns if asked to - DropCols/KeepOnlyCols)
	config := &geoparquet.ReaderConfig{Reader: input}

	parquetFileReader, err := geoparquet.NewParquetFileReader(config)
	if err != nil {
		return NewCommandError("could not get ParquetFileReader: %w", err)
	}

	arrowFileReader, err := geoparquet.NewArrowFileReader(config, parquetFileReader)
	if err != nil {
		return NewCommandError("could not get ArrowFileReader: %w", err)
	}

	geoMetadata, err := geoparquet.GetMetadataFromFileReader(parquetFileReader)
	if err != nil {
		return NewCommandError("could not get geo metadata from file reader: %w", err)
	}

	arrowSchema, schemaErr := arrowFileReader.Schema()
	if schemaErr != nil {
		return NewCommandError("trouble getting arrow schema: %w", schemaErr)
	}

	// projection pushdown - column filtering
	var columnIndices []int = nil

	var includeColumns []string
	var excludeColumns []string
	if c.DropCols != "" {
		excludeColumns = strings.Split(c.DropCols, ",")
	}
	if c.KeepOnlyCols != "" {
		includeColumns = strings.Split(c.KeepOnlyCols, ",")
	}

	excludeColNamesProvided := len(excludeColumns) > 0
	includeColNamesProvided := len(includeColumns) > 0

	if excludeColNamesProvided || includeColNamesProvided {
		if excludeColNamesProvided == includeColNamesProvided {
			return NewCommandError("please pass only one of DropColumns/KeepOnlyColumns")
		}

		if includeColNamesProvided {
			columnIndices, err = geoparquet.GetColumnIndices(includeColumns, arrowSchema)
			if err != nil {
				return NewCommandError("trouble inferring column names (positive selection): %w", err)
			}
		}

		if excludeColNamesProvided {
			columnIndices, err = geoparquet.GetColumnIndicesByDifference(excludeColumns, arrowSchema)
			if err != nil {
				return NewCommandError("trouble inferring column names (negative selection): %w", err)
			}
		}
	}
	config.Columns = columnIndices

	// predicate pushdown - spatial row filtering
	var rowGroups []int = nil

	// parse bbox filter argument into geo.Bbox struct if applicable
	inputBbox, err := geo.NewBboxFromString(c.Bbox)
	if err != nil {
		return NewCommandError("trouble getting bbox from input string: %w", err)
	}
	var bboxCol *geoparquet.BboxColumn
	if inputBbox != nil {
		bboxCol = geoparquet.GetBboxColumn(parquetFileReader.MetaData().Schema, geoMetadata)

		if bboxCol.Name != "" { // if there is a bbox col in the file
			rowGroups, err = geoparquet.GetRowGroupsByBbox(parquetFileReader, bboxCol, inputBbox)
			if err != nil {
				return NewCommandError("trouble scanning row group metadata: %w", err)
			}
		}
	}

	config.RowGroups = rowGroups

	// create new record reader - based on the config values for
	// Columns and RowGroups it will only read a subset of
	// columns and row groups
	ctx := context.Background()

	recordReader, err := geoparquet.NewRecordReader(ctx, arrowFileReader, geoMetadata, columnIndices, rowGroups)
	if err != nil {
		return NewCommandError("trouble creating geoparquet record reader: %w", err)
	}
	defer recordReader.Close()

	// prepare output writer
	recordWriter, rwErr := geoparquet.NewRecordWriter(&geoparquet.WriterConfig{
		Writer:      output,
		Metadata:    recordReader.Metadata(),
		ArrowSchema: recordReader.ArrowSchema(),
	})
	if rwErr != nil {
		return NewCommandError("trouble getting record writer: %w", rwErr)
	}
	defer recordWriter.Close()

	// read and write records in loop
	for {
		record, readErr := recordReader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}

		// filter by bbox if asked to
		var filteredRecord *arrow.Record
		if inputBbox != nil && bboxCol != nil {
			var filterErr error
			filteredRecord, filterErr = geoparquet.FilterRecordBatchByBbox(ctx, &record, inputBbox, bboxCol)
			if filterErr != nil {
				return NewCommandError("trouble filtering record batch by bbox: %w", filterErr)
			}
		} else {
			filteredRecord = &record
		}

		if err := recordWriter.Write(*filteredRecord); err != nil {
			return err
		}
	}
	return nil
}
