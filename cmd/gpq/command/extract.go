package command

import (
	"context"
	"io"
	"os"
	"strings"

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
	if c.DropCols != "" {
		cols := strings.Split(c.DropCols, ",")
		config.ExcludeColNames = cols
	}
	if c.KeepOnlyCols != "" {
		cols := strings.Split(c.KeepOnlyCols, ",")
		config.IncludeColNames = cols
	}

	recordReader, rrErr := geoparquet.NewRecordReader(config)
	if rrErr != nil {
		return NewCommandError("trouble reading geoparquet: %w", rrErr)
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

	// parse bbox filter argument into geo.Bbox struct if applicable
	inputBbox, err := geo.NewBboxFromString(c.Bbox)
	if err != nil {
		return NewCommandError(err.Error())
	}

	// read and write records in loop
	for {
		record, readErr := recordReader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}

		filteredRecord, err := geoparquet.FilterRecordBatchByBbox(context.Background(), recordReader, &record, inputBbox)
		if err != nil {
			return NewCommandError(err.Error())
		}

		if err := recordWriter.Write(*filteredRecord); err != nil {
			return err
		}
	}
	return nil
}
