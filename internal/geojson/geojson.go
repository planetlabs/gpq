package geojson

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v16/parquet"
	"github.com/planetlabs/gpq/internal/geo"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/planetlabs/gpq/internal/pqutil"
)

const primaryColumn = "geometry"

func GetDefaultMetadata() *geoparquet.Metadata {
	return &geoparquet.Metadata{
		Version:       geoparquet.Version,
		PrimaryColumn: primaryColumn,
		Columns: map[string]*geoparquet.GeometryColumn{
			primaryColumn: {
				Encoding:      "WKB",
				GeometryTypes: []string{},
			},
		},
	}
}

func FromParquet(reader parquet.ReaderAtSeeker, writer io.Writer) error {
	recordReader, rrErr := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		Reader: reader,
	})
	if rrErr != nil {
		return rrErr
	}
	defer recordReader.Close()

	geoMetadata := recordReader.Metadata()

	jsonWriter, jsonErr := NewRecordWriter(writer, geoMetadata)
	if jsonErr != nil {
		return jsonErr
	}

	for {
		record, readErr := recordReader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
		if err := jsonWriter.Write(record); err != nil {
			return err
		}
	}

	return jsonWriter.Close()
}

type ConvertOptions struct {
	MinFeatures    int
	MaxFeatures    int
	Compression    string
	RowGroupLength int
	Metadata       string
}

var defaultOptions = &ConvertOptions{
	MinFeatures: 1,
	MaxFeatures: 50,
	Compression: "zstd",
}

func ToParquet(input io.Reader, output io.Writer, convertOptions *ConvertOptions) error {
	if convertOptions == nil {
		convertOptions = defaultOptions
	}
	reader := NewFeatureReader(input)
	buffer := []*geo.Feature{}
	builder := pqutil.NewArrowSchemaBuilder()
	featuresRead := 0

	var pqWriterProps *parquet.WriterProperties
	var writerOptions []parquet.WriterProperty
	if convertOptions.Compression != "" {
		compression, err := pqutil.GetCompression(convertOptions.Compression)
		if err != nil {
			return err
		}
		writerOptions = append(writerOptions, parquet.WithCompression(compression))
	}
	if convertOptions.RowGroupLength > 0 {
		writerOptions = append(writerOptions, parquet.WithMaxRowGroupLength(int64(convertOptions.RowGroupLength)))
	}
	if len(writerOptions) > 0 {
		pqWriterProps = parquet.NewWriterProperties(writerOptions...)
	}

	var featureWriter *geoparquet.FeatureWriter
	writeBuffered := func() error {
		if !builder.Ready() {
			return fmt.Errorf("failed to create schema after reading %d features", len(buffer))
		}
		if err := builder.AddGeometry(geoparquet.DefaultGeometryColumn, geoparquet.DefaultGeometryEncoding); err != nil {
			return err
		}
		sc, scErr := builder.Schema()
		if scErr != nil {
			return scErr
		}
		fw, fwErr := geoparquet.NewFeatureWriter(&geoparquet.WriterConfig{
			Writer:             output,
			ArrowSchema:        sc,
			ParquetWriterProps: pqWriterProps,
		})
		if fwErr != nil {
			return fwErr
		}

		for _, buffered := range buffer {
			if err := fw.Write(buffered); err != nil {
				return err
			}
		}
		featureWriter = fw
		return nil
	}

	for {
		feature, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		featuresRead += 1
		if featureWriter == nil {
			if err := builder.Add(feature.Properties); err != nil {
				return err
			}

			if !builder.Ready() {
				buffer = append(buffer, feature)
				if len(buffer) > convertOptions.MaxFeatures {
					return fmt.Errorf("failed to create parquet schema after reading %d features", convertOptions.MaxFeatures)
				}
				continue
			}

			if len(buffer) < convertOptions.MinFeatures-1 {
				buffer = append(buffer, feature)
				continue
			}

			if err := writeBuffered(); err != nil {
				return err
			}
		}
		if err := featureWriter.Write(feature); err != nil {
			return err
		}
	}
	if featuresRead > 0 {
		if featureWriter == nil {
			if err := writeBuffered(); err != nil {
				return err
			}
		}
		return featureWriter.Close()
	}
	return nil
}
