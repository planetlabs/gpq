package geoparquet

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/compress"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/apache/arrow/go/v16/parquet/schema"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/planetlabs/gpq/internal/geo"
	"github.com/planetlabs/gpq/internal/pqutil"
)

type ConvertOptions struct {
	InputPrimaryColumn string
	Compression        string
	RowGroupLength     int
}

func getMetadata(fileReader *file.Reader, convertOptions *ConvertOptions) *Metadata {
	metadata, err := GetMetadata(fileReader.MetaData().KeyValueMetadata())
	if err != nil {
		primaryColumn := DefaultGeometryColumn
		if convertOptions.InputPrimaryColumn != "" {
			primaryColumn = convertOptions.InputPrimaryColumn
		}
		metadata = &Metadata{
			Version:       Version,
			PrimaryColumn: primaryColumn,
			Columns: map[string]*GeometryColumn{
				primaryColumn: getDefaultGeometryColumn(),
			},
		}
	}
	if convertOptions.InputPrimaryColumn != "" && metadata.PrimaryColumn != convertOptions.InputPrimaryColumn {
		metadata.PrimaryColumn = convertOptions.InputPrimaryColumn
	}
	return metadata
}

func FromParquet(input parquet.ReaderAtSeeker, output io.Writer, convertOptions *ConvertOptions) error {
	if convertOptions == nil {
		convertOptions = &ConvertOptions{}
	}

	var compression *compress.Compression
	if convertOptions.Compression != "" {
		c, err := pqutil.GetCompression(convertOptions.Compression)
		if err != nil {
			return err
		}
		compression = &c
	}

	datasetInfo := geo.NewDatasetStats(true)
	transformSchema := func(fileReader *file.Reader) (*schema.Schema, error) {
		inputSchema := fileReader.MetaData().Schema
		inputRoot := inputSchema.Root()
		metadata := getMetadata(fileReader, convertOptions)
		for geomColName := range metadata.Columns {
			if inputRoot.FieldIndexByName(geomColName) < 0 {
				message := fmt.Sprintf(
					"expected a geometry column named %q,"+
						" use the --input-primary-column to supply a different primary geometry",
					geomColName,
				)
				return nil, errors.New(message)
			}
		}
		for fieldNum := 0; fieldNum < inputRoot.NumFields(); fieldNum += 1 {
			field := inputRoot.Field(fieldNum)
			name := field.Name()
			if _, ok := metadata.Columns[name]; !ok {
				continue
			}
			if field.LogicalType() == pqutil.ParquetStringType {
				datasetInfo.AddCollection(name)
			}
		}

		if datasetInfo.NumCollections() == 0 {
			return inputSchema, nil
		}

		numFields := inputRoot.NumFields()
		fields := make([]schema.Node, numFields)
		for fieldNum := 0; fieldNum < numFields; fieldNum += 1 {
			inputField := inputRoot.Field(fieldNum)
			if !datasetInfo.HasCollection(inputField.Name()) {
				fields[fieldNum] = inputField
				continue
			}
			outputField, err := schema.NewPrimitiveNode(inputField.Name(), inputField.RepetitionType(), parquet.Types.ByteArray, -1, -1)
			if err != nil {
				return nil, err
			}
			fields[fieldNum] = outputField
		}

		outputRoot, err := schema.NewGroupNode(inputRoot.Name(), inputRoot.RepetitionType(), fields, -1)
		if err != nil {
			return nil, err
		}
		return schema.NewSchema(outputRoot), nil
	}

	transformColumn := func(inputField *arrow.Field, outputField *arrow.Field, chunked *arrow.Chunked) (*arrow.Chunked, error) {
		if !datasetInfo.HasCollection(inputField.Name) {
			return chunked, nil
		}
		chunks := chunked.Chunks()
		transformed := make([]arrow.Array, len(chunks))
		builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer builder.Release()

		collectionInfo := geo.NewGeometryStats(false)
		for i, arr := range chunks {
			stringArray, ok := arr.(*array.String)
			if !ok {
				return nil, fmt.Errorf("expected a string array for %q, got %v", inputField.Name, arr)
			}
			for rowNum := 0; rowNum < stringArray.Len(); rowNum += 1 {
				if outputField.Nullable && stringArray.IsNull(rowNum) {
					builder.AppendNull()
					continue
				}
				str := stringArray.Value(rowNum)
				geometry, wktErr := wkt.Unmarshal(str)
				if wktErr != nil {
					return nil, wktErr
				}
				value, wkbErr := wkb.Marshal(geometry)
				if wkbErr != nil {
					return nil, wkbErr
				}
				collectionInfo.AddType(geometry.GeoJSONType())
				bounds := geometry.Bound()
				collectionInfo.AddBounds(&bounds)
				builder.Append(value)
			}
			transformed[i] = builder.NewArray()
		}
		datasetInfo.AddBounds(inputField.Name, collectionInfo.Bounds())
		datasetInfo.AddTypes(inputField.Name, collectionInfo.Types())
		chunked.Release()
		return arrow.NewChunked(builder.Type(), transformed), nil
	}

	beforeClose := func(fileReader *file.Reader, fileWriter *pqarrow.FileWriter) error {
		metadata := getMetadata(fileReader, convertOptions)
		for name, geometryCol := range metadata.Columns {
			if !datasetInfo.HasCollection(name) {
				continue
			}
			bounds := datasetInfo.Bounds(name)
			geometryCol.Bounds = []float64{
				bounds.Left(), bounds.Bottom(), bounds.Right(), bounds.Top(),
			}
			geometryCol.GeometryTypes = datasetInfo.Types(name)
		}
		encodedMetadata, jsonErr := json.Marshal(metadata)
		if jsonErr != nil {
			return fmt.Errorf("trouble encoding %q metadata: %w", MetadataKey, jsonErr)
		}
		if err := fileWriter.AppendKeyValueMetadata(MetadataKey, string(encodedMetadata)); err != nil {
			return fmt.Errorf("trouble appending %q metadata: %w", MetadataKey, err)
		}
		return nil
	}

	config := &pqutil.TransformConfig{
		Reader:          input,
		Writer:          output,
		TransformSchema: transformSchema,
		TransformColumn: transformColumn,
		BeforeClose:     beforeClose,
		Compression:     compression,
		RowGroupLength:  convertOptions.RowGroupLength,
	}

	return pqutil.TransformByColumn(config)
}

type BboxColumnFieldNames struct {
	Xmin string
	Ymin string
	Xmax string
	Ymax string
}

func getBboxColumnFieldNames(metadata *Metadata) *BboxColumnFieldNames {
	// infer bbox struct field names
	fieldNames := &BboxColumnFieldNames{}

	if metadata.Columns[metadata.PrimaryColumn].Covering != nil {
		fieldNames.Xmin = metadata.Columns[metadata.PrimaryColumn].Covering.Bbox.Xmin[1]
		fieldNames.Ymin = metadata.Columns[metadata.PrimaryColumn].Covering.Bbox.Ymin[1]
		fieldNames.Xmax = metadata.Columns[metadata.PrimaryColumn].Covering.Bbox.Xmax[1]
		fieldNames.Ymax = metadata.Columns[metadata.PrimaryColumn].Covering.Bbox.Ymax[1]
	} else {
		// fallback to standard names
		fieldNames.Xmin = "xmin"
		fieldNames.Ymin = "ymin"
		fieldNames.Xmax = "xmax"
		fieldNames.Ymax = "ymax"
	}

	return fieldNames
}

type BboxColumn struct {
	Index              int
	Name               string
	BaseColumn         int // the primary geometry column the bbox column references
	BaseColumnEncoding string
	BboxColumnFieldNames
}

// Returns a *BboxColumn struct that contains index, name and other data
// that describe the bounding box column contained in the schema.
// If there is no match for the standard name "bbox" in the schema,
// the covering metadata is consulted.
// An index field value of -1 (alongside an empty name field) means no bbox column found.
func GetBboxColumn(schema *schema.Schema, geoMetadata *Metadata) *BboxColumn {
	bboxCol := &BboxColumn{}
	// try standard name first
	bboxCol.Name = "bbox"
	bboxCol.Index = schema.Root().FieldIndexByName("bbox")

	// if no match, check covering metadata
	if bboxCol.Index == -1 {
		if geoMetadata.Columns[geoMetadata.PrimaryColumn].Covering != nil && len(geoMetadata.Columns[geoMetadata.PrimaryColumn].Covering.Bbox.Xmin) == 2 {
			bboxCol.Name = geoMetadata.Columns[geoMetadata.PrimaryColumn].Covering.Bbox.Xmin[0]
			bboxCol.Index = schema.Root().FieldIndexByName(bboxCol.Name)
		} else {
			bboxCol.Name = ""
		}
	}

	bboxCol.BaseColumn = schema.ColumnIndexByName(geoMetadata.PrimaryColumn)
	bboxCol.BboxColumnFieldNames = *getBboxColumnFieldNames(geoMetadata)
	return bboxCol
}
