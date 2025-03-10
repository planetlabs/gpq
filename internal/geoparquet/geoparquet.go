package geoparquet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/compute"
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

// Returns the index of the bbox column, -1 means not found.
// If there is no match for the standard name "bbox", the covering metadata is consulted.
func GetBboxColumnIndex(schema *schema.Schema, metadata *Metadata) int {
	// try standard name first
	bboxColIdx := schema.Root().FieldIndexByName("bbox")
	// if no match, check covering metadata
	if bboxColIdx == -1 && metadata.Columns[metadata.PrimaryColumn].Covering != nil && len(metadata.Columns[metadata.PrimaryColumn].Covering.Bbox.Xmin) == 2 {
		bboxColName := metadata.Columns[metadata.PrimaryColumn].Covering.Bbox.Xmin[0]
		bboxColIdx = schema.Root().FieldIndexByName(bboxColName)
	}
	return bboxColIdx
}

func FilterRecordBatchByBbox(ctx context.Context, recordReader *RecordReader, record *arrow.Record, inputBbox *geo.Bbox) (*arrow.Record, error) {

	metadata := recordReader.Metadata()
	schema := recordReader.Schema()

	bboxColIdx := -1 // -1 means no column found
	if inputBbox != nil {
		bboxColIdx = GetBboxColumnIndex(schema, metadata)
	}

	var filteredRecord *arrow.Record

	if inputBbox != nil && bboxColIdx != -1 { // bbox argument has been provided and there is a bbox column we can use for filtering
		col := (*record).Column(bboxColIdx).(*array.Struct)
		defer col.Release()

		// we build a boolean mask and pass it to compute.FilterRecordBatch later
		maskBuilder := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer maskBuilder.Release()

		var xminName string
		var yminName string
		var xmaxName string
		var ymaxName string

		// loop over individual bbox values per record
		for idx := 0; idx < col.Len(); idx++ {
			var bbox map[string]json.RawMessage
			if err := json.Unmarshal([]byte(col.ValueStr(idx)), &bbox); err != nil {
				return nil, fmt.Errorf("trouble unmarshalling bbox struct: %w", err)
			}

			// infer bbox field names from the first element
			if idx == 0 {
				// check standard name first, if no match, check covering metadata
				if _, ok := bbox["xmin"]; ok {
					xminName = "xmin"
				} else if metadata.Columns[metadata.PrimaryColumn].Covering != nil {
					xminName = "xmin" // DEBUG metadata.Columns[metadata.PrimaryColumn].Covering.Xmin[1]
				} else {
					return nil, fmt.Errorf("can not infer bbox field name for 'xmin'")
				}

				if _, ok := bbox["ymin"]; ok {
					yminName = "ymin"
				} else if metadata.Columns[metadata.PrimaryColumn].Covering != nil {
					yminName = metadata.Columns[metadata.PrimaryColumn].Covering.Bbox.Ymin[1]
				} else {
					return nil, fmt.Errorf("can not infer bbox field name for 'ymin'")
				}

				if _, ok := bbox["xmax"]; ok { // check standard name first
					xmaxName = "xmax"
				} else if metadata.Columns[metadata.PrimaryColumn].Covering != nil {
					xmaxName = metadata.Columns[metadata.PrimaryColumn].Covering.Bbox.Xmax[1]
				} else {
					return nil, fmt.Errorf("can not infer bbox field name for 'xmax'")
				}

				if _, ok := bbox["ymax"]; ok {
					ymaxName = "ymax"
				} else if metadata.Columns[metadata.PrimaryColumn].Covering != nil {
					ymaxName = metadata.Columns[metadata.PrimaryColumn].Covering.Bbox.Ymax[1]
				} else {
					return nil, fmt.Errorf("can not infer bbox field name for 'ymax'")
				}
			}

			bboxValue := &geo.Bbox{} // create empty struct to hold bbox values of this record

			if err := json.Unmarshal(bbox[xminName], &bboxValue.Xmin); err != nil {
				return nil, fmt.Errorf("trouble parsing bbox.%v field: %w", xminName, err)
			}
			if err := json.Unmarshal(bbox[yminName], &bboxValue.Ymin); err != nil {
				return nil, fmt.Errorf("trouble parsing bbox.%v field: %w", yminName, err)
			}
			if err := json.Unmarshal(bbox[xmaxName], &bboxValue.Xmax); err != nil {
				return nil, fmt.Errorf("trouble parsing bbox.%v field: %w", xmaxName, err)
			}
			if err := json.Unmarshal(bbox[ymaxName], &bboxValue.Ymax); err != nil {
				return nil, fmt.Errorf("trouble parsing bbox.%v field: %w", ymaxName, err)
			}

			// check whether the bbox passed to this function
			// intersects with the bbox of the record
			maskBuilder.Append(inputBbox.Intersects(bboxValue))
		}

		r, filterErr := compute.FilterRecordBatch(ctx, *record, maskBuilder.NewBooleanArray(), &compute.FilterOptions{NullSelection: 0}) // TODO check what this is doing
		if filterErr != nil {
			return nil, fmt.Errorf("trouble filtering record batch: %w", filterErr)
		}
		filteredRecord = &r
	} else if inputBbox != nil && bboxColIdx == -1 {
		// bbox filter passed to function but there is no bbox col.
		// this means we have to compute the bbox of the records ourselves
		primaryColIdx := schema.ColumnIndexByName(metadata.PrimaryColumn)
		col := (*record).Column(primaryColIdx)
		defer col.Release()

		maskBuilder := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer maskBuilder.Release()

		for idx := 0; idx < col.Len(); idx++ {
			value := col.GetOneForMarshal(idx)
			g, decodeErr := geo.DecodeGeometry(value, metadata.Columns[metadata.PrimaryColumn].Encoding)
			if decodeErr != nil {
				return nil, fmt.Errorf("trouble decoding geometry: %w", decodeErr)
			}
			bounds := g.Coordinates.Bound()
			bboxValue := &geo.Bbox{
				Xmin: bounds.Min.X(),
				Ymin: bounds.Min.Y(),
				Xmax: bounds.Max.X(),
				Ymax: bounds.Max.Y(),
			}

			// now that we've computed the bbox, same logic as above
			maskBuilder.Append(inputBbox.Intersects(bboxValue))
		}
		filter := maskBuilder.NewBooleanArray()
		r, filterErr := compute.FilterRecordBatch(ctx, *record, filter, &compute.FilterOptions{NullSelection: 0}) // TODO check what this is doing
		if filterErr != nil {
			return nil, fmt.Errorf("trouble filtering record batch with computed bbox: %w (%v vs. %v)", filterErr, (*record).NumRows(), filter.Len())
		}
		filteredRecord = &r
	} else {
		filteredRecord = record
	}
	return filteredRecord, nil
}
