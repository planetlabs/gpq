package geojson

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	orbjson "github.com/paulmach/orb/geojson"
	"github.com/planetlabs/gpq/internal/geo"
	"github.com/planetlabs/gpq/internal/geoparquet"
)

type RecordWriter struct {
	geoMetadata *geoparquet.Metadata
	writer      io.Writer
	writing     bool
}

func NewRecordWriter(writer io.Writer, geoMetadata *geoparquet.Metadata) (*RecordWriter, error) {
	w := &RecordWriter{writer: writer, geoMetadata: geoMetadata}
	return w, nil
}

var (
	featureCollectionPrefix = []byte(`{"type":"FeatureCollection","features":[`)
	arraySeparator          = []byte(",")
	featureCollectionSuffix = []byte("]}")
)

func (w *RecordWriter) Write(record arrow.Record) error {
	if !w.writing {
		if _, err := w.writer.Write(featureCollectionPrefix); err != nil {
			return err
		}
		w.writing = true
	} else {
		if _, err := w.writer.Write(arraySeparator); err != nil {
			return err
		}
	}
	arr := array.RecordToStructArray(record)
	defer arr.Release()

	schema := record.Schema()
	for rowNum := 0; rowNum < arr.Len(); rowNum += 1 {
		if rowNum > 0 {
			if _, err := w.writer.Write(arraySeparator); err != nil {
				return err
			}
		}

		var geometry *orbjson.Geometry
		var bbox *orbjson.BBox
		properties := map[string]any{}
		for fieldNum := 0; fieldNum < arr.NumField(); fieldNum += 1 {
			value := arr.Field(fieldNum).GetOneForMarshal(rowNum)
			name := schema.Field(fieldNum).Name
			if geomColumn, ok := w.geoMetadata.Columns[name]; ok {
				g, decodeErr := geo.DecodeGeometry(value, geomColumn.Encoding)
				if decodeErr != nil {
					return decodeErr
				}
				if name == w.geoMetadata.PrimaryColumn {
					geometry = g
					continue
				}
				properties[name] = g
				continue
			}

			bboxCol := geoparquet.GetBboxColumnNameFromMetadata(w.geoMetadata)
			if value != nil && (name == bboxCol || (bboxCol == "" && name == geoparquet.DefaultBboxColumn)) {
				bboxMap, ok := value.(map[string]any)
				if !ok {
					return errors.New("value is not of type map[string]any")
				}
				fieldNames := geoparquet.GetBboxColumnFieldNames(w.geoMetadata)
				xmin, xminOk := bboxMap[fieldNames.Xmin]
				ymin, yminOk := bboxMap[fieldNames.Ymin]
				xmax, xmaxOk := bboxMap[fieldNames.Xmax]
				ymax, ymaxOk := bboxMap[fieldNames.Ymax]
				if !(xminOk && yminOk && xmaxOk && ymaxOk) {
					return fmt.Errorf("bbox struct must have fields %v/%v/%v/%v", fieldNames.Xmin, fieldNames.Ymin, fieldNames.Xmax, fieldNames.Ymax)
				}
				if xmin == nil || ymin == nil || xmax == nil || ymax == nil {
					return errors.New("bbox struct must have non-null values")
				}
				orbBbox := orbjson.BBox([]float64{xmin.(float64), ymin.(float64), xmax.(float64), ymax.(float64)})
				bbox = &orbBbox
				continue
			}

			properties[name] = value
		}

		feature := map[string]any{
			"type":       "Feature",
			"properties": properties,
			"geometry":   geometry,
		}

		if bbox != nil {
			feature["bbox"] = bbox
		}

		featureData, jsonErr := json.Marshal(feature)
		if jsonErr != nil {
			return jsonErr
		}
		if _, err := w.writer.Write(featureData); err != nil {
			return err
		}
	}

	return nil
}

func (w *RecordWriter) Close() error {
	if w.writing {
		if _, err := w.writer.Write(featureCollectionSuffix); err != nil {
			return err
		}
		w.writing = false
	}

	closer, ok := w.writer.(io.Closer)
	if ok {
		return closer.Close()
	}
	return nil
}
