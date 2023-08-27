package geojson

import (
	"encoding/json"
	"io"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
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
			properties[name] = value
		}

		feature := map[string]any{
			"type":       "Feature",
			"properties": properties,
			"geometry":   geometry,
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
