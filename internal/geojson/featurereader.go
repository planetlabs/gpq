package geojson

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/paulmach/orb"
	orbjson "github.com/paulmach/orb/geojson"
	"github.com/planetlabs/gpq/internal/geo"
)

type FeatureReader struct {
	collection bool
	decoder    *json.Decoder
}

func NewFeatureReader(input io.Reader) *FeatureReader {
	return &FeatureReader{
		decoder: json.NewDecoder(input),
	}
}

func (r *FeatureReader) Read() (*geo.Feature, error) {
	if r.decoder == nil {
		return nil, io.EOF
	}

	if r.collection {
		return r.readFeature()
	}

	defer func() {
		if !r.collection {
			r.decoder = nil
		}
	}()

	token, err := r.decoder.Token()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}

	delim, ok := token.(json.Delim)
	if !ok || delim != json.Delim('{') {
		return nil, fmt.Errorf("expected a JSON object, got %s", token)
	}

	var parsedType string
	var feature *geo.Feature
	var coordinatesJSON json.RawMessage
	for {
		keyToken, keyErr := r.decoder.Token()
		if keyErr == io.EOF {
			if feature == nil {
				return nil, io.EOF
			}
			return feature, nil
		}
		if keyErr != nil {
			return nil, keyErr
		}

		delim, ok := keyToken.(json.Delim)
		if ok && delim == json.Delim('}') {
			if r.decoder.More() {
				r.collection = true
			}
			if feature == nil {
				return nil, errors.New("expected a FeatureCollection, a Feature, or a Geometry object")
			}
			return feature, nil
		}

		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected token: %s", token)
		}

		if key == "geometry" {
			if feature == nil {
				feature = &geo.Feature{}
			} else if feature.Geometry != nil {
				return nil, errors.New("found duplicate geometry")
			}
			geometry := &orbjson.Geometry{}
			if err := r.decoder.Decode(geometry); err != nil {
				return nil, fmt.Errorf("trouble parsing geometry: %w", err)
			}
			feature.Geometry = geometry.Geometry()
			continue
		}

		if key == "properties" {
			if feature == nil {
				feature = &geo.Feature{}
			} else if feature.Properties != nil {
				return nil, errors.New("found duplicate properties")
			}
			properties := map[string]any{}
			if err := r.decoder.Decode(&properties); err != nil {
				return nil, fmt.Errorf("trouble parsing properties: %w", err)
			}
			feature.Properties = properties
			continue
		}

		if key == "coordinates" {
			if feature == nil {
				feature = &geo.Feature{}
			} else if feature.Geometry != nil {
				return nil, errors.New("found unexpected coordinates")
			}
			if coordinatesJSON != nil {
				return nil, errors.New("found duplicate coordinates")
			}
			if err := r.decoder.Decode(&coordinatesJSON); err != nil {
				return nil, fmt.Errorf("trouble parsing coordinates")
			}
			if parsedType != "" {
				return r.featureFromCoordinates(parsedType, coordinatesJSON)
			}
			continue
		}

		valueToken, valueErr := r.decoder.Token()
		if valueErr != nil {
			return nil, valueErr
		}

		if key == "type" {
			if parsedType != "" {
				return nil, errors.New("found duplicate type")
			}
			value, ok := valueToken.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected type: %s", valueToken)
			}
			parsedType = value
			if coordinatesJSON != nil {
				return r.featureFromCoordinates(parsedType, coordinatesJSON)
			}
			continue
		}

		if key == "features" {
			if parsedType != "" && parsedType != "FeatureCollection" {
				return nil, fmt.Errorf("found features in unexpected %q type", parsedType)
			}
			delim, ok := valueToken.(json.Delim)
			if !ok || delim != json.Delim('[') {
				return nil, fmt.Errorf("expected an array of features, got %s", token)
			}
			r.collection = true
			return r.readFeature()
		}

		if key == "geometries" {
			if parsedType != "" && parsedType != "GeometryCollection" {
				return nil, fmt.Errorf("found geometries in unexpected %q type", parsedType)
			}
			delim, ok := valueToken.(json.Delim)
			if !ok || delim != json.Delim('[') {
				return nil, fmt.Errorf("expected an array of geometries, got %s", token)
			}
			return r.readGeometryCollection()
		}

		if key == "id" {
			if feature == nil {
				feature = &geo.Feature{}
			} else if feature.Id != nil {
				return nil, errors.New("found duplicate id")
			}
			_, stringId := valueToken.(string)
			_, floatId := valueToken.(float64)
			if !stringId && !floatId {
				return nil, fmt.Errorf("expected id to be a string or number, got: %v", valueToken)
			}
			feature.Id = valueToken
			continue
		}

		if delim, ok := valueToken.(json.Delim); ok {
			switch delim {
			case json.Delim('['):
				err := r.scanToMatching('[', ']')
				if err != nil {
					return nil, err
				}
			case json.Delim('{'):
				err := r.scanToMatching('{', '}')
				if err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("unexpected token: %s", delim)
			}
		}
	}
}

func (r *FeatureReader) scanToMatching(fromDelim json.Delim, toDelim json.Delim) error {
	depth := 1
	for {
		token, err := r.decoder.Token()
		if err != nil {
			return fmt.Errorf("unexpected token: %w", err)
		}
		delim, ok := token.(json.Delim)
		if !ok {
			continue
		}
		if delim == fromDelim {
			depth += 1
			continue
		}
		if delim == toDelim {
			depth -= 1
			if depth == 0 {
				return nil
			}
		}
	}
}

func (r *FeatureReader) featureFromCoordinates(geometryType string, coordinatesJSON json.RawMessage) (*geo.Feature, error) {
	prefix := []byte(`{"type":"` + geometryType + `","coordinates":`)
	geometryData := append(prefix, coordinatesJSON...)
	geometryData = append(geometryData, "}"...)
	geometry := &orbjson.Geometry{}
	if err := json.Unmarshal(geometryData, geometry); err != nil {
		return nil, fmt.Errorf("trouble parsing geometry coordinates: %w", err)
	}
	feature := &geo.Feature{
		Geometry:   geometry.Geometry(),
		Properties: map[string]any{},
	}
	return feature, nil
}

func (r *FeatureReader) readFeature() (*geo.Feature, error) {
	if !r.decoder.More() {
		r.decoder = nil
		return nil, io.EOF
	}
	feature := &geo.Feature{}
	if err := r.decoder.Decode(feature); err != nil {
		return nil, err
	}
	return feature, nil
}

func (r *FeatureReader) readGeometryCollection() (*geo.Feature, error) {
	feature := &geo.Feature{Properties: map[string]any{}}

	if !r.decoder.More() {
		return feature, nil
	}

	geometries := []orb.Geometry{}
	for r.decoder.More() {
		geometry := &orbjson.Geometry{}
		if err := r.decoder.Decode(geometry); err != nil {
			return nil, fmt.Errorf("trouble parsing geometry: %w", err)
		}
		geometries = append(geometries, geometry.Geometry())
	}

	feature.Geometry = orb.Collection(geometries)
	return feature, nil
}
