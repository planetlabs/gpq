package command_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"

	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/planetlabs/gpq/cmd/gpq/command"
	"github.com/planetlabs/gpq/internal/geo"
	"github.com/planetlabs/gpq/internal/test"
)

func (s *Suite) TestConvertGeoParquetToGeoJSONStdout() {
	cmd := &command.ConvertCmd{
		From:  "auto",
		Input: "../../../internal/testdata/cases/example-v1.0.0.parquet",
		To:    "geojson",
	}

	s.Require().NoError(cmd.Run())
	data := s.readStdout()

	collection := &geo.FeatureCollection{}
	s.Require().NoError(json.Unmarshal(data, collection))
	s.Len(collection.Features, 5)
}

func (s *Suite) TestConvertGeoJSONToGeoParquetStdout() {
	cmd := &command.ConvertCmd{
		From:  "auto",
		Input: "../../../internal/geojson/testdata/example.geojson",
		To:    "parquet",
	}

	s.Require().NoError(cmd.Run())
	data := s.readStdout()

	fileReader, err := file.NewParquetReader(bytes.NewReader(data))
	s.Require().NoError(err)
	defer func() { _ = fileReader.Close() }()

	s.Equal(int64(5), fileReader.NumRows())
}

func (s *Suite) TestConvertGeoParquetToUnknownStdout() {
	cmd := &command.ConvertCmd{
		From:  "auto",
		Input: "../../../internal/testdata/cases/example-v1.0.0.parquet",
	}

	s.ErrorContains(cmd.Run(), "when writing to stdout, the --to option must be provided")
}

func (s *Suite) TestConvertGeoJSONStdinToGeoParquetStdout() {
	s.writeStdin([]byte(`{
		"type": "FeatureCollection",
		"features": [
			{
				"type": "Feature",
				"properties": {
					"name": "Null Island"
				},
				"geometry": {
					"type": "Point",
					"coordinates": [0, 0]
				}
			}
		]
	}`))

	cmd := &command.ConvertCmd{
		From: "geojson",
		To:   "geoparquet",
	}

	s.Require().NoError(cmd.Run())
	data := s.readStdout()

	fileReader, err := file.NewParquetReader(bytes.NewReader(data))
	s.Require().NoError(err)
	defer func() { _ = fileReader.Close() }()

	s.Equal(int64(1), fileReader.NumRows())
}

func (s *Suite) TestConvertGeoParquetStdinToGeoJSONStdout() {
	s.writeStdin(test.GeoParquetFromJSON(s.T(), `{
		"type": "FeatureCollection",
		"features": [
			{
				"type": "Feature",
				"properties": {
					"name": "Null Island"
				},
				"geometry": {
					"type": "Point",
					"coordinates": [0, 0]
				}
			}
		]
	}`))

	cmd := &command.ConvertCmd{
		From: "geoparquet",
		To:   "geojson",
	}

	s.Require().NoError(cmd.Run())
	data := s.readStdout()

	collection := &geo.FeatureCollection{}
	s.Require().NoError(json.Unmarshal(data, collection))
	s.Len(collection.Features, 1)
}

func (s *Suite) TestConvertUnknownStdinToGeoParquetStdout() {
	cmd := &command.ConvertCmd{
		To: "geoparquet",
	}

	s.ErrorContains(cmd.Run(), "when reading from stdin, the --from option must be provided")
}

func (s *Suite) TestConvertGeoParquetUrlToGeoJSONStdout() {
	cmd := &command.ConvertCmd{
		Input: s.server.URL + "/testdata/cases/example-v1.0.0.parquet?filename=example.geojson#example.geojson",
		To:    "geojson",
	}

	s.Require().NoError(cmd.Run())
	data := s.readStdout()

	collection := &geo.FeatureCollection{}
	s.Require().NoError(json.Unmarshal(data, collection))
	s.Len(collection.Features, 5)
}

func (s *Suite) TestConvertLocalFilenamesWithURLCharacters() {
	data, err := os.ReadFile("../../../internal/geojson/testdata/example.geojson")
	s.Require().NoError(err)
	expected := &geo.FeatureCollection{}
	s.Require().NoError(json.Unmarshal(data, expected))

	for _, name := range []string{"part#1", "part?1", "part%231"} {
		s.Run(name, func() {
			if runtime.GOOS == "windows" && name == "part?1" {
				s.T().Skip("question marks are not allowed in Windows filenames")
			}
			dir := s.T().TempDir()
			input := filepath.Join(dir, name+".geojson")
			parquet := filepath.Join(dir, name+".parquet")
			output := filepath.Join(dir, name+"-roundtrip.geojson")
			s.Require().NoError(os.WriteFile(input, data, 0600))

			toParquet := &command.ConvertCmd{Input: input, Output: parquet}
			s.Require().NoError(toParquet.Run())
			toGeoJSON := &command.ConvertCmd{Input: parquet, Output: output}
			s.Require().NoError(toGeoJSON.Run())

			converted, err := os.ReadFile(output)
			s.Require().NoError(err)
			actual := &geo.FeatureCollection{}
			s.Require().NoError(json.Unmarshal(converted, actual))
			s.Equal(expected, actual)
		})
	}
}
