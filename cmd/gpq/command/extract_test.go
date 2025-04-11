package command_test

import (
	"bytes"

	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/planetlabs/gpq/cmd/gpq/command"
	"github.com/planetlabs/gpq/internal/geoparquet"
)

func (s *Suite) TestExtractDropCols() {
	cmd := &command.ExtractCmd{
		Input:    "../../../internal/testdata/cases/example-v1.0.0.parquet",
		DropCols: "pop_est,iso_a3",
	}
	s.Require().NoError(cmd.Run())

	data := s.readStdout()

	fileReader, err := file.NewParquetReader(bytes.NewReader(data))
	s.Require().NoError(err)
	defer fileReader.Close()

	s.Equal(int64(5), fileReader.NumRows())

	s.Require().NoError(err)
	s.Equal(4, fileReader.MetaData().Schema.NumColumns())

	recordReader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		Reader: bytes.NewReader(data),
	})
	s.Require().NoError(err)
	defer recordReader.Close()

	record, readErr := recordReader.Read()
	s.Require().NoError(readErr)
	s.Assert().Equal(int64(4), record.NumCols())
}

func (s *Suite) TestExtractKeepOnlyCols() {
	cmd := &command.ExtractCmd{
		Input:        "../../../internal/testdata/cases/example-v1.1.0.parquet",
		KeepOnlyCols: "geometry,pop_est,iso_a3",
	}
	s.Require().NoError(cmd.Run())

	data := s.readStdout()

	fileReader, err := file.NewParquetReader(bytes.NewReader(data))
	s.Require().NoError(err)
	defer fileReader.Close()

	s.Equal(int64(5), fileReader.NumRows())

	s.Require().NoError(err)
	s.Equal(3, fileReader.MetaData().Schema.NumColumns())

	recordReader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		Reader: bytes.NewReader(data),
	})
	s.Require().NoError(err)
	defer recordReader.Close()

	record, readErr := recordReader.Read()
	s.Require().NoError(readErr)
	s.Assert().Equal(int64(3), record.NumCols())
}

// Since the 1.1.0 parquet file includes a bbox column, we expect the bbox column to be used for spatial filtering.
func (s *Suite) TestExtractBbox110() {
	cmd := &command.ExtractCmd{
		Input: "../../../internal/testdata/cases/example-v1.1.0.parquet",
		Bbox:  "34,-7,36,-6",
	}
	s.Require().NoError(cmd.Run())

	data := s.readStdout()

	recordReader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		Reader: bytes.NewReader(data),
	})
	s.Require().NoError(err)
	defer recordReader.Close()

	// we expect only one row, namely Tanzania
	s.Require().Equal(int64(1), recordReader.NumRows())

	record, readErr := recordReader.Read()
	s.Require().NoError(readErr)
	s.Assert().Equal(int64(7), record.NumCols())
	s.Assert().Equal(int64(1), record.NumRows())

	country := record.Column(recordReader.Schema().ColumnIndexByName("name")).ValueStr(0)
	s.Assert().Equal("Tanzania", country)
}

// Since the 1.1.0 parquet file includes a bbox column and is partitioned into spatially ordered row groups,
// we expect the bbox column row group statistic to be used for spatial pushdown filtering.
func (s *Suite) TestExtractBbox110Partitioned() {
	cmd := &command.ExtractCmd{
		Input: "../../../internal/testdata/cases/example-v1.1.0-partitioned.parquet",
		Bbox:  "34,-7,36,-6",
	}
	s.Require().NoError(cmd.Run())

	data := s.readStdout()

	recordReader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		Reader: bytes.NewReader(data),
	})
	s.Require().NoError(err)
	defer recordReader.Close()

	// we expect only one row, namely Tanzania
	s.Require().Equal(int64(1), recordReader.NumRows())

	record, readErr := recordReader.Read()
	s.Require().NoError(readErr)
	s.Assert().Equal(int64(8), record.NumCols())
	s.Assert().Equal(int64(1), record.NumRows())

	country := record.Column(recordReader.Schema().ColumnIndexByName("name")).ValueStr(0)
	s.Assert().Equal("Tanzania", country)
}

// Since the 1.0.0 parquet file doesn't have a bbox column, we expect the bbox column to be calculated on the fly.
func (s *Suite) TestExtractBbox100() {
	cmd := &command.ExtractCmd{
		Input: "../../../internal/testdata/cases/example-v1.0.0.parquet",
		Bbox:  "34,-7,36,-6",
	}
	s.Require().NoError(cmd.Run())

	data := s.readStdout()

	recordReader, err := geoparquet.NewRecordReaderFromConfig(&geoparquet.ReaderConfig{
		Reader: bytes.NewReader(data),
	})
	s.Require().NoError(err)
	defer recordReader.Close()

	// we expect only one row, namely Tanzania
	s.Require().Equal(int64(1), recordReader.NumRows())

	record, readErr := recordReader.Read()
	s.Require().NoError(readErr)
	s.Assert().Equal(int64(6), record.NumCols())
	s.Assert().Equal(int64(1), record.NumRows())

	country := record.Column(recordReader.Schema().ColumnIndexByName("name")).ValueStr(0)
	s.Assert().Equal("Tanzania", country)
}
