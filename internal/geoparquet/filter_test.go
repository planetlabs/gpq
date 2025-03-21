package geoparquet_test

import (
	"os"
	"testing"

	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/planetlabs/gpq/internal/geo"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowGroupIntersects(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.1.0-partitioned.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	fileReader, err := file.NewParquetReader(input)
	require.NoError(t, err)

	require.Equal(t, 2, fileReader.NumRowGroups())

	bbox := &geo.Bbox{Xmin: 34.0, Ymin: -7.0, Xmax: 36.0, Ymax: -6.0} // somewhere in tanzania
	geoMetadata, err := geoparquet.GetMetadataFromFileReader(fileReader)
	require.NoError(t, err)

	bboxCol := geoparquet.GetBboxColumn(fileReader.MetaData().Schema, geoMetadata)

	// the file has two row groups - the first one contains all data for the eastern hemisphere,
	// the second for the western hemisphere
	intersectsEasternHemisphere, err := geoparquet.RowGroupIntersects(fileReader.MetaData(), bboxCol, 0, bbox)
	assert.NoError(t, err)
	assert.Equal(t, intersectsEasternHemisphere, true)

	intersectsWesternHemisphere, err := geoparquet.RowGroupIntersects(fileReader.MetaData(), bboxCol, 1, bbox)
	assert.NoError(t, err)
	assert.Equal(t, intersectsWesternHemisphere, false)
}

func TestGetRowGroupsByBbox(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.1.0-partitioned.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	fileReader, err := file.NewParquetReader(input)
	require.NoError(t, err)

	require.Equal(t, 2, fileReader.NumRowGroups())

	bbox := &geo.Bbox{Xmin: 34.0, Ymin: -7.0, Xmax: 36.0, Ymax: -6.0} // somewhere in tanzania
	geoMetadata, err := geoparquet.GetMetadataFromFileReader(fileReader)
	require.NoError(t, err)

	bboxCol := geoparquet.GetBboxColumn(fileReader.MetaData().Schema, geoMetadata)

	// the file has two row groups - the first one contains all data for the eastern hemisphere,
	// the second for the western hemisphere
	rowGroups, err := geoparquet.GetRowGroupsByBbox(fileReader, bboxCol, bbox)
	require.NoError(t, err)

	// only the eastern hemisphere row group matches
	require.Len(t, rowGroups, 1)
	assert.Equal(t, 0, rowGroups[0])
}

func TestGetRowGroupsByBbox2(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.1.0-partitioned.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	fileReader, err := file.NewParquetReader(input)
	require.NoError(t, err)

	require.Equal(t, 2, fileReader.NumRowGroups())

	bbox := &geo.Bbox{Xmin: -92.0, Ymin: 32.0, Xmax: -88.0, Ymax: 35.0} // somewhere in louisiana
	geoMetadata, err := geoparquet.GetMetadataFromFileReader(fileReader)
	require.NoError(t, err)

	bboxCol := geoparquet.GetBboxColumn(fileReader.MetaData().Schema, geoMetadata)

	// the file has two row groups - the first one contains all data for the eastern hemisphere,
	// the second for the western hemisphere
	rowGroups, err := geoparquet.GetRowGroupsByBbox(fileReader, bboxCol, bbox)
	require.NoError(t, err)

	// only the western hemisphere row group matches
	require.Len(t, rowGroups, 1)
	assert.Equal(t, 1, rowGroups[0])
}

func TestGetRowGroupsByBboxErrorNoBboxCol(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.1.0-partitioned.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	fileReader, err := file.NewParquetReader(input)
	require.NoError(t, err)

	require.Equal(t, 2, fileReader.NumRowGroups())

	bbox := &geo.Bbox{Xmin: -92.0, Ymin: 32.0, Xmax: -88.0, Ymax: 35.0} // somewhere in louisiana

	bboxCol := &geoparquet.BboxColumn{} // empty bbox col, will raise error

	// the file has two row groups - the first one contains all data for the eastern hemisphere,
	// the second for the western hemisphere
	rowGroups, err := geoparquet.GetRowGroupsByBbox(fileReader, bboxCol, bbox)
	require.ErrorContains(t, err, "bbox column")
	assert.Empty(t, rowGroups)
}

func TestGetColumnMinMax(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.1.0-partitioned.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	fileReader, err := file.NewParquetReader(input)
	require.NoError(t, err)

	require.Equal(t, 2, fileReader.NumRowGroups())

	xminMin, xminMax, err := geoparquet.GetColumnMinMax(fileReader.MetaData(), 0, "bbox.xmin")
	assert.NoError(t, err)
	assert.Equal(t, 29.339997592900346, xminMin)
	assert.Equal(t, 29.339997592900346, xminMax)

	xmaxMin, xmaxMax, err := geoparquet.GetColumnMinMax(fileReader.MetaData(), 0, "bbox.xmax")
	assert.NoError(t, err)
	assert.Equal(t, 40.31659000000002, xmaxMin)
	assert.Equal(t, 40.31659000000002, xmaxMax)

	xminMin, xminMax, err = geoparquet.GetColumnMinMax(fileReader.MetaData(), 1, "bbox.xmin")
	assert.NoError(t, err)
	assert.Equal(t, -171.79111060289122, xminMin)
	assert.Equal(t, -17.06342322434257, xminMax)

	xmaxMin, xmaxMax, err = geoparquet.GetColumnMinMax(fileReader.MetaData(), 1, "bbox.xmax")
	assert.NoError(t, err)
	assert.Equal(t, -66.96465999999998, xmaxMin)
	assert.Equal(t, -8.665124477564191, xmaxMax)
}

func TestGetColumnIndices(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.1.0.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	fileReader, err := file.NewParquetReader(input)
	require.NoError(t, err)

	arrowReader, err := pqarrow.NewFileReader(fileReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	require.NoError(t, err)
	schema, err := arrowReader.Schema()
	require.NoError(t, err)

	indices, err := geoparquet.GetColumnIndices([]string{"pop_est", "name", "iso_a3"}, schema)
	assert.NoError(t, err)
	assert.Equal(t, []int{0, 2, 3}, indices)
}

func TestGetColumnIndicesByDifference(t *testing.T) {
	fixturePath := "../testdata/cases/example-v1.1.0.parquet"
	input, openErr := os.Open(fixturePath)
	require.NoError(t, openErr)

	fileReader, err := file.NewParquetReader(input)
	require.NoError(t, err)

	arrowReader, err := pqarrow.NewFileReader(fileReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	require.NoError(t, err)
	schema, err := arrowReader.Schema()
	require.NoError(t, err)

	indices, err := geoparquet.GetColumnIndicesByDifference([]string{"pop_est", "name", "iso_a3"}, schema)
	assert.NoError(t, err)
	assert.Equal(t, []int{1, 4, 5, 6}, indices)
}
