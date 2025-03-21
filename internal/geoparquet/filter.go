package geoparquet

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/compute"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/metadata"
	"github.com/planetlabs/gpq/internal/geo"
)

// PROJECTION PUSHDOWN - COLUMN FILTERING UTILS

// A Set type based on map, to hold arrow column indices.
// Implements common Set methods such as Difference() and Contains().
// To instantiate, use the constructor newIndicesSet() followed by either
// Add() if you want to build the Set sequentially or the convenience function
// FromColNames().
type indicesSet map[int]struct{}

func newIndicesSet(size int) *indicesSet {
	var s indicesSet = make(map[int]struct{}, size)
	return &s
}

func (s *indicesSet) Add(col int) *indicesSet {
	(*s)[col] = struct{}{}
	return s
}

func (s *indicesSet) FromColNames(cols []string, schema *arrow.Schema) *indicesSet {
	for _, col := range cols {
		if indicesForColumn := schema.FieldIndices(col); indicesForColumn != nil {
			for _, colIdx := range indicesForColumn {
				s.Add(colIdx)
			}
		}
	}
	return s
}

func (s *indicesSet) Contains(col int) bool {
	_, ok := (*s)[col]
	return ok
}

func (s *indicesSet) Difference(other *indicesSet) *indicesSet {
	sSize := s.Size()
	otherSize := s.Size()
	var newSet *indicesSet
	if sSize < otherSize {
		newSet = newIndicesSet(otherSize - sSize)
	} else {
		newSet = newIndicesSet(sSize - otherSize)
	}
	for key := range *s {
		if !other.Contains(key) {
			newSet.Add(key)
		}
	}
	return newSet
}

func (s *indicesSet) Size() int {
	return len(*s)
}

func (s *indicesSet) List() []int {
	keys := make([]int, 0, len(*s))
	for k := range *s {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Given a list of columns names to include, return the corresponding columns indices.
func GetColumnIndices(includeColumns []string, arrowSchema *arrow.Schema) ([]int, error) {
	// generate indices from col names
	indices := newIndicesSet(len(includeColumns)).FromColNames(includeColumns, arrowSchema).List()

	return indices, nil
}

// Given a list of column names to exclude, return a list of the remaining columns indices.
func GetColumnIndicesByDifference(excludeColumns []string, arrowSchema *arrow.Schema) ([]int, error) {
	// generate indices from col names and compute the indices to include
	indicesToExclude := newIndicesSet(arrowSchema.NumFields()-len(excludeColumns)).FromColNames(excludeColumns, arrowSchema)
	allIndices := newIndicesSet(arrowSchema.NumFields())
	for i := 0; i < arrowSchema.NumFields(); i++ {
		allIndices.Add(i)
	}
	return allIndices.Difference(indicesToExclude).List(), nil
}

// PREDICATE PUSHDOWN - ROW FILTERING UTILS

type rowGroupIntersectionResult struct {
	Index      int
	Intersects bool
	Error      error
}

// Get row group indices that intersect with the input bbox. Uses the bbox column row group
// stats to calculate intersection.
func GetRowGroupsByBbox(fileReader *file.Reader, bboxCol *BboxColumn, inputBbox *geo.Bbox) ([]int, error) {
	numRowGroups := fileReader.NumRowGroups()
	intersectingRowGroups := make([]int, 0, numRowGroups)

	// process row groups concurrently
	queue := make(chan *rowGroupIntersectionResult)
	for i := 0; i < numRowGroups; i += 1 {
		go func(i int) {
			result := &rowGroupIntersectionResult{Index: i}
			result.Intersects, result.Error = RowGroupIntersects(fileReader.MetaData(), bboxCol, i, inputBbox)
			queue <- result
		}(i)
	}

	// read goroutine results
	for i := 0; i < numRowGroups; i += 1 {
		res := <-queue
		if res.Error != nil {
			return intersectingRowGroups, res.Error
		}
		if res.Intersects {
			intersectingRowGroups = append(intersectingRowGroups, res.Index)
		}
	}
	slices.Sort(intersectingRowGroups)
	return intersectingRowGroups, nil
}

// Return min/max statistics for a given column and RowGroup.
// For nested structures, use `<column>.<field>`.
func GetColumnMinMax(fileMetadata *metadata.FileMetaData, rowGroup int, columnPath string) (min float64, max float64, err error) {
	rowGroupMetadata := fileMetadata.RowGroup(rowGroup)
	if rowGroupMetadata == nil {
		return 0, 0, fmt.Errorf("metadata for RowGroup %v is nil", rowGroup)
	}

	rowGroupSchema := rowGroupMetadata.Schema
	if rowGroupSchema == nil {
		return 0, 0, fmt.Errorf("schema for RowGroup %v is nil", rowGroup)
	}

	columnIdx := rowGroupSchema.ColumnIndexByName(columnPath)
	if columnIdx == -1 {
		return 0, 0, fmt.Errorf("column %v not found", columnPath)
	}

	fieldMetadata, err := rowGroupMetadata.ColumnChunk(columnIdx)
	if err != nil {
		return 0, 0, fmt.Errorf("couldn't get ColumnChunkMetadata for RowGroup %v/Column %v: %w", rowGroup, columnPath, err)
	}
	fieldStats, err := fieldMetadata.Statistics()
	if err != nil {
		return 0, 0, fmt.Errorf("couldn't get ColumnChunkMetadata stats: %w", err)
	}
	if !fieldStats.HasMinMax() {
		return 0, 0, fmt.Errorf("no min/max statistics available for ")
	}

	bitsMin := binary.LittleEndian.Uint64(fieldStats.EncodeMin())
	min = math.Float64frombits(bitsMin)
	bitsMax := binary.LittleEndian.Uint64(fieldStats.EncodeMax())
	max = math.Float64frombits(bitsMax)

	return min, max, nil
}

// Check whether the bbox features in a row group intersect with the input bbox, based on the row group min/max stats.
func RowGroupIntersects(fileMetadata *metadata.FileMetaData, bboxCol *BboxColumn, rowGroup int, inputBbox *geo.Bbox) (bool, error) {
	if bboxCol.Name == "" {
		return false, errors.New("name field of bbox column struct is empty")
	}
	xminPath := fmt.Sprintf("%v.%v", bboxCol.Name, bboxCol.Xmin)
	xmin, _, err := GetColumnMinMax(fileMetadata, rowGroup, xminPath)
	if err != nil {
		return false, fmt.Errorf("could not get min/max statistics for %v: %w", xminPath, err)
	}

	yminPath := fmt.Sprintf("%v.%v", bboxCol.Name, bboxCol.Ymin)
	ymin, _, err := GetColumnMinMax(fileMetadata, rowGroup, yminPath)
	if err != nil {
		return false, fmt.Errorf("could not get min/max statistics for %v: %w", yminPath, err)
	}

	xmaxPath := fmt.Sprintf("%v.%v", bboxCol.Name, bboxCol.Xmax)
	_, xmax, err := GetColumnMinMax(fileMetadata, rowGroup, xmaxPath)
	if err != nil {
		return false, fmt.Errorf("could not get min/max statistics for %v: %w", xmaxPath, err)
	}

	ymaxPath := fmt.Sprintf("%v.%v", bboxCol.Name, bboxCol.Ymax)
	_, ymax, err := GetColumnMinMax(fileMetadata, rowGroup, ymaxPath)
	if err != nil {
		return false, fmt.Errorf("could not get min/max statistics for %v: %w", ymaxPath, err)
	}

	rowGroupBbox := &geo.Bbox{Xmin: xmin, Ymin: ymin, Xmax: xmax, Ymax: ymax}
	return rowGroupBbox.Intersects(inputBbox), nil
}

func filterRecord(ctx context.Context, record *arrow.Record, predicate func(int64) (bool, error)) (*arrow.Record, error) {
	// we build a boolean mask and pass it to compute.FilterRecordBatch later
	maskBuilder := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer maskBuilder.Release()

	// loop over individual bbox values per record
	for idx := int64(0); idx < (*record).NumRows(); idx++ {
		p, err := predicate(idx)
		if err != nil {
			return nil, err
		}
		maskBuilder.Append(p)
	}

	r, filterErr := compute.FilterRecordBatch(ctx, *record, maskBuilder.NewBooleanArray(), &compute.FilterOptions{NullSelection: 0}) // TODO check what this is doing
	if filterErr != nil {
		return nil, fmt.Errorf("trouble filtering record batch: %w", filterErr)
	}
	return &r, nil
}

// Filter rows in an arrow.Record by intersection of the feature bounding boxes with an input bbox.
// If there is a bbox column, it will be used to compute intersection. If not, the bbox will be computed
// on the fly.
func FilterRecordBatchByBbox(ctx context.Context, record *arrow.Record, inputBbox *geo.Bbox, bboxCol *BboxColumn) (*arrow.Record, error) {
	var filteredRecord *arrow.Record
	var filterErr error

	if inputBbox != nil && bboxCol.Index != -1 { // bbox argument has been provided and there is a bbox column we can use for filtering
		col := (*record).Column(bboxCol.Index).(*array.Struct)
		defer col.Release()

		filteredRecord, filterErr = filterRecord(ctx, record, func(idx int64) (bool, error) {
			var bbox map[string]json.RawMessage
			if err := json.Unmarshal([]byte(col.ValueStr(int(idx))), &bbox); err != nil {
				return false, fmt.Errorf("trouble unmarshalling bbox struct: %w", err)
			}

			bboxValue := &geo.Bbox{} // create empty struct to hold bbox values of this row

			if err := json.Unmarshal(bbox[bboxCol.Xmin], &bboxValue.Xmin); err != nil {
				return false, fmt.Errorf("trouble parsing bbox.%v field: %w", bboxCol.Xmin, err)
			}
			if err := json.Unmarshal(bbox[bboxCol.Ymin], &bboxValue.Ymin); err != nil {
				return false, fmt.Errorf("trouble parsing bbox.%v field: %w", bboxCol.Ymin, err)
			}
			if err := json.Unmarshal(bbox[bboxCol.Xmax], &bboxValue.Xmax); err != nil {
				return false, fmt.Errorf("trouble parsing bbox.%v field: %w", bboxCol.Xmax, err)
			}
			if err := json.Unmarshal(bbox[bboxCol.Ymax], &bboxValue.Ymax); err != nil {
				return false, fmt.Errorf("trouble parsing bbox.%v field: %w", bboxCol.Ymax, err)
			}

			// check whether the bbox passed to this function
			// intersects with the bbox of the record
			return inputBbox.Intersects(bboxValue), nil
		})
	} else if inputBbox != nil && bboxCol.Index == -1 {
		// bbox filter passed to function but there is no bbox col.
		// this means we have to compute the bboxes of the rows ourselves
		primaryColIdx := bboxCol.BaseColumn
		col := (*record).Column(primaryColIdx)
		defer col.Release()

		filteredRecord, filterErr = filterRecord(ctx, record, func(idx int64) (bool, error) {
			value := col.GetOneForMarshal(int(idx))
			g, decodeErr := geo.DecodeGeometry(value, bboxCol.BaseColumnEncoding)
			if decodeErr != nil {
				return false, fmt.Errorf("trouble decoding geometry: %w", decodeErr)
			}
			bounds := g.Coordinates.Bound()
			bboxValue := &geo.Bbox{
				Xmin: bounds.Min.X(),
				Ymin: bounds.Min.Y(),
				Xmax: bounds.Max.X(),
				Ymax: bounds.Max.Y(),
			}
			// now that we've computed the bbox, same logic as above
			return inputBbox.Intersects(bboxValue), nil
		})
	} else {
		filteredRecord = record
	}
	return filteredRecord, filterErr
}
