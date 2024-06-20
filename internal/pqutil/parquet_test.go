package pqutil_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/planetlabs/gpq/internal/pqutil"
	"github.com/planetlabs/gpq/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowToParquetString(t *testing.T) {
	cases := []struct {
		name     string
		schema   *arrow.Schema
		expected string
	}{
		{
			name: "basic",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "optional_bytes", Type: arrow.BinaryTypes.Binary, Nullable: true},
				{Name: "optional_float32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
				{Name: "optional_float64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				{Name: "optional_int32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: "optional_int64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "optional_bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				{Name: "required_bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
				{Name: "optional_string", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "required_fixed_binary", Type: &arrow.FixedSizeBinaryType{ByteWidth: 3}, Nullable: false},
			}, nil),
			expected: `
				message {
					optional binary optional_bytes;
					optional float optional_float32;
					optional double optional_float64;
					optional int32 optional_int32 (INT (32, true));
					optional int64 optional_int64 (INT (64, true));
					optional boolean optional_bool;
					required boolean required_bool;
					optional binary optional_string (STRING);
					required fixed_len_byte_array (24) required_fixed_binary;
				}
			`,
		},
		{
			name: "lists",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "optional_bools", Type: arrow.ListOf(arrow.FixedWidthTypes.Boolean), Nullable: true},
				{Name: "required_nullable_strings", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
			}, nil),
			expected: `
				message {
					optional group optional_bools (LIST) {
						repeated group list {
							optional boolean element;
						}
					}
					required group required_nullable_strings (LIST) {
						repeated group list {
							optional binary element (STRING);
						}
					}
				}
			`,
		},
		{
			name: "TODO: ticket this issue with non-nullable list items",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "optional_nonnullable_bools", Type: arrow.ListOfNonNullable(arrow.FixedWidthTypes.Boolean), Nullable: false},
			}, nil),
			expected: `
				message {
					required group optional_nonnullable_bools (LIST) {
						repeated group list {
							optional boolean element;
						}
					}
				}
			`,
		},
		{
			name: "structs",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "soup", Type: arrow.StructOf(
					arrow.Field{Name: "good", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
					arrow.Field{Name: "helpings", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				), Nullable: false},
			}, nil),
			expected: `
				message {
					required group soup {
						required boolean good;
						optional double helpings;
					}
				}
			`,
		},
		{
			name: "lists of structs",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "things", Type: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
					arrow.Field{Name: "cost", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				)), Nullable: true},
			}, nil),
			expected: `
				message {
					optional group things (LIST) {
						repeated group list {
							optional group element {
								required binary name (STRING);
								optional double cost;
							}
						}
					}
				}
			`,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%s (case %d)", c.name, i), func(t *testing.T) {
			parquetSchema, err := pqarrow.ToParquet(c.schema, nil, pqarrow.DefaultWriterProps())
			require.NoError(t, err)

			assert.Equal(t, test.Tab2Space(test.Dedent(c.expected)), pqutil.ParquetSchemaString(parquetSchema))
		})
	}
}
