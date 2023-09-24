package pqutil_test

import (
	"fmt"
	"testing"

	"github.com/planetlabs/gpq/internal/pqutil"
	"github.com/planetlabs/gpq/internal/test"
	"github.com/stretchr/testify/require"
)

func TestBuilder(t *testing.T) {
	cases := []struct {
		name   string
		record map[string]any
		schema string
	}{
		{
			name: "flat map",
			record: map[string]any{
				"maybe":  true,
				"answer": 42,
				"small":  int32(32),
				"pi":     4.13,
				"data":   []byte{'a', 'b', 'c'},
				"good":   "yup",
			},
			schema: `
				message {
					optional int64 answer (INT (64, true));
					optional binary data;
					optional binary good (STRING);
					optional boolean maybe;
					optional double pi;
					optional int32 small (INT (32, true));
				}
			`,
		},
		{
			name: "with slices",
			record: map[string]any{
				"bools":   []any{true, false, true},
				"strings": []any{"chicken", "noodle", "soup"},
				"floats":  []any{1.23, 4.56, 7.89},
				"ints":    []any{3, 2, 1},
			},
			schema: `
				message {
					optional group bools (LIST) {
						repeated group list {
							optional boolean element;
						}
					}
					optional group floats (LIST) {
						repeated group list {
							optional double element;
						}
					}
					optional group ints (LIST) {
						repeated group list {
							optional int64 element (INT (64, true));
						}
					}
					optional group strings (LIST) {
						repeated group list {
							optional binary element (STRING);
						}
					}
				}
			`,
		},
		{
			name: "with maps",
			record: map[string]any{
				"complex": map[string]any{
					"maybe":  true,
					"answer": 42,
					"small":  int32(32),
					"pi":     4.13,
					"data":   []byte{'a', 'b', 'c'},
					"good":   "yup",
				},
			},
			schema: `
				message {
					optional group complex {
						optional int64 answer (INT (64, true));
						optional binary data;
						optional binary good (STRING);
						optional boolean maybe;
						optional double pi;
						optional int32 small (INT (32, true));
					}
				}
			`,
		},
		{
			name: "with slices of maps",
			record: map[string]any{
				"things": []any{
					map[string]any{
						"what": "soup",
						"cost": 1.00,
					},
					map[string]any{
						"what": "car",
						"cost": 40000.00,
					},
					map[string]any{
						"what": "house",
						"cost": 1000000.00,
					},
				},
			},
			schema: `
				message {
					optional group things (LIST) {
						repeated group list {
							optional group element {
								optional double cost;
								optional binary what (STRING);
							}
						}
					}
				}
			`,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%s (case %d)", c.name, i), func(t *testing.T) {
			b := pqutil.NewArrowSchemaBuilder()
			require.NoError(t, b.Add(c.record))
			s, err := b.Schema()
			require.NoError(t, err)
			require.NotNil(t, s)
			test.AssertArrowSchemaMatches(t, c.schema, s)
		})
	}
}
