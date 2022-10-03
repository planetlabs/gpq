// Copyright 2023 Planet Labs PBC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package geojson

import (
	"github.com/planetlabs/gpq/internal/geoparquet"
)

const primaryColumn = "geometry"

func GetDefaultMetadata() *geoparquet.GeoMetadata {
	return &geoparquet.GeoMetadata{
		Version:       geoparquet.Version,
		PrimaryColumn: primaryColumn,
		Columns: map[string]*geoparquet.GeometryColumn{
			primaryColumn: {
				Encoding:      "WKB",
				GeometryTypes: []string{},
			},
		},
	}
}
