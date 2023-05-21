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

package main

import (
	"github.com/alecthomas/kong"
)

var CLI struct {
	Convert  ConvertCmd  `cmd:"" help:"Convert data from one format to another."`
	Validate ValidateCmd `cmd:"" help:"Validate a GeoParquet file."`
	Describe DescribeCmd `cmd:"" help:"Describe a GeoParquet file."`
	Version  VersionCmd  `cmd:"" help:"Print the version of this program."`
}

func main() {
	ctx := kong.Parse(&CLI)
	err := ctx.Run(ctx)
	ctx.FatalIfErrorf(err)
}
