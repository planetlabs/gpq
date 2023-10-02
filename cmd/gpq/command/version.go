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

package command

import "fmt"

type VersionCmd struct {
	Detail bool `help:"Include detail about the commit and build date."`
}

type VersionInfo struct {
	Version string
	Commit  string
	Date    string
}

func (c *VersionCmd) Run(info *VersionInfo) error {
	output := info.Version
	if c.Detail {
		output = fmt.Sprintf("%s (%s %s)", output, info.Commit, info.Date)
	}
	fmt.Println(output)
	return nil
}
