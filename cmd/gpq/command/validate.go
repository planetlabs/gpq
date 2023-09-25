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

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/fatih/color"
	"github.com/planetlabs/gpq/internal/validator"
)

type ValidateCmd struct {
	Input        string `arg:"" optional:"" name:"input" help:"Path to a GeoParquet file.  If not provided, input is read from stdin." type:"existingfile"`
	MetadataOnly bool   `help:"Only run rules that apply to file metadata and schema (no data will be scanned)."`
	Unpretty     bool   `help:"No colors in text output, no newlines and indentation in JSON output."`
	Format       string `help:"Report format.  Possible values: ${enum}." enum:"text, json" default:"text"`
}

func (c *ValidateCmd) Run(ctx *kong.Context) error {
	inputName := c.Input
	var input ReaderAtSeeker
	if c.Input == "" {
		if !hasStdin() {
			return fmt.Errorf("input argument must be provided if there is no stdin data")
		}
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("trouble reading from stdin: %w", err)
		}
		input = bytes.NewReader(data)
		inputName = "<stdin>"
	} else {
		i, readErr := os.Open(c.Input)
		if readErr != nil {
			return fmt.Errorf("failed to read from %q: %w", c.Input, readErr)
		}
		defer i.Close()
		input = i
	}

	v := validator.New(c.MetadataOnly)
	report, err := v.Validate(context.Background(), input, inputName)
	if err != nil {
		return err
	}

	valid := true
	for _, check := range report.Checks {
		if !check.Passed {
			valid = false
			break
		}
	}

	if c.Format == "json" {
		if err := c.formatJSON(report); err != nil {
			return err
		}
	} else {
		if err := c.formatText(report); err != nil {
			return err
		}
	}

	if !valid {
		ctx.Kong.Exit(1)
	}
	return nil
}

func (c *ValidateCmd) formatJSON(report *validator.Report) error {
	encoder := json.NewEncoder(os.Stdout)
	if !c.Unpretty {
		encoder.SetIndent("", "  ")
		encoder.SetEscapeHTML(false)
	}

	return encoder.Encode(report)
}

func (c *ValidateCmd) formatText(report *validator.Report) error {
	passed := 0
	failed := 0
	unrun := 0
	for _, check := range report.Checks {
		if !check.Run {
			unrun++
		} else if check.Passed {
			passed++
		} else {
			failed++
		}
	}

	summaries := []string{
		fmt.Sprintf("Passed %d check%s", passed, maybeS(passed)),
	}
	if failed > 0 {
		summaries = append(summaries, fmt.Sprintf("failed %d check%s", failed, maybeS(failed)))
	}
	if unrun > 0 {
		summaries = append(summaries, fmt.Sprintf("%d check%s not run", unrun, maybeS(unrun)))
	}

	if c.Unpretty {
		color.NoColor = true
	}

	fmt.Printf("\nSummary: %s.\n\n", strings.Join(summaries, ", "))
	if report.MetadataOnly {
		skipped := len(validator.DataScanningRules())
		color.Yellow("Metadata and schema checks only.  Skipped %d data scanning check%s.\n\n", skipped, maybeS(skipped))
	}

	passPrefix := " ✓"
	failPrefix := " ✗"
	unrunPrefix := " !"
	reasonPrefix := "   ↳"
	for _, check := range report.Checks {
		if !check.Run {
			color.Yellow("%s %s", unrunPrefix, check.Title)
			color.Yellow("%s %s", reasonPrefix, "not checked")
			continue
		}

		if check.Passed {
			color.Green("%s %s", passPrefix, check.Title)
			continue
		}

		color.Red("%s %s", failPrefix, check.Title)
		color.Red("%s %s", reasonPrefix, check.Message)
	}
	fmt.Println()

	return nil
}

func maybeS(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}
