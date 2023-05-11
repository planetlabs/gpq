//go:build js && wasm

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
	"bytes"
	"strings"
	"syscall/js"

	"github.com/planetlabs/gpq/internal/geojson"
	"github.com/segmentio/parquet-go"
)

var uint8ArrayConstructor = js.Global().Get("Uint8Array")

const (
	errorKey = "error"
	valueKey = "value"
)

func returnFromErrorMessage(message string) map[string]any {
	return map[string]any{errorKey: message}
}

func returnFromError(err error) map[string]any {
	return returnFromErrorMessage(err.Error())
}

func returnFromValue(value any) map[string]any {
	return map[string]any{valueKey: value}
}

var fromParquet = js.FuncOf(func(this js.Value, args []js.Value) any {
	if len(args) != 1 {
		return returnFromErrorMessage("Must be called with a single argument")
	}
	if !args[0].InstanceOf(uint8ArrayConstructor) {
		return returnFromErrorMessage("Must be called with a Uint8Array")
	}

	numBytes := args[0].Length()
	data := make([]byte, numBytes)
	js.CopyBytesToGo(data, args[0])

	input, fileErr := parquet.OpenFile(bytes.NewReader(data), int64(numBytes))
	if fileErr != nil {
		return returnFromError(fileErr)
	}

	output := &bytes.Buffer{}
	convertErr := geojson.FromParquet(input, output)
	if convertErr != nil {
		return returnFromError(convertErr)
	}

	metadata, _ := input.Lookup("geo")

	return returnFromValue(map[string]any{
		"data":    output.String(),
		"geo":     metadata,
		"schema":  input.Schema().String(),
		"records": input.NumRows(),
	})
})

var toParquet = js.FuncOf(func(this js.Value, args []js.Value) any {
	if len(args) != 1 {
		return returnFromErrorMessage("Must be called with a single argument")
	}
	if args[0].Type() != js.TypeString {
		return returnFromErrorMessage("Must be called with a string")
	}

	input := strings.NewReader(args[0].String())
	output := &bytes.Buffer{}
	convertErr := geojson.ToParquet(input, output, &geojson.ConvertOptions{MinFeatures: 10, MaxFeatures: 250})

	if convertErr != nil {
		return returnFromError(convertErr)
	}

	file, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		return returnFromError(err)
	}

	metadata, _ := file.Lookup("geo")

	array := uint8ArrayConstructor.New(output.Len())
	js.CopyBytesToJS(array, output.Bytes())

	return returnFromValue(map[string]any{
		"data":    array,
		"geo":     metadata,
		"schema":  file.Schema().String(),
		"records": file.NumRows(),
	})
})

func main() {
	exports := map[string]interface{}{
		"fromParquet": fromParquet,
		"toParquet":   toParquet,
	}
	js.Global().Get("Go").Set("exports", exports)
	<-make(chan struct{})
}
