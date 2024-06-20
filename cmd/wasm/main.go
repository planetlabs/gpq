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

	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/planetlabs/gpq/internal/geojson"
	"github.com/planetlabs/gpq/internal/geoparquet"
	"github.com/planetlabs/gpq/internal/pqutil"
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

	output := &bytes.Buffer{}
	convertErr := geojson.FromParquet(bytes.NewReader(data), output)
	if convertErr != nil {
		return returnFromError(convertErr)
	}

	reader, readerErr := file.NewParquetReader(bytes.NewReader(data))
	if readerErr != nil {
		return returnFromError(readerErr)
	}
	defer reader.Close()

	metadata, metadataErr := geoparquet.GetMetadataValue(reader.MetaData().KeyValueMetadata())
	if metadataErr != nil {
		return returnFromError(metadataErr)
	}

	return returnFromValue(map[string]any{
		"data":    output.String(),
		"geo":     metadata,
		"schema":  pqutil.ParquetSchemaString(reader.MetaData().Schema),
		"records": reader.NumRows(),
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
	convertErr := geojson.ToParquet(input, output, &geojson.ConvertOptions{
		MinFeatures: 10, MaxFeatures: 250, Compression: "zstd",
	})

	if convertErr != nil {
		return returnFromError(convertErr)
	}

	reader, readerErr := file.NewParquetReader(bytes.NewReader(output.Bytes()))
	if readerErr != nil {
		return returnFromError(readerErr)
	}

	metadata, metadataErr := geoparquet.GetMetadataValue(reader.MetaData().KeyValueMetadata())
	if metadataErr != nil {
		return returnFromError(metadataErr)
	}

	array := uint8ArrayConstructor.New(output.Len())
	js.CopyBytesToJS(array, output.Bytes())

	return returnFromValue(map[string]any{
		"data":    array,
		"geo":     metadata,
		"schema":  pqutil.ParquetSchemaString(reader.MetaData().Schema),
		"records": reader.NumRows(),
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
