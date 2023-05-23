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

import './wasm_exec.js';

/**
 * @typedef {object} GPQ
 * @property {function(string):GeoParquetOutput} toParquet Transform GeoJSON to GeoParquet.
 * @property {function(string):GeoJSONOutput} fromParquet Transform GeoParquet to GeoJSON.
 */

/**
 * @typedef {object} GeoParquetOutput
 * @property {Uint8Array} data The GeoParquet data.
 * @property {string} geo Geo key metadata value.
 * @property {string} schema Parquet schema.
 * @property {number} records The number of rows.
 */

/**
 * @typedef {object} GeoJSONOutput
 * @property {string} data The GeoJSON data.
 * @property {string} geo Geo key metadata value.
 * @property {string} schema Parquet schema.
 * @property {number} records The number of features.
 */

/**
 * @return {Promise<GPQ>} GPQ exports.
 */
export async function getGQP() {
  const go = new Go();
  const result = await WebAssembly.instantiateStreaming(
    fetch(new URL('./gpq.wasm', import.meta.url)),
    go.importObject
  );
  go.run(result.instance);

  const exports = {};
  for (const name in Go.exports) {
    exports[name] = wrapFunction(Go.exports[name]);
  }
  return exports;
}

function unwrapReturn(data) {
  if (!data) {
    throw new Error('Unexpected response, see the console for more detail');
  }
  if (data.error) {
    throw new Error(data.error);
  }
  return data.value;
}

function wrapFunction(fn) {
  return function (...args) {
    return unwrapReturn(fn(...args));
  };
}
