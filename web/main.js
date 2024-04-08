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
import {
  LitElement,
  css,
  html,
} from 'https://cdn.jsdelivr.net/gh/lit/dist@2.4.0/core/lit-core.min.js';

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
 * @typedef {object} DownloadInfo
 * @property {File} input The input file.
 * @property {string} name The output name.
 * @property {string} url The output URL.
 * @property {string} summary A summary of the output.
 * @property {string} geo The geo key metadata value.
 * @property {string} schema The Parquet schema.
 */

/**
 * @return {Promise<GPQ>} GPQ exports.
 */
async function getGQP() {
  const go = new Go();
  const result = await WebAssembly.instantiateStreaming(
    fetch(new URL('./gpq.wasm', import.meta.url)),
    go.importObject,
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

/**
 * @param {File} file The input file.
 * @return {Promise<Uint8Array>} The file data.
 */
function getParquetData(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(new Uint8Array(reader.result));
    reader.onerror = () => reject(new Error(`failed to read ${file.name}`));
    reader.readAsArrayBuffer(file);
  });
}

/**
 * @param {File} file The input file.
 * @return {Promise<string>} The file data.
 */
function getJSONData(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(new Error(`failed to read ${file.name}`));
    reader.readAsText(file);
  });
}

/**
 * @param {string} oldName Old file name.
 * @param {string} newExtension New file extension (including '.').
 * @return {string} New file name.
 */
function rename(oldName, newExtension) {
  const oldBase = oldName.substring(0, oldName.lastIndexOf('.'));
  return oldBase + newExtension;
}

/**
 * @param {string} string JSON string.
 * @return {string} Formatted JSON.
 */
function formatJSON(string) {
  return JSON.stringify(JSON.parse(string), null, 2);
}

/**
 * @param {number} bytes The number of bytes.
 * @return {string} The formatted size.
 */
function formatSize(bytes) {
  if (bytes === 0) {
    return 'empty';
  }
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(
    Math.floor(Math.log(bytes) / Math.log(1024)),
    sizes.length - 1,
  );
  if (i === 0) {
    return `${bytes} ${sizes[i]}`;
  }
  return `${(bytes / 1024 ** i).toFixed(1)} ${sizes[i]}`;
}

/**
 * @typedef {object} FileChangeDetail
 * @property {File} file The file.
 */

class Upload extends LitElement {
  static styles = css`
    :host {
      align-self: center;
    }

    *,
    *:before,
    *:after {
      box-sizing: border-box;
    }

    /**
     * WTF, forms?
     * Released under MIT and copyright 2014 Mark Otto.
     * http://wtfforms.com
     */
    .file {
      position: relative;
      display: inline-block;
      cursor: pointer;
      height: 2.5rem;
    }
    .file input {
      min-width: 14rem;
      margin: 0;
      filter: alpha(opacity=0);
      opacity: 0;
    }
    .file-custom {
      position: absolute;
      top: 0;
      right: 0;
      left: 0;
      z-index: 5;
      height: 2.5rem;
      padding: 0.5rem 1rem;
      line-height: 1.5;
      color: #555;
      background-color: #fff;
      border: 0.075rem solid #ddd;
      border-radius: 0.25rem;
      box-shadow: inset 0 0.2rem 0.4rem rgba(0, 0, 0, 0.05);
      -webkit-user-select: none;
      -moz-user-select: none;
      -ms-user-select: none;
      user-select: none;
    }
    .file-custom:after {
      content: 'Choose file...';
    }
    .file-custom:before {
      position: absolute;
      top: -0.075rem;
      right: -0.075rem;
      bottom: -0.075rem;
      z-index: 6;
      display: block;
      content: 'Browse';
      height: 2.5rem;
      padding: 0.5rem 1rem;
      line-height: 1.5;
      color: #555;
      background-color: #eee;
      border: 0.075rem solid #ddd;
      border-radius: 0 0.25rem 0.25rem 0;
    }

    .file input:focus ~ .file-custom {
      box-shadow:
        0 0 0 0.075rem #fff,
        0 0 0 0.2rem #0074d9;
    }
  `;

  constructor() {
    super();
  }

  _handleChange(event) {
    const file = event.target.files[0];
    if (!file) {
      return;
    }

    this.dispatchEvent(new CustomEvent('change', {detail: {file}}));
  }

  render() {
    return html`
      <label class="file">
        <input
          id="upload"
          @change="${this._handleChange}"
          type="file"
          type="file"
          accept=".parquet, .geoparquet, .geojson, .json, application/json"
          aria-label="File upload"
        />
        <span class="file-custom"></span>
      </label>
    `;
  }
}
customElements.define('gpq-upload', Upload);

class Converter extends LitElement {
  static properties = {
    _downloadInfo: {state: true},
    _error: {state: true},
    _working: {state: true},
  };

  static styles = css`
    :host {
      display: flex;
      flex-direction: column;
      min-width: 80%;
    }

    a {
      color: #679;
      text-decoration: none;
    }

    a:hover {
      text-decoration: underline;
    }

    pre {
      background-color: #eee;
      border: 0.075rem solid #ddd;
      border-radius: 0.25rem;
      padding: 1em;
      max-height: 30em;
      overflow: auto;
    }

    code {
      font-family: 'Lucida Console', Courier, monospace;
      font-size: smaller;
    }

    button {
      padding: 0.5rem 1rem;
      font-size: inherit;
      color: #555;
      background-color: #eee;
      border: 0.075rem solid #ddd;
      border-radius: 0.25rem;
      align-self: center;
    }

    .error {
      align-self: center;
      margin-bottom: 2em;
    }

    .status {
      text-align: center;
    }
  `;

  constructor() {
    super();

    /**
     * @type {Promise<GPQ>}
     */
    this._exports = getGQP();

    /**
     * @type {DownloadInfo}
     */
    this._downloadInfo = null;

    /**
     * @type {Error}
     */
    this._error = null;

    /**
     * @type {string}
     */
    this._working = '';
  }

  _handleReset() {
    if (this._downloadInfo) {
      URL.revokeObjectURL(this._downloadInfo.url);
    }
    this._downloadInfo = null;
    this._error = null;
  }

  /**
   * @param {CustomEvent<FileChangeDetail>} event The change event with file detail.
   */
  async _handleChange(event) {
    const {fromParquet, toParquet} = await this._exports;
    if (this._downloadInfo) {
      URL.revokeObjectURL(this._downloadInfo.url);
    }

    const file = event.detail.file;

    /**
     * @type {GeoJSONOutput | GeoParquetOutput}
     */
    let output;

    /**
     * @type {string}
     */
    let extension;

    /**
     * @type {BlobPropertyBag}
     */
    let blobOptions;

    /**
     * @type {string}
     */
    let summary;

    this._working = `Converting ${file.name} (${formatSize(file.size)})...`;
    try {
      if (file.name.endsWith('parquet')) {
        output = fromParquet(await getParquetData(file));
        extension = '.geojson';
        blobOptions = {type: 'application/geo+json'};
        const size = formatSize(output.data.length);
        const plural = output.records === 1 ? '' : 's';
        summary = `${output.records} feature${plural}, ${size}`;
      } else if (file.name.endsWith('json')) {
        output = toParquet(await getJSONData(file));
        extension = '.parquet';
        blobOptions = {type: 'application/octet-stream'};
        const size = formatSize(output.data.length);
        const plural = output.records === 1 ? '' : 's';
        summary = `${output.records} row${plural}, ${size}`;
      } else {
        throw new Error(
          'Only works with .parquet, .geoparquet, .geojson, and .json files',
        );
      }
    } catch (err) {
      this._error = err;
      return;
    } finally {
      this._working = '';
    }

    const blob = new Blob([output.data], blobOptions);
    this._downloadInfo = {
      input: file,
      name: rename(file.name, extension),
      url: URL.createObjectURL(blob),
      geo: formatJSON(output.geo),
      schema: output.schema.replaceAll('\t', '  '),
      summary,
    };
  }

  /**
   * @param {DownloadInfo} info Download info.
   * @return {any} The content to render.
   */
  _renderDownload({url, name, schema, geo, summary}) {
    if (!this._downloadInfo) {
      return null;
    }

    return html`
      <div>
        Parquet schema
        <pre><code>${schema}</code></pre>
      </div>

      <div>
        File "geo" metadata
        <pre><code>${geo}</code></pre>
      </div>

      <p>
        Download <a href="${url}" download="${name}">${name}</a> (${summary}).
      </p>
      <button @click="${this._handleReset}">Reset</button>
    `;
  }

  _renderUpload() {
    return html`<gpq-upload @change="${this._handleChange}"></gpq-upload>`;
  }

  _renderError(err) {
    return html`
      <div class="error">
        <strong>Something went wrong:</strong>
        <em>${err.message}</em>
      </div>
      <button @click="${this._handleReset}">Try Again</button>
    `;
  }

  render() {
    if (this._working) {
      return html`<div class="status"><p>${this._working}</p></div>`;
    }
    if (this._error) {
      return this._renderError(this._error);
    }
    if (!this._downloadInfo) {
      return this._renderUpload();
    }
    return this._renderDownload(this._downloadInfo);
  }
}
customElements.define('gpq-converter', Converter);
