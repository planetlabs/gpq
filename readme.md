# gpq

A utility for working with [GeoParquet](https://github.com/opengeospatial/geoparquet).

## Installation

The `gpq` program can be installed by downloading one of the archives from [the latest release](https://github.com/planetlabs/gpq/releases).

Extract the archive and place the `gpq` executable somewhere on your path.  See a list of available commands by running `gpq` in your terminal.

For Homebrew users, you can install `gpq` from the [Planet tap](https://github.com/planetlabs/homebrew-tap):

```shell
# run `brew update` first if you have used this tap previously and want the latest formula
brew install planetlabs/tap/gpq
```

## WebAssembly

In addition to the CLI program, the `gpq` utility is built as a WebAssembly binary.  The WASM build can be downloaded from [the latest release](https://github.com/planetlabs/gpq/releases).

To give it a try without downloading or installing anything, see https://planetlabs.github.io/gpq/.

## Command Line Utility

The `gpq` program can be used to validate GeoParquet files and to convert to and from GeoJSON.

```shell
# see the available commands
gpq --help
```

### validate

The `validate` command generates a validation report for a GeoParquet file.

```shell
gpq validate example.parquet
```

By default, the command writes out a text report with a list of status checks.  The command exits with status code 1 if one or more of the checks does not pass.

The validation includes scanning the data to ensure that values in geometry columns conform with the specification (making assertions about the encoding, ring orientation, bounding box, and alignment with other metadata).  For very large GeoParquet files, the rules that scan the geometry data can be skipped with the `--metadata-only` argument.  With this argument, the command only runs rules related to the file metadata and Parquet schema.

To generate a JSON report instead of the text report, use the `--format json` argument.

See `gpq validate --help` for the full list of options.

### convert

The `convert` command can convert a GeoJSON file to GeoParquet or a GeoParquet file to GeoJSON.

```shell
# read geojson and write geoparquet
gpq convert example.geojson example.parquet
```

```shell
# read geoparquet and write geojson
gpq convert example.parquet example.geojson
```

The `convert` command can also be used to convert an input Parquet file without "geo" metadata to a valid GeoParquet file.

```shell
# read parquet and write geoparquet
gpq convert non-geo.parquet valid-geo.parquet
```

When reading from a Parquet file and writing out GeoParquet, the input geometry values can be WKB or WKT encoded.  The output geometry values will always be WKB encoded.

The `--input-primary-column` argument can be used to provide a primary geometry column name when reading Parquet files without "geo" metadata (defaults to `geometry`).

The `--compression` argument can be used to control the compression codec used when writing GeoParquet.  See `gpq convert --help` for the available options.


### describe

The `describe` command prints schema information and metadata about a GeoParquet file.

```shell
gpq describe example.parquet
```

## Limitations

 * Non-geographic CRS information is not preserved when converting GeoParquet to GeoJSON.
 * Page and row group size is not configurable when writing GeoParquet.  This may change soon.
 * Feature identifiers in GeoJSON are not written to GeoParquet columns.  This may change soon.
