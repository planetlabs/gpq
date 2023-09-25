package command

import "io"

var CLI struct {
	Convert  ConvertCmd  `cmd:"" help:"Convert data from one format to another."`
	Validate ValidateCmd `cmd:"" help:"Validate a GeoParquet file."`
	Describe DescribeCmd `cmd:"" help:"Describe a GeoParquet file."`
	Version  VersionCmd  `cmd:"" help:"Print the version of this program."`
}

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}
