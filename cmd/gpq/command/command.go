package command

import (
	"fmt"
	"io"
)

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

type CommandError struct {
	err error
}

func NewCommandError(format string, a ...any) *CommandError {
	return &CommandError{err: fmt.Errorf(format, a...)}
}

func (e *CommandError) Error() string {
	return e.err.Error()
}

func (e *CommandError) Unwrap() error {
	return e.err
}
