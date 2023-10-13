package command_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type Suite struct {
	suite.Suite
	originalStdin  *os.File
	mockStdin      *os.File
	originalStdout *os.File
	mockStdout     *os.File
	server         *httptest.Server
}

func (s *Suite) SetupTest() {
	stdin, err := os.CreateTemp("", "stdin")
	s.Require().NoError(err)
	s.originalStdin = os.Stdin
	s.mockStdin = stdin
	os.Stdin = stdin

	stdout, err := os.CreateTemp("", "stdout")
	s.Require().NoError(err)
	s.originalStdout = os.Stdout
	s.mockStdout = stdout
	os.Stdout = stdout

	handler := http.FileServer(http.Dir("../../../internal"))
	s.server = httptest.NewServer(handler)
}

func (s *Suite) writeStdin(data []byte) {
	_, writeErr := s.mockStdin.Write(data)
	s.Require().NoError(writeErr)
	_, seekErr := s.mockStdin.Seek(0, 0)
	s.Require().NoError(seekErr)
}

func (s *Suite) readStdout() []byte {
	if _, seekErr := s.mockStdout.Seek(0, 0); seekErr != nil {
		// assume the file is closed
		stdout, err := os.Open(s.mockStdout.Name())
		s.Require().NoError(err)
		s.mockStdout = stdout
	}
	data, err := io.ReadAll(s.mockStdout)
	s.Require().NoError(err)
	return data
}

func (s *Suite) TearDownTest() {
	os.Stdout = s.originalStdout
	os.Stdin = s.originalStdin

	_ = s.mockStdin.Close()
	s.NoError(os.Remove(s.mockStdin.Name()))

	_ = s.mockStdout.Close()
	s.NoError(os.Remove(s.mockStdout.Name()))

	s.server.Close()
}

func TestSuite(t *testing.T) {
	suite.Run(t, &Suite{})
}
