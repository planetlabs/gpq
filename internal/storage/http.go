package storage

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const (
	initialHttpRequestSize = 512
	minHttpRequestSize     = 1024
)

type HttpReader struct {
	url          string
	offset       int64
	size         int64
	client       *http.Client
	buffer       ReaderAtSeeker
	bufferOffset int64
	bufferSize   int64
	validator    string
}

func NewHttpReader(url string) (*HttpReader, error) {
	reader := &HttpReader{
		url:    url,
		client: &http.Client{},
	}
	if err := reader.init(); err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *HttpReader) init() error {
	req, err := http.NewRequest(http.MethodGet, r.url, nil)
	if err != nil {
		return err
	}

	// make an initial range request to determine size
	req.Header.Add("Range", fmt.Sprintf("bytes=0-%d", initialHttpRequestSize-1))
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if !success(resp) {
		return fmt.Errorf("unexpected response from %s: %d", r.url, resp.StatusCode)
	}

	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("failed to read response from %s: %w", r.url, readErr)
	}

	r.buffer = bytes.NewReader(data)
	r.bufferSize = int64(len(data))

	str := resp.Header.Get("Content-Range")
	if strings.Contains(str, "/") {
		size, err := strconv.ParseInt(strings.Split(str, "/")[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid content-range header from %s: %w", r.url, err)
		}
		r.size = size
		r.validator = validatorFromResponse(resp)
	} else {
		r.size = int64(len(data))
	}
	return nil
}

func success(response *http.Response) bool {
	return response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices
}

func validatorFromResponse(resp *http.Response) string {
	etag := resp.Header.Get("ETag")
	if etag != "" && etag[0] == '"' {
		return etag
	}

	return resp.Header.Get("Last-Modified")
}

func (r *HttpReader) ReadAt(data []byte, offset int64) (int, error) {
	_, err := r.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	total := 0
	for {
		if total >= len(data) {
			break
		}
		n, err := r.Read(data[total:])
		if err != nil {
			return total + n, err
		}
		total = total + n
	}
	return total, nil
}

func (r *HttpReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		offset = r.offset + offset
	case io.SeekEnd:
		offset = r.size + offset
	}

	if offset < 0 {
		return 0, fmt.Errorf("attempt to seek to a negative offset: %d", offset)
	}
	r.offset = offset
	return offset, nil
}

func (r *HttpReader) Read(data []byte) (n int, err error) {
	if r.offset > r.size {
		return 0, io.EOF
	}
	if r.buffer == nil || r.offset < r.bufferOffset || r.offset > r.bufferOffset+r.bufferSize {
		if err := r.request(int64(len(data))); err != nil {
			return 0, err
		}
	}
	read, err := r.buffer.ReadAt(data, r.offset-r.bufferOffset)
	r.offset = r.offset + int64(read)
	if err == io.EOF && r.offset < r.size {
		r.buffer = nil
		return read, nil
	}
	return read, err
}

func (r *HttpReader) request(size int64) error {
	req, err := http.NewRequest(http.MethodGet, r.url, nil)
	if err != nil {
		return err
	}
	requestSize := size
	if requestSize < minHttpRequestSize {
		requestSize = minHttpRequestSize
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", r.offset, r.offset+requestSize))
	if r.validator != "" {
		req.Header.Set("If-Range", r.validator)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if !success(resp) {
		return fmt.Errorf("unexpected response from %s: %d", r.url, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	r.buffer = bytes.NewReader(data)
	r.bufferOffset = r.offset
	r.bufferSize = int64(len(data))
	return nil
}

func (r *HttpReader) Close() error {
	if r.buffer != nil {
		r.buffer = nil
	}
	r.client.CloseIdleConnections()
	return nil
}
