package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RespType represents the type of a RESP value.
type RespType byte

const (
	RespSimpleString RespType = '+'
	RespError        RespType = '-'
	RespInteger      RespType = ':'
	RespBulkString   RespType = '$'
	RespArray        RespType = '*'
)

// RespValue represents a parsed RESP value.
type RespValue struct {
	Type  RespType
	Str   string    // for simple strings, errors, bulk strings
	Int   int64     // for integers
	Array []*RespValue // for arrays
	Null  bool      // true if this is a null value
}

// RespParser reads RESP values from a reader.
type RespParser struct {
	reader *bufio.Reader
}

// NewRespParser creates a new RESP parser.
func NewRespParser(r io.Reader) *RespParser {
	return &RespParser{reader: bufio.NewReader(r)}
}

// Parse reads and parses the next RESP value.
func (p *RespParser) Parse() (*RespValue, error) {
	b, err := p.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch RespType(b) {
	case RespSimpleString:
		return p.parseSimpleString()
	case RespError:
		return p.parseError()
	case RespInteger:
		return p.parseInteger()
	case RespBulkString:
		return p.parseBulkString()
	case RespArray:
		return p.parseArray()
	default:
		return nil, fmt.Errorf("unknown resp type: %c", b)
	}
}

func (p *RespParser) parseSimpleString() (*RespValue, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	return &RespValue{Type: RespSimpleString, Str: line}, nil
}

func (p *RespParser) parseError() (*RespValue, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	return &RespValue{Type: RespError, Str: line}, nil
}

func (p *RespParser) parseInteger() (*RespValue, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	i, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return nil, err
	}
	return &RespValue{Type: RespInteger, Int: i}, nil
}

const (
	maxBulkStringLen = 64 * 1024 * 1024 // 64 MiB safety cap
	maxArrayElements = 1024 * 1024      // 1M elements safety cap
)

func (p *RespParser) parseBulkString() (*RespValue, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	n, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}
	if n == -1 {
		return &RespValue{Type: RespBulkString, Null: true}, nil
	}
	if n < 0 || n > maxBulkStringLen {
		return nil, fmt.Errorf("invalid bulk string length: %d", n)
	}
	data := make([]byte, n+2) // +2 for \r\n
	if _, err := io.ReadFull(p.reader, data); err != nil {
		return nil, err
	}
	return &RespValue{Type: RespBulkString, Str: string(data[:n])}, nil
}

func (p *RespParser) parseArray() (*RespValue, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	count, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}
	if count == -1 {
		return &RespValue{Type: RespArray, Null: true}, nil
	}
	if count < 0 || count > maxArrayElements {
		return nil, fmt.Errorf("invalid array length: %d", count)
	}
	arr := make([]*RespValue, count)
	for i := 0; i < count; i++ {
		val, err := p.Parse()
		if err != nil {
			return nil, err
		}
		arr[i] = val
	}
	return &RespValue{Type: RespArray, Array: arr}, nil
}

// RespWriter writes RESP values to a writer.
type RespWriter struct {
	writer io.Writer
}

// NewRespWriter creates a new RESP writer.
func NewRespWriter(w io.Writer) *RespWriter {
	return &RespWriter{writer: w}
}

// WriteSimpleString writes a simple string value.
func (w *RespWriter) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.writer, "+%s\r\n", s)
	return err
}

// WriteError writes an error value.
func (w *RespWriter) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.writer, "-%s\r\n", msg)
	return err
}

// WriteInteger writes an integer value.
func (w *RespWriter) WriteInteger(i int64) error {
	_, err := fmt.Fprintf(w.writer, ":%d\r\n", i)
	return err
}

// WriteBulkString writes a bulk string value.
func (w *RespWriter) WriteBulkString(s string) error {
	_, err := fmt.Fprintf(w.writer, "$%d\r\n%s\r\n", len(s), s)
	return err
}

// WriteBulkStringBytes writes a bulk string from bytes.
func (w *RespWriter) WriteBulkStringBytes(b []byte) error {
	if b == nil {
		_, err := w.writer.Write([]byte("$-1\r\n"))
		return err
	}
	_, err := fmt.Fprintf(w.writer, "$%d\r\n", len(b))
	if err != nil {
		return err
	}
	_, err = w.writer.Write(b)
	if err != nil {
		return err
	}
	_, err = w.writer.Write([]byte("\r\n"))
	return err
}

// WriteArray writes an array of values.
func (w *RespWriter) WriteArray(values []*RespValue) error {
	if values == nil {
		_, err := w.writer.Write([]byte("*-1\r\n"))
		return err
	}
	if _, err := fmt.Fprintf(w.writer, "*%d\r\n", len(values)); err != nil {
		return err
	}
	for _, val := range values {
		if err := w.writeValue(val); err != nil {
			return err
		}
	}
	return nil
}

func (w *RespWriter) writeValue(val *RespValue) error {
	if val.Null {
		_, err := w.writer.Write([]byte("$-1\r\n"))
		return err
	}
	switch val.Type {
	case RespSimpleString:
		return w.WriteSimpleString(val.Str)
	case RespError:
		return w.WriteError(val.Str)
	case RespInteger:
		return w.WriteInteger(val.Int)
	case RespBulkString:
		return w.WriteBulkString(val.Str)
	case RespArray:
		return w.WriteArray(val.Array)
	default:
		return errors.New("unknown resp type")
	}
}

// ExtractCommand extracts command name and arguments from a RESP array.
func ExtractCommand(val *RespValue) (string, []string, error) {
	if val.Type != RespArray {
		return "", nil, errors.New("command must be an array")
	}
	if len(val.Array) < 1 {
		return "", nil, errors.New("empty command")
	}
	if val.Array[0].Type != RespBulkString {
		return "", nil, errors.New("command name must be bulk string")
	}
	cmd := strings.ToUpper(val.Array[0].Str)
	args := make([]string, len(val.Array)-1)
	for i, v := range val.Array[1:] {
		if v.Type != RespBulkString {
			return "", nil, errors.New("argument must be bulk string")
		}
		args[i] = v.Str
	}
	return cmd, args, nil
}
