package resp

import (
	"bufio"
	"io"
	"strconv"

	log "github.com/sirupsen/logrus"
)

const (
	STRING      = '+'
	ERROR       = '-'
	INTEGER     = ':'
	BULK_STRING = '$'
	ARRAY       = '*'
)

type RespValue struct {
	Type   string
	String string
	Number int
	Bulk   string
	Array  []RespValue
}

type Response struct {
	reader *bufio.Reader
}

func NewResponse(r io.Reader) *Response {
	return &Response{reader: bufio.NewReader(r)}
}

func (r *Response) readLine() (line []byte, n int, err error) {
	for {
		bytes, err := r.reader.ReadByte()

		if err != nil {
			return nil, 0, err
		}
		n++
		line = append(line, bytes)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Response) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	in64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return int(in64), n, nil
}

func (r *Response) readArray() (RespValue, error) {
	v := RespValue{}
	v.Type = "array"

	len, _, err := r.readInteger()
	if err != nil {
		return RespValue{}, err
	}
	v.Array = make([]RespValue, 0)
	for i := 0; i < len; i++ {
		value, err := r.Read()
		if err != nil {
			return RespValue{}, err
		}
		v.Array = append(v.Array, value)
	}
	return v, nil
}

func (r *Response) readBulk() (RespValue, error) {
	v := RespValue{}

	v.Type = "bulk"

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)

	r.reader.Read(bulk)

	v.Bulk = string(bulk)

	// Read the trailing CRLF
	r.readLine()

	return v, nil
}

func (r *Response) Read() (RespValue, error) {
	OpType, err := r.reader.ReadByte()
	if err != nil {
		return RespValue{}, err
	}
	switch OpType {
	case ARRAY:
		return r.readArray()
	case BULK_STRING:
		return r.readBulk()
	default:
		log.Errorf("Unkown value type: %v", OpType)
		return RespValue{}, nil

	}
}

func (v RespValue) Marshal() []byte {
	switch v.Type {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshallError()
	default:
		return []byte{}
	}
}

func (v RespValue) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.String...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v RespValue) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK_STRING)
	bytes = append(bytes, strconv.Itoa(len(v.Bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.Bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v RespValue) marshalArray() []byte {
	len := len(v.Array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.Array[i].Marshal()...)
	}

	return bytes
}

func (v RespValue) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.String...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v RespValue) marshallNull() []byte {
	return []byte("$-1\r\n")
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v RespValue) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
