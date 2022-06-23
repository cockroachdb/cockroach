package goparquet

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"io"
	"math"
	"math/bits"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/pkg/errors"
)

// DefaultHashFunc is used to generate a hash value to detect and handle duplicate values.
// The function has to return any type that can be used as a map key. In particular, the
// result can not be a slice. The default implementation used the fnv hash function as
// implemented in Go's standard library.
var DefaultHashFunc func([]byte) interface{}

func init() {
	DefaultHashFunc = fnvHashFunc
}

type byteReader struct {
	io.Reader
}

func (br *byteReader) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(br.Reader, buf); err != nil {
		return 0, err
	}

	return buf[0], nil
}

type offsetReader struct {
	inner  io.ReadSeeker
	offset int64
	count  int64
}

func (o *offsetReader) Read(p []byte) (int, error) {
	n, err := o.inner.Read(p)
	o.offset += int64(n)
	o.count += int64(n)
	return n, err
}

func (o *offsetReader) Seek(offset int64, whence int) (int64, error) {
	i, err := o.inner.Seek(offset, whence)
	if err == nil {
		o.count += i - o.offset
		o.offset = i
	}

	return i, err
}

func (o *offsetReader) Count() int64 {
	return o.count
}

func decodeRLEValue(bytes []byte) int32 {
	switch len(bytes) {
	case 0:
		return 0
	case 1:
		return int32(bytes[0])
	case 2:
		return int32(bytes[0]) + int32(bytes[1])<<8
	case 3:
		return int32(bytes[0]) + int32(bytes[1])<<8 + int32(bytes[2])<<16
	case 4:
		return int32(bytes[0]) + int32(bytes[1])<<8 + int32(bytes[2])<<16 + int32(bytes[3])<<24
	default:
		panic("invalid argument")
	}
}

func writeFull(w io.Writer, buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	cnt, err := w.Write(buf)
	if err != nil {
		return err
	}

	if cnt != len(buf) {
		return errors.Errorf("need to write %d byte wrote %d", cnt, len(buf))
	}

	return nil
}

type thriftReader interface {
	Read(context.Context, thrift.TProtocol) error
}

func readThrift(ctx context.Context, tr thriftReader, r io.Reader) error {
	// Make sure we are not using any kind of buffered reader here. bufio.Reader "can" reads more data ahead of time,
	// which is a problem on this library
	transport := &thrift.StreamTransport{Reader: r}
	proto := thrift.NewTCompactProtocolConf(transport, &thrift.TConfiguration{})
	return tr.Read(ctx, proto)
}

type thriftWriter interface {
	Write(context.Context, thrift.TProtocol) error
}

func writeThrift(ctx context.Context, tr thriftWriter, w io.Writer) error {
	transport := &thrift.StreamTransport{Writer: w}
	proto := thrift.NewTCompactProtocolConf(transport, &thrift.TConfiguration{})
	return tr.Write(ctx, proto)
}

func decodeInt32(d decoder, data []int32) error {
	for i := range data {
		u, err := d.next()
		if err != nil {
			return err
		}
		data[i] = u
	}

	return nil
}

func decodePackedArray(d levelDecoder, count int) (*packedArray, int, error) {
	ret := &packedArray{}
	ret.reset(bits.Len16(d.maxLevel()))
	nn := 0 // Counting not nulls only good for dLevels
	for i := 0; i < count; i++ {
		u, err := d.next()
		if err != nil {
			return nil, 0, err
		}
		ret.appendSingle(u)
		if u == int32(d.maxLevel()) {
			nn++
		}
	}

	return ret, nn, nil
}

func readUVariant32(r io.Reader) (int32, error) {
	b, ok := r.(io.ByteReader)
	if !ok {
		b = &byteReader{Reader: r}
	}

	i, err := binary.ReadUvarint(b)
	if err != nil {
		return 0, err
	}

	if i > math.MaxInt32 {
		return 0, errors.New("int32 out of range")
	}

	return int32(i), nil
}

func readVariant32(r io.Reader) (int32, error) {
	b, ok := r.(io.ByteReader)
	if !ok {
		b = &byteReader{Reader: r}
	}

	i, err := binary.ReadVarint(b)
	if err != nil {
		return 0, err
	}

	if i > math.MaxInt32 || i < math.MinInt32 {
		return 0, errors.New("int32 out of range")
	}

	return int32(i), nil
}

func writeVariant(w io.Writer, in int64) error {
	buf := make([]byte, 12)
	n := binary.PutVarint(buf, in)

	return writeFull(w, buf[:n])
}

func writeUVariant(w io.Writer, in uint64) error {
	buf := make([]byte, 12)
	n := binary.PutUvarint(buf, in)

	return writeFull(w, buf[:n])
}

func readVariant64(r io.Reader) (int64, error) {
	b, ok := r.(io.ByteReader)
	if !ok {
		b = &byteReader{Reader: r}
	}

	return binary.ReadVarint(b)
}

type constDecoder int32

func (cd constDecoder) initSize(io.Reader) error {
	return nil
}

func (cd constDecoder) init(io.Reader) error {
	return nil
}

func (cd constDecoder) next() (int32, error) {
	return int32(cd), nil
}

type levelDecoderWrapper struct {
	decoder
	max uint16
}

func (l *levelDecoderWrapper) maxLevel() uint16 {
	return l.max
}

// check the b2 into b1 to find the max prefix len
func prefix(b1, b2 []byte) int {
	l := len(b1)
	if l2 := len(b2); l > l2 {
		l = l2
	}
	for i := 0; i < l; i++ {
		if b1[i] != b2[i] {
			return i
		}
	}

	return l
}

func encodeValue(w io.Writer, enc valuesEncoder, all []interface{}) error {
	if err := enc.init(w); err != nil {
		return err
	}

	if err := enc.encodeValues(all); err != nil {
		return err
	}

	return enc.Close()
}

// In PageV1 the rle stream for rep/def level has the size in stream , but in V2 the size is inside the header not the
// stream
func encodeLevelsV1(w io.Writer, max uint16, values *packedArray) error {
	rle := newHybridEncoder(bits.Len16(max))
	if err := rle.initSize(w); err != nil {
		return errors.Wrap(err, "level writer initialize with size failed")
	}
	if err := rle.encodePacked(values); err != nil {
		return errors.Wrap(err, "level writer encode values failed")
	}

	return errors.Wrap(rle.Close(), "level writer flush failed")
}

func encodeLevelsV2(w io.Writer, max uint16, values *packedArray) error {
	rle := newHybridEncoder(bits.Len16(max))
	if err := rle.init(w); err != nil {
		return errors.Wrap(err, "level writer initialize with size failed")
	}
	if err := rle.encodePacked(values); err != nil {
		return errors.Wrap(err, "level writer encode values failed")
	}

	return errors.Wrap(rle.Close(), "level writer flush failed")
}

func mapKey(a interface{}) interface{} {
	switch v := a.(type) {
	case int, int32, int64, string, bool:
		return a
	case float64:
		return math.Float64bits(v)
	case float32:
		return math.Float32bits(v)
	case []byte:
		return DefaultHashFunc(v)
	case [12]byte:
		return DefaultHashFunc(v[:])
	default:
		panic("not supported type")
	}
}

func fnvHashFunc(in []byte) interface{} {
	hash := fnv.New64()
	if err := writeFull(hash, in); err != nil {
		panic(err)
	}
	return hash.Sum64()
}

type writePos interface {
	io.Writer
	Pos() int64
}

type writePosStruct struct {
	w   io.Writer
	pos int64
}

func (w *writePosStruct) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.pos += int64(n)
	return n, err
}

func (w *writePosStruct) Pos() int64 {
	return w.pos
}
