package goparquet

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"sync"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/golang/snappy"
)

var (
	compressors    = make(map[parquet.CompressionCodec]BlockCompressor)
	compressorLock sync.RWMutex
)

type (
	// BlockCompressor is an interface to describe of a block compressor to be used
	// in compressing the content of parquet files.
	BlockCompressor interface {
		CompressBlock([]byte) ([]byte, error)
		DecompressBlock([]byte) ([]byte, error)
	}

	plainCompressor  struct{}
	snappyCompressor struct{}
	gzipCompressor   struct{}
)

func (plainCompressor) CompressBlock(block []byte) ([]byte, error) {
	return block, nil
}

func (plainCompressor) DecompressBlock(block []byte) ([]byte, error) {
	return block, nil
}

func (snappyCompressor) CompressBlock(block []byte) ([]byte, error) {
	return snappy.Encode(nil, block), nil
}

func (snappyCompressor) DecompressBlock(block []byte) ([]byte, error) {
	return snappy.Decode(nil, block)
}

func (gzipCompressor) CompressBlock(block []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	if _, err := w.Write(block); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (gzipCompressor) DecompressBlock(block []byte) ([]byte, error) {
	buf := bytes.NewReader(block)
	r, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}

	ret, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return ret, r.Close()
}

func compressBlock(block []byte, method parquet.CompressionCodec) ([]byte, error) {
	compressorLock.RLock()
	defer compressorLock.RUnlock()

	c, ok := compressors[method]
	if !ok {
		return nil, errors.Errorf("method %q is not supported", method.String())
	}

	return c.CompressBlock(block)
}

func decompressBlock(block []byte, method parquet.CompressionCodec) ([]byte, error) {
	compressorLock.RLock()
	defer compressorLock.RUnlock()

	c, ok := compressors[method]
	if !ok {
		return nil, errors.Errorf("method %q is not supported", method.String())
	}

	return c.DecompressBlock(block)
}

func newBlockReader(buf []byte, codec parquet.CompressionCodec, compressedSize int32, uncompressedSize int32) (io.Reader, error) {
	if compressedSize < 0 || uncompressedSize < 0 {
		return nil, errors.New("invalid page data size")
	}

	if len(buf) != int(compressedSize) {
		return nil, errors.Errorf("compressed data must be %d byte but its %d byte", compressedSize, len(buf))
	}

	res, err := decompressBlock(buf, codec)
	if err != nil {
		return nil, errors.Wrap(err, "decompression failed")
	}

	if len(res) != int(uncompressedSize) {
		return nil, errors.Errorf("decompressed data must be %d byte but its %d byte", uncompressedSize, len(res))
	}

	return bytes.NewReader(res), nil
}

// RegisterBlockCompressor is a function to to register additional block compressors to the package. By default,
// only UNCOMPRESSED, GZIP and SNAPPY are supported as parquet compression algorithms. The parquet file format
// supports more compression algorithms, such as LZO, BROTLI, LZ4 and ZSTD. To limit the amount of external dependencies,
// the number of supported algorithms was reduced to a core set. If you want to use any of the other compression
// algorithms, please provide your own implementation of it in a way that satisfies the BlockCompressor interface,
// and register it using this function from your code.
func RegisterBlockCompressor(method parquet.CompressionCodec, compressor BlockCompressor) {
	compressorLock.Lock()
	defer compressorLock.Unlock()

	compressors[method] = compressor
}

// GetRegisteredBlockCompressors returns a map of compression codecs to block compressors that
// are currently registered.
func GetRegisteredBlockCompressors() map[parquet.CompressionCodec]BlockCompressor {
	result := make(map[parquet.CompressionCodec]BlockCompressor)

	compressorLock.Lock()
	defer compressorLock.Unlock()

	for k, v := range compressors {
		result[k] = v
	}

	return result
}

func init() {
	RegisterBlockCompressor(parquet.CompressionCodec_UNCOMPRESSED, plainCompressor{})
	RegisterBlockCompressor(parquet.CompressionCodec_GZIP, gzipCompressor{})
	RegisterBlockCompressor(parquet.CompressionCodec_SNAPPY, snappyCompressor{})
}
