package sarama

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"sync"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/pierrec/lz4"
)

var (
	lz4ReaderPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewReader(nil)
		},
	}

	gzipReaderPool sync.Pool
)

func decompress(cc CompressionCodec, data []byte) ([]byte, error) {
	switch cc {
	case CompressionNone:
		return data, nil
	case CompressionGZIP:
		var err error
		reader, ok := gzipReaderPool.Get().(*gzip.Reader)
		if !ok {
			reader, err = gzip.NewReader(bytes.NewReader(data))
		} else {
			err = reader.Reset(bytes.NewReader(data))
		}

		if err != nil {
			return nil, err
		}

		defer gzipReaderPool.Put(reader)

		return ioutil.ReadAll(reader)
	case CompressionSnappy:
		return snappy.Decode(data)
	case CompressionLZ4:
		reader, ok := lz4ReaderPool.Get().(*lz4.Reader)
		if !ok {
			reader = lz4.NewReader(bytes.NewReader(data))
		} else {
			reader.Reset(bytes.NewReader(data))
		}
		defer lz4ReaderPool.Put(reader)

		return ioutil.ReadAll(reader)
	case CompressionZSTD:
		return zstdDecompress(nil, data)
	default:
		return nil, PacketDecodingError{fmt.Sprintf("invalid compression specified (%d)", cc)}
	}
}
