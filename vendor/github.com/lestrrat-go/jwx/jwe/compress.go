package jwe

import (
	"bytes"
	"compress/flate"
	"io/ioutil"

	"github.com/lestrrat-go/jwx/internal/pool"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/pkg/errors"
)

func uncompress(plaintext []byte) ([]byte, error) {
	return ioutil.ReadAll(flate.NewReader(bytes.NewReader(plaintext)))
}

func compress(plaintext []byte, alg jwa.CompressionAlgorithm) ([]byte, error) {
	if alg == jwa.NoCompress {
		return plaintext, nil
	}

	buf := pool.GetBytesBuffer()
	defer pool.ReleaseBytesBuffer(buf)

	w, _ := flate.NewWriter(buf, 1)
	in := plaintext
	for len(in) > 0 {
		n, err := w.Write(in)
		if err != nil {
			return nil, errors.Wrap(err, `failed to write to compression writer`)
		}
		in = in[n:]
	}
	if err := w.Close(); err != nil {
		return nil, errors.Wrap(err, "failed to close compression writer")
	}

	ret := make([]byte, buf.Len())
	copy(ret, buf.Bytes())
	return ret, nil
}
