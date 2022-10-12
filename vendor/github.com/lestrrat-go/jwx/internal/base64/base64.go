package base64

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"

	"github.com/pkg/errors"
)

func Encode(src []byte) []byte {
	enc := base64.RawURLEncoding
	dst := make([]byte, enc.EncodedLen(len(src)))
	enc.Encode(dst, src)
	return dst
}

func EncodeToStringStd(src []byte) string {
	return base64.StdEncoding.EncodeToString(src)
}

func EncodeToString(src []byte) string {
	return base64.RawURLEncoding.EncodeToString(src)
}

func EncodeUint64ToString(v uint64) string {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, v)

	i := 0
	for ; i < len(data); i++ {
		if data[i] != 0x0 {
			break
		}
	}

	return EncodeToString(data[i:])
}

func Decode(src []byte) ([]byte, error) {
	var enc *base64.Encoding

	var isRaw = !bytes.HasSuffix(src, []byte{'='})
	var isURL = !bytes.ContainsAny(src, "+/")
	switch {
	case isRaw && isURL:
		enc = base64.RawURLEncoding
	case isURL:
		enc = base64.URLEncoding
	case isRaw:
		enc = base64.RawStdEncoding
	default:
		enc = base64.StdEncoding
	}

	dst := make([]byte, enc.DecodedLen(len(src)))
	n, err := enc.Decode(dst, src)
	if err != nil {
		return nil, errors.Wrap(err, `failed to decode source`)
	}
	return dst[:n], nil
}

func DecodeString(src string) ([]byte, error) {
	return Decode([]byte(src))
}
