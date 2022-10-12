package concatkdf

import (
	"crypto"
	"encoding/binary"

	"github.com/pkg/errors"
)

type KDF struct {
	buf       []byte
	otherinfo []byte
	z         []byte
	hash      crypto.Hash
}

func ndata(src []byte) []byte {
	buf := make([]byte, 4+len(src))
	binary.BigEndian.PutUint32(buf, uint32(len(src)))
	copy(buf[4:], src)
	return buf
}

func New(hash crypto.Hash, alg, Z, apu, apv, pubinfo, privinfo []byte) *KDF {
	algbuf := ndata(alg)
	apubuf := ndata(apu)
	apvbuf := ndata(apv)

	concat := make([]byte, len(algbuf)+len(apubuf)+len(apvbuf)+len(pubinfo)+len(privinfo))
	n := copy(concat, algbuf)
	n += copy(concat[n:], apubuf)
	n += copy(concat[n:], apvbuf)
	n += copy(concat[n:], pubinfo)
	copy(concat[n:], privinfo)

	return &KDF{
		hash:      hash,
		otherinfo: concat,
		z:         Z,
	}
}

func (k *KDF) Read(out []byte) (int, error) {
	var round uint32 = 1
	h := k.hash.New()

	for len(out) > len(k.buf) {
		h.Reset()

		if err := binary.Write(h, binary.BigEndian, round); err != nil {
			return 0, errors.Wrap(err, "failed to write round using kdf")
		}
		if _, err := h.Write(k.z); err != nil {
			return 0, errors.Wrap(err, "failed to write z using kdf")
		}
		if _, err := h.Write(k.otherinfo); err != nil {
			return 0, errors.Wrap(err, "failed to write other info using kdf")
		}

		k.buf = append(k.buf, h.Sum(nil)...)
		round++
	}

	n := copy(out, k.buf[:len(out)])
	k.buf = k.buf[len(out):]
	return n, nil
}
