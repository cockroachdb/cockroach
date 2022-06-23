// +build gofuzz

package goparquet

import (
	"bytes"
	"fmt"
	"math/bits"
)

func FuzzHybrid(data []byte) int {

	l := len(data)

bigLoop:
	for w := 1; w <= 32; w++ {
		d := newHybridDecoder(w)
		if err := d.init(bytes.NewReader(data)); err != nil {
			continue bigLoop
		}

		maxCount := l / w
		for i := 0; i < maxCount; i++ {
			v, err := d.next()
			if err != nil {
				continue bigLoop
			}
			if bits.LeadingZeros32(uint32(v)) < 32-w {
				panic(fmt.Sprintf("decoded value %d is too large for width %d", v, w))
			}
		}
		return 1
	}
	return 0
}
