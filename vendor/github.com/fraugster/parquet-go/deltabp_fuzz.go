// +build gofuzz

package goparquet

import (
	"bytes"
)

func FuzzDelta(data []byte) int {
	d := deltaBitPackDecoder32{}
	if err := d.init(bytes.NewReader(data)); err != nil {
		return 0
	}

	for i := 0; i < len(data)/4; i++ {
		_, err := d.next()
		if err != nil {
			return 0
		}
	}

	return 1
}
