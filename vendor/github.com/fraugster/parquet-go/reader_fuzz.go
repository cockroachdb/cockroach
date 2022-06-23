// +build gofuzz

package goparquet

import "bytes"

func FuzzFileReader(data []byte) int {
	r, err := NewFileReader(bytes.NewReader(data))
	if err != nil {
		return 0
	}

	rows := r.NumRows()
	for i := int64(0); i < rows; i++ {
		_, err := r.NextRow()
		if err != nil {
			return 0
		}
	}

	return 1
}
