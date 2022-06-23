// Copyright (c) 2012 VMware, Inc.

package gosigar

import (
	"bytes"
)

// byteListToString converts the raw byte arrays we get into a string. This is a bit of a process, as byte strings are normally []uint8
func byteListToString(raw []int8) string {
	byteList := make([]byte, len(raw))

	for pos, singleByte := range raw {
		byteList[pos] = byte(singleByte)
		if singleByte == 0 {
			break
		}
	}

	return string(bytes.Trim(byteList, "\x00"))
}

func chop(buf []byte) []byte {
	return buf[0 : len(buf)-1]
}

// convertBytesToString trims null bytes and returns a string
func convertBytesToString(arr []byte) string {
	return string(bytes.Trim(arr, "\x00"))
}
