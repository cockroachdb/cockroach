// Histograms are encoded using the HdrHistogram V2 format which is based on an adapted ZigZag LEB128 encoding where:
// consecutive zero counters are encoded as a negative number representing the count of consecutive zeros
// non zero counter values are encoded as a positive number
// A typical histogram (2 digits precision 1 usec to 1 day range) can be encoded in less than the typical MTU size of 1500 bytes.
package hdrhistogram

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io/ioutil"
)

const (
	V2EncodingCookieBase           int32 = 0x1c849303
	V2CompressedEncodingCookieBase int32 = 0x1c849304
	encodingCookie                 int32 = V2EncodingCookieBase | 0x10
	compressedEncodingCookie       int32 = V2CompressedEncodingCookieBase | 0x10

	ENCODING_HEADER_SIZE = 40
)

// Encode returns a snapshot view of the Histogram.
// The snapshot is compact binary representations of the state of the histogram.
// They are intended to be used for archival or transmission to other systems for further analysis.
func (h *Histogram) Encode(version int32) (buffer []byte, err error) {
	switch version {
	case V2CompressedEncodingCookieBase:
		buffer, err = h.dumpV2CompressedEncoding()
	default:
		err = fmt.Errorf("The provided enconding version %d is not supported.", version)
	}
	return
}

// Decode returns a new Histogram by decoding it from a String containing
// a base64 encoded compressed histogram representation.
func Decode(encoded []byte) (rh *Histogram, err error) {
	var decoded []byte
	decoded, err = base64.StdEncoding.DecodeString(string(encoded))
	if err != nil {
		return
	}
	rbuf := bytes.NewBuffer(decoded[0:8])
	r32 := make([]int32, 2)
	err = binary.Read(rbuf, binary.BigEndian, &r32)
	if err != nil {
		return
	}
	Cookie := r32[0] & ^0xf0
	lengthOfCompressedContents := r32[1]
	if Cookie != V2CompressedEncodingCookieBase {
		err = fmt.Errorf("Encoding not supported, only V2 is supported. Got %d want %d", Cookie, V2CompressedEncodingCookieBase)
		return
	}
	decodeLengthOfCompressedContents := int32(len(decoded[8:]))
	if lengthOfCompressedContents > decodeLengthOfCompressedContents {
		err = fmt.Errorf("The compressed contents buffer is smaller than the lengthOfCompressedContents. Got %d want %d", decodeLengthOfCompressedContents, lengthOfCompressedContents)
		return
	}
	rh, err = decodeCompressedFormat(decoded[8:8+lengthOfCompressedContents], ENCODING_HEADER_SIZE)
	return
}

// internal method to encode an histogram in V2 Compressed format
func (h *Histogram) dumpV2CompressedEncoding() (outBuffer []byte, err error) {
	// final buffer
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.BigEndian, compressedEncodingCookie)
	if err != nil {
		return
	}
	toCompress, err := h.encodeIntoByteBuffer()
	if err != nil {
		return
	}
	uncompressedBytes := toCompress.Bytes()

	var b bytes.Buffer
	w, err := zlib.NewWriterLevel(&b, zlib.BestCompression)
	if err != nil {
		return
	}
	_, err = w.Write(uncompressedBytes)
	if err != nil {
		return
	}
	w.Close()

	// LengthOfCompressedContents
	compressedContents := b.Bytes()
	err = binary.Write(buf, binary.BigEndian, int32(len(compressedContents)))
	if err != nil {
		return
	}
	err = binary.Write(buf, binary.BigEndian, compressedContents)
	if err != nil {
		return
	}
	outBuffer = []byte(base64.StdEncoding.EncodeToString(buf.Bytes()))
	return
}

func (h *Histogram) encodeIntoByteBuffer() (*bytes.Buffer, error) {

	countsBytes, err := h.fillBufferFromCountsArray()
	if err != nil {
		return nil, err
	}

	toCompress := new(bytes.Buffer)
	err = binary.Write(toCompress, binary.BigEndian, encodingCookie) // 0-3
	if err != nil {
		return nil, err
	}
	err = binary.Write(toCompress, binary.BigEndian, int32(len(countsBytes))) // 3-7
	if err != nil {
		return nil, err
	}
	err = binary.Write(toCompress, binary.BigEndian, h.getNormalizingIndexOffset()) // 8-11
	if err != nil {
		return nil, err
	}
	err = binary.Write(toCompress, binary.BigEndian, int32(h.significantFigures)) // 12-15
	if err != nil {
		return nil, err
	}
	err = binary.Write(toCompress, binary.BigEndian, h.lowestDiscernibleValue) // 16-23
	if err != nil {
		return nil, err
	}
	err = binary.Write(toCompress, binary.BigEndian, h.highestTrackableValue) // 24-31
	if err != nil {
		return nil, err
	}
	err = binary.Write(toCompress, binary.BigEndian, h.getIntegerToDoubleValueConversionRatio()) // 32-39
	if err != nil {
		return nil, err
	}
	err = binary.Write(toCompress, binary.BigEndian, countsBytes)
	if err != nil {
		return nil, err
	}
	return toCompress, err
}

func decodeCompressedFormat(compressedContents []byte, headerSize int) (rh *Histogram, err error) {
	b := bytes.NewReader(compressedContents)
	z, err := zlib.NewReader(b)
	if err != nil {
		return
	}
	defer z.Close()
	decompressedSlice, err := ioutil.ReadAll(z)
	if err != nil {
		return
	}
	decompressedSliceLen := int32(len(decompressedSlice))
	cookie, PayloadLength, _, NumberOfSignificantValueDigits, LowestTrackableValue, HighestTrackableValue, _, err := decodeDeCompressedHeaderFormat(decompressedSlice[0:headerSize])
	if err != nil {
		return
	}
	if cookie != V2EncodingCookieBase {
		err = fmt.Errorf("Encoding not supported, only V2 is supported. Got %d want %d", cookie, V2EncodingCookieBase)
		return
	}
	actualPayloadLen := decompressedSliceLen - int32(headerSize)
	if PayloadLength != actualPayloadLen {
		err = fmt.Errorf("PayloadLength should have the same size of the actual payload. Got %d want %d", actualPayloadLen, PayloadLength)
		return
	}
	rh = New(LowestTrackableValue, HighestTrackableValue, int(NumberOfSignificantValueDigits))
	payload := decompressedSlice[headerSize:]
	err = fillCountsArrayFromSourceBuffer(payload, rh)
	return rh, err
}

func fillCountsArrayFromSourceBuffer(payload []byte, rh *Histogram) (err error) {
	var payloadSlicePos = 0
	var dstIndex int64 = 0
	var n int
	var count int64
	var zerosCount int64
	for payloadSlicePos < len(payload) {
		count, n, err = zig_zag_decode_i64(payload[payloadSlicePos:])
		if err != nil {
			return
		}
		payloadSlicePos += n
		if count < 0 {
			zerosCount = -count
			dstIndex += zerosCount
		} else {
			rh.setCountAtIndex(int(dstIndex), count)
			dstIndex += 1
		}
	}
	return
}

func (rh *Histogram) fillBufferFromCountsArray() (buffer []byte, err error) {
	buf := new(bytes.Buffer)
	// V2 encoding format uses a ZigZag LEB128-64b9B encoded long. Positive values are counts,
	// while negative values indicate a repeat zero counts.
	var countsLimit int32 = int32(rh.countsIndexFor(rh.Max()) + 1)
	var srcIndex int32 = 0
	for srcIndex < countsLimit {
		count := rh.counts[srcIndex]
		srcIndex++

		var zeros int64 = 0
		// check for contiguous zeros
		if count == 0 {
			zeros = 1
			for srcIndex < countsLimit && 0 == rh.counts[srcIndex] {
				zeros++
				srcIndex++
			}
		}
		if zeros > 1 {
			err = binary.Write(buf, binary.BigEndian, zig_zag_encode_i64(-zeros))
			if err != nil {
				return
			}
		} else {
			err = binary.Write(buf, binary.BigEndian, zig_zag_encode_i64(count))
			if err != nil {
				return
			}
		}
	}
	buffer = buf.Bytes()
	return
}

func decodeDeCompressedHeaderFormat(decoded []byte) (Cookie int32, PayloadLength int32, NormalizingIndexOffSet int32, NumberOfSignificantValueDigits int32, LowestTrackableValue int64, HighestTrackableValue int64, IntegerToDoubleConversionRatio float64, err error) {
	rbuf := bytes.NewBuffer(decoded[0:40])
	r32 := make([]int32, 4)
	r64 := make([]int64, 2)
	err = binary.Read(rbuf, binary.BigEndian, &r32)
	if err != nil {
		return
	}
	err = binary.Read(rbuf, binary.BigEndian, &r64)
	if err != nil {
		return
	}
	err = binary.Read(rbuf, binary.BigEndian, &IntegerToDoubleConversionRatio)
	if err != nil {
		return
	}
	Cookie = r32[0] & ^0xf0
	PayloadLength = r32[1]
	NormalizingIndexOffSet = r32[2]
	NumberOfSignificantValueDigits = r32[3]
	LowestTrackableValue = r64[0]
	HighestTrackableValue = r64[1]
	return
}
