// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	encoding_binary "encoding/binary"
	"math"
)

// ResetRetainingSlices clears all fields in the InternalTimeSeriesData, but
// retains any backing slices.
func (m *InternalTimeSeriesData) ResetRetainingSlices() {
	m.StartTimestampNanos = 0
	m.SampleDurationNanos = 0
	m.Samples = m.Samples[:0]
	m.Offset = m.Offset[:0]
	m.Last = m.Last[:0]
	m.Count = m.Count[:0]
	m.Sum = m.Sum[:0]
	m.Max = m.Max[:0]
	m.Min = m.Min[:0]
	m.First = m.First[:0]
	m.Variance = m.Variance[:0]
}

type InternalTimeSeriesDataMarshaller struct {
	data *InternalTimeSeriesData
	size int
	// offsetSize is the encoded length of m.Offset.
	offsetSize int
	// countSize is the encoded length of m.Count.
	countSize int
}

func (marshaller *InternalTimeSeriesDataMarshaller) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	m := marshaller.data
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Variance) > 0 {
		for iNdEx := len(m.Variance) - 1; iNdEx >= 0; iNdEx-- {
			f1 := math.Float64bits(float64(m.Variance[iNdEx]))
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(f1))
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.Variance)*8))
		i--
		dAtA[i] = 0x5a
	}
	if len(m.First) > 0 {
		for iNdEx := len(m.First) - 1; iNdEx >= 0; iNdEx-- {
			f2 := math.Float64bits(float64(m.First[iNdEx]))
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(f2))
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.First)*8))
		i--
		dAtA[i] = 0x52
	}
	if len(m.Min) > 0 {
		for iNdEx := len(m.Min) - 1; iNdEx >= 0; iNdEx-- {
			f3 := math.Float64bits(float64(m.Min[iNdEx]))
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(f3))
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.Min)*8))
		i--
		dAtA[i] = 0x4a
	}
	if len(m.Max) > 0 {
		for iNdEx := len(m.Max) - 1; iNdEx >= 0; iNdEx-- {
			f4 := math.Float64bits(float64(m.Max[iNdEx]))
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(f4))
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.Max)*8))
		i--
		dAtA[i] = 0x42
	}
	if len(m.Sum) > 0 {
		for iNdEx := len(m.Sum) - 1; iNdEx >= 0; iNdEx-- {
			f5 := math.Float64bits(float64(m.Sum[iNdEx]))
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(f5))
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.Sum)*8))
		i--
		dAtA[i] = 0x3a
	}
	if len(m.Count) > 0 {
		dAtA7 := make([]byte, len(m.Count)*10)
		var j6 int
		for _, num := range m.Count {
			for num >= 1<<7 {
				dAtA7[j6] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j6++
			}
			dAtA7[j6] = uint8(num)
			j6++
		}
		i -= j6
		copy(dAtA[i:], dAtA7[:j6])
		i = encodeVarintInternal(dAtA, i, uint64(j6))
		i--
		dAtA[i] = 0x32
	}
	if len(m.Last) > 0 {
		for iNdEx := len(m.Last) - 1; iNdEx >= 0; iNdEx-- {
			f8 := math.Float64bits(float64(m.Last[iNdEx]))
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(f8))
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.Last)*8))
		i--
		dAtA[i] = 0x2a
	}
	if len(m.Offset) > 0 {
		dAtA10 := make([]byte, len(m.Offset)*10)
		var j9 int
		for _, num1 := range m.Offset {
			num := uint64(num1)
			for num >= 1<<7 {
				dAtA10[j9] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j9++
			}
			dAtA10[j9] = uint8(num)
			j9++
		}
		i -= j9
		copy(dAtA[i:], dAtA10[:j9])
		i = encodeVarintInternal(dAtA, i, uint64(j9))
		i--
		dAtA[i] = 0x22
	}
	if len(m.Samples) > 0 {
		for iNdEx := len(m.Samples) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Samples[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintInternal(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x1a
		}
	}
	i = encodeVarintInternal(dAtA, i, uint64(m.SampleDurationNanos))
	i--
	dAtA[i] = 0x10
	i = encodeVarintInternal(dAtA, i, uint64(m.StartTimestampNanos))
	i--
	dAtA[i] = 0x8
	return len(dAtA) - i, nil
}

// Size is equivalent to m.data.Size().
func (marshaller *InternalTimeSeriesDataMarshaller) Size() int {
	return marshaller.size
}

func MakeInternalTimeSeriesDataMarshaller(
	m *InternalTimeSeriesData,
) InternalTimeSeriesDataMarshaller {
	panic("unimplemented")
}

// UnmarshalReusingSlices is similar to Unmarshal but it makes an effort to
// reuse any existing slices. This is a copy of InternalTimeSeriesData.Unmarshal
// where we replace `make()` with `slices.Grow`.
func (m *InternalTimeSeriesData) UnmarshalReusingSlices(dAtA []byte) error {
	panic("unimplemented")
}
