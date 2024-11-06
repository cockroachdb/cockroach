// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	encoding_binary "encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/crlib/crencoding"
)

// IsColumnar returns true if this InternalTimeSeriesData stores its samples
// in columnar format.
func (data *InternalTimeSeriesData) IsColumnar() bool {
	return len(data.Offset) > 0
}

// IsRollup returns true if this InternalTimeSeriesData is both in columnar
// format and contains "rollup" data.
func (data *InternalTimeSeriesData) IsRollup() bool {
	return len(data.Count) > 0
}

// SampleCount returns the number of samples contained in this
// InternalTimeSeriesData.
func (data *InternalTimeSeriesData) SampleCount() int {
	if data.IsColumnar() {
		return len(data.Offset)
	}
	return len(data.Samples)
}

// OffsetForTimestamp returns the offset within this collection that would
// represent the provided timestamp.
func (data *InternalTimeSeriesData) OffsetForTimestamp(timestampNanos int64) int32 {
	return int32((timestampNanos - data.StartTimestampNanos) / data.SampleDurationNanos)
}

// TimestampForOffset returns the timestamp that would represent the provided
// offset in this collection.
func (data *InternalTimeSeriesData) TimestampForOffset(offset int32) int64 {
	return data.StartTimestampNanos + int64(offset)*data.SampleDurationNanos
}

// ResetRetainingSlices clears all fields in the InternalTimeSeriesData, but
// retains any backing slices.
func (data *InternalTimeSeriesData) ResetRetainingSlices() {
	data.StartTimestampNanos = 0
	data.SampleDurationNanos = 0
	data.Samples = data.Samples[:0]
	data.Offset = data.Offset[:0]
	data.Last = data.Last[:0]
	data.Count = data.Count[:0]
	data.Sum = data.Sum[:0]
	data.Max = data.Max[:0]
	data.Min = data.Min[:0]
	data.First = data.First[:0]
	data.Variance = data.Variance[:0]
}

type InternalTimeSeriesDataMarshaller struct {
	data *InternalTimeSeriesData
	size int
	// offsetSize is the encoded length of m.Offset.
	offsetSize int
	// countSize is the encoded length of m.Count.
	countSize int
}

func MakeInternalTimeSeriesDataMarshaller(
	m *InternalTimeSeriesData,
) InternalTimeSeriesDataMarshaller {
	// Calculate the size of the marshalled data.
	n := 0
	var l int
	_ = l
	n += 1 + crencoding.UvarintLen64(uint64(m.StartTimestampNanos))
	n += 1 + crencoding.UvarintLen64(uint64(m.SampleDurationNanos))
	if len(m.Samples) > 0 {
		for _, e := range m.Samples {
			l = e.Size()
			n += 1 + l + crencoding.UvarintLen32(uint32(l))
		}
	}
	offsetSize := 0
	if len(m.Offset) > 0 {
		for _, e := range m.Offset {
			// Apparently int32s are encoded as uint64s instead of uint32s, which
			// unnecessarily adds 5 extra bytes for each negative value :/
			offsetSize += crencoding.UvarintLen64(uint64(e))
		}
		n += 1 + crencoding.UvarintLen32(uint32(offsetSize)) + offsetSize
	}
	if len(m.Last) > 0 {
		n += 1 + crencoding.UvarintLen32(uint32(len(m.Last)*8)) + len(m.Last)*8
	}
	countSize := 0
	if len(m.Count) > 0 {
		for _, e := range m.Count {
			countSize += crencoding.UvarintLen32(e)
		}
		n += 1 + crencoding.UvarintLen32(uint32(countSize)) + countSize
	}
	if len(m.Sum) > 0 {
		n += 1 + crencoding.UvarintLen32(uint32(len(m.Sum)*8)) + len(m.Sum)*8
	}
	if len(m.Max) > 0 {
		n += 1 + crencoding.UvarintLen32(uint32(len(m.Max)*8)) + len(m.Max)*8
	}
	if len(m.Min) > 0 {
		n += 1 + crencoding.UvarintLen32(uint32(len(m.Min)*8)) + len(m.Min)*8
	}
	if len(m.First) > 0 {
		n += 1 + crencoding.UvarintLen32(uint32(len(m.First)*8)) + len(m.First)*8
	}
	if len(m.Variance) > 0 {
		n += 1 + crencoding.UvarintLen32(uint32(len(m.Variance)*8)) + len(m.Variance)*8
	}
	return InternalTimeSeriesDataMarshaller{
		data:       m,
		size:       n,
		offsetSize: offsetSize,
		countSize:  countSize,
	}
}

// Size is equivalent to m.data.Size().
func (marshaller *InternalTimeSeriesDataMarshaller) Size() int {
	return marshaller.size
}

func (marshaller *InternalTimeSeriesDataMarshaller) MarshalToSizedBuffer(dAtA []byte) {
	m := marshaller.data
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Variance) > 0 {
		for iNdEx := len(m.Variance) - 1; iNdEx >= 0; iNdEx-- {
			f1 := math.Float64bits(m.Variance[iNdEx])
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], f1)
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.Variance)*8))
		i--
		dAtA[i] = 0x5a
	}
	if len(m.First) > 0 {
		for iNdEx := len(m.First) - 1; iNdEx >= 0; iNdEx-- {
			f2 := math.Float64bits(m.First[iNdEx])
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], f2)
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.First)*8))
		i--
		dAtA[i] = 0x52
	}
	if len(m.Min) > 0 {
		for iNdEx := len(m.Min) - 1; iNdEx >= 0; iNdEx-- {
			f3 := math.Float64bits(m.Min[iNdEx])
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], f3)
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.Min)*8))
		i--
		dAtA[i] = 0x4a
	}
	if len(m.Max) > 0 {
		for iNdEx := len(m.Max) - 1; iNdEx >= 0; iNdEx-- {
			f4 := math.Float64bits(m.Max[iNdEx])
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], f4)
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.Max)*8))
		i--
		dAtA[i] = 0x42
	}
	if len(m.Sum) > 0 {
		for iNdEx := len(m.Sum) - 1; iNdEx >= 0; iNdEx-- {
			f5 := math.Float64bits(m.Sum[iNdEx])
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], f5)
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.Sum)*8))
		i--
		dAtA[i] = 0x3a
	}
	if len(m.Count) > 0 {
		i -= marshaller.countSize
		// Fast path for the case where all the values are small.
		if marshaller.countSize == len(m.Count) {
			for k, num := range m.Count {
				dAtA[i+k] = uint8(num)
			}
		} else {
			j6 := i
			for _, num := range m.Count {
				for num >= 1<<7 {
					dAtA[j6] = uint8(uint64(num)&0x7f | 0x80)
					num >>= 7
					j6++
				}
				dAtA[j6] = uint8(num)
				j6++
			}
			if buildutil.CrdbTestBuild && j6-i != marshaller.countSize {
				panic("countSize mismatch")
			}
		}
		i = encodeVarintInternal(dAtA, i, uint64(marshaller.countSize))
		i--
		dAtA[i] = 0x32
	}
	if len(m.Last) > 0 {
		for iNdEx := len(m.Last) - 1; iNdEx >= 0; iNdEx-- {
			f8 := math.Float64bits(m.Last[iNdEx])
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], f8)
		}
		i = encodeVarintInternal(dAtA, i, uint64(len(m.Last)*8))
		i--
		dAtA[i] = 0x2a
	}
	if len(m.Offset) > 0 {
		i -= marshaller.offsetSize
		// Fast path for the case where all the values are small.
		if marshaller.offsetSize == len(m.Offset) {
			for k, num := range m.Offset {
				dAtA[i+k] = uint8(num)
			}
		} else {
			j9 := i
			for _, num1 := range m.Offset {
				num := uint64(num1)
				for num >= 1<<7 {
					dAtA[j9] = uint8(num&0x7f | 0x80)
					num >>= 7
					j9++
				}
				dAtA[j9] = uint8(num)
				j9++
			}
			if buildutil.CrdbTestBuild && j9-i != marshaller.offsetSize {
				panic("offsetSize mismatch")
			}
		}
		i = encodeVarintInternal(dAtA, i, uint64(marshaller.offsetSize))
		i--
		dAtA[i] = 0x22
	}
	if len(m.Samples) > 0 {
		for iNdEx := len(m.Samples) - 1; iNdEx >= 0; iNdEx-- {
			{
				// Note: this method never returns an error.
				size, _ := m.Samples[iNdEx].MarshalToSizedBuffer(dAtA[:i])
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
	if i != 0 {
		panic("mismatch in size of marshalled data")
	}
}

// UnmarshalReusingSlices is similar to Unmarshal but it makes an effort to
// reuse any existing slices. This is a copy of InternalTimeSeriesData.Unmarshal
// where we replace `make()` with `slices.Grow`.
func (m *InternalTimeSeriesData) UnmarshalReusingSlices(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowInternal
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: InternalTimeSeriesData: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: InternalTimeSeriesData: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field StartTimestampNanos", wireType)
			}
			m.StartTimestampNanos = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInternal
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.StartTimestampNanos |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SampleDurationNanos", wireType)
			}
			m.SampleDurationNanos = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInternal
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.SampleDurationNanos |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Samples", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInternal
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthInternal
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthInternal
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Samples = append(m.Samples, InternalTimeSeriesSample{})
			if err := m.Samples[len(m.Samples)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Offset = append(m.Offset, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthInternal
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthInternal
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				m.Offset = slices.Grow(m.Offset, elementCount)
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowInternal
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Offset = append(m.Offset, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Offset", wireType)
			}
		case 5:
			if wireType == 1 {
				var v uint64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
				iNdEx += 8
				v2 := math.Float64frombits(v)
				m.Last = append(m.Last, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthInternal
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthInternal
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				elementCount = packedLen / 8
				m.Last = slices.Grow(m.Last, elementCount)
				for iNdEx < postIndex {
					var v uint64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
					iNdEx += 8
					v2 := math.Float64frombits(v)
					m.Last = append(m.Last, v2)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Last", wireType)
			}
		case 6:
			if wireType == 0 {
				var v uint32
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Count = append(m.Count, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthInternal
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthInternal
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				m.Count = slices.Grow(m.Count, elementCount)
				for iNdEx < postIndex {
					var v uint32
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowInternal
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Count = append(m.Count, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Count", wireType)
			}
		case 7:
			if wireType == 1 {
				var v uint64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
				iNdEx += 8
				v2 := math.Float64frombits(v)
				m.Sum = append(m.Sum, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthInternal
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthInternal
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				elementCount = packedLen / 8
				m.Sum = slices.Grow(m.Sum, elementCount)
				for iNdEx < postIndex {
					var v uint64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
					iNdEx += 8
					v2 := math.Float64frombits(v)
					m.Sum = append(m.Sum, v2)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Sum", wireType)
			}
		case 8:
			if wireType == 1 {
				var v uint64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
				iNdEx += 8
				v2 := math.Float64frombits(v)
				m.Max = append(m.Max, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthInternal
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthInternal
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				elementCount = packedLen / 8
				m.Max = slices.Grow(m.Max, elementCount)
				for iNdEx < postIndex {
					var v uint64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
					iNdEx += 8
					v2 := math.Float64frombits(v)
					m.Max = append(m.Max, v2)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Max", wireType)
			}
		case 9:
			if wireType == 1 {
				var v uint64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
				iNdEx += 8
				v2 := math.Float64frombits(v)
				m.Min = append(m.Min, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthInternal
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthInternal
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				elementCount = packedLen / 8
				m.Min = slices.Grow(m.Min, elementCount)
				for iNdEx < postIndex {
					var v uint64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
					iNdEx += 8
					v2 := math.Float64frombits(v)
					m.Min = append(m.Min, v2)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Min", wireType)
			}
		case 10:
			if wireType == 1 {
				var v uint64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
				iNdEx += 8
				v2 := math.Float64frombits(v)
				m.First = append(m.First, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthInternal
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthInternal
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				elementCount = packedLen / 8
				m.First = slices.Grow(m.First, elementCount)
				for iNdEx < postIndex {
					var v uint64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
					iNdEx += 8
					v2 := math.Float64frombits(v)
					m.First = append(m.First, v2)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field First", wireType)
			}
		case 11:
			if wireType == 1 {
				var v uint64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
				iNdEx += 8
				v2 := math.Float64frombits(v)
				m.Variance = append(m.Variance, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowInternal
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthInternal
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthInternal
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				elementCount = packedLen / 8
				m.Variance = slices.Grow(m.Variance, elementCount)
				for iNdEx < postIndex {
					var v uint64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					v = encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:])
					iNdEx += 8
					v2 := math.Float64frombits(v)
					m.Variance = append(m.Variance, v2)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Variance", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipInternal(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthInternal
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
