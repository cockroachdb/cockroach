// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

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
