package goparquet

import (
	"bytes"
	"encoding/binary"
	"math"
)

type nilStats struct{}

func (s *nilStats) minValue() []byte {
	return nil
}

func (s *nilStats) maxValue() []byte {
	return nil
}

func (s *nilStats) reset() {
}

type statistics struct {
	min []byte
	max []byte
}

func (s *statistics) minValue() []byte {
	return s.min
}

func (s *statistics) maxValue() []byte {
	return s.max
}

func (s *statistics) reset() {
	s.min, s.max = nil, nil
}

func (s *statistics) setMinMax(j []byte) {
	if s.max == nil || s.min == nil {
		s.min = j
		s.max = j
		return
	}

	if bytes.Compare(j, s.min) < 0 {
		s.min = j
	}
	if bytes.Compare(j, s.max) > 0 {
		s.max = j
	}
}

type floatStats struct {
	min float32
	max float32
}

func newFloatStats() *floatStats {
	s := &floatStats{}
	s.reset()
	return s
}

func (s *floatStats) reset() {
	s.min = math.MaxFloat32
	s.max = -math.MaxFloat32
}

func (s *floatStats) minValue() []byte {
	if s.min == math.MaxFloat32 {
		return nil
	}
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, math.Float32bits(s.min))
	return ret
}

func (s *floatStats) maxValue() []byte {
	if s.max == -math.MaxFloat32 {
		return nil
	}
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, math.Float32bits(s.max))
	return ret
}

func (s *floatStats) setMinMax(j float32) {
	if j < s.min {
		s.min = j
	}
	if j > s.max {
		s.max = j
	}
}

type doubleStats struct {
	min float64
	max float64
}

func newDoubleStats() *doubleStats {
	s := &doubleStats{}
	s.reset()
	return s
}

func (s *doubleStats) reset() {
	s.min = math.MaxFloat64
	s.max = -math.MaxFloat64
}

func (s *doubleStats) minValue() []byte {
	if s.min == math.MaxFloat64 {
		return nil
	}
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, math.Float64bits(s.min))
	return ret
}

func (s *doubleStats) maxValue() []byte {
	if s.max == -math.MaxFloat64 {
		return nil
	}
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, math.Float64bits(s.max))
	return ret
}

func (s *doubleStats) setMinMax(j float64) {
	if j < s.min {
		s.min = j
	}
	if j > s.max {
		s.max = j
	}
}

type int32Stats struct {
	min int32
	max int32
}

func newInt32Stats() *int32Stats {
	s := &int32Stats{}
	s.reset()
	return s
}

func (s *int32Stats) reset() {
	s.min = math.MaxInt32
	s.max = math.MinInt32
}

func (s *int32Stats) minValue() []byte {
	if s.min == math.MaxInt32 {
		return nil
	}
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, uint32(s.min))
	return ret
}

func (s *int32Stats) maxValue() []byte {
	if s.max == math.MinInt32 {
		return nil
	}
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, uint32(s.max))
	return ret
}

func (s *int32Stats) setMinMax(j int32) {
	if j < s.min {
		s.min = j
	}
	if j > s.max {
		s.max = j
	}
}

type int64Stats struct {
	min int64
	max int64
}

func newInt64Stats() *int64Stats {
	s := &int64Stats{}
	s.reset()
	return s
}

func (s *int64Stats) reset() {
	s.min = math.MaxInt64
	s.max = math.MinInt64
}

func (s *int64Stats) minValue() []byte {
	if s.min == math.MaxInt64 {
		return nil
	}
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, uint64(s.min))
	return ret
}

func (s *int64Stats) maxValue() []byte {
	if s.min == math.MinInt64 {
		return nil
	}
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, uint64(s.max))
	return ret
}

func (s *int64Stats) setMinMax(j int64) {
	if j < s.min {
		s.min = j
	}
	if j > s.max {
		s.max = j
	}
}
