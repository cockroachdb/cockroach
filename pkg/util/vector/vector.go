// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vector

import (
	"math"
	"math/rand"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
)

// MaxDim is the maximum number of dimensions a vector can have.
const MaxDim = 16000

// T is the type of a PGVector-like vector.
type T []float32

// ParseVector parses the Postgres string representation of a vector.
func ParseVector(input string) (T, error) {
	input = strings.TrimSpace(input)
	if !strings.HasPrefix(input, "[") || !strings.HasSuffix(input, "]") {
		return T{}, pgerror.Newf(pgcode.InvalidTextRepresentation,
			"malformed vector literal: Vector contents must start with \"[\" and"+
				" end with \"]\"")
	}

	input = strings.TrimPrefix(input, "[")
	input = strings.TrimSuffix(input, "]")
	parts := strings.Split(input, ",")

	if len(parts) > MaxDim {
		return T{}, pgerror.Newf(pgcode.ProgramLimitExceeded, "vector cannot have more than %d dimensions", MaxDim)
	}

	vector := make([]float32, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return T{}, pgerror.New(pgcode.InvalidTextRepresentation, "invalid input syntax for type vector: empty string")
		}

		val, err := strconv.ParseFloat(part, 32)
		if err != nil {
			return T{}, pgerror.Newf(pgcode.InvalidTextRepresentation, "invalid input syntax for type vector: %s", part)
		}

		if math.IsInf(val, 0) {
			return T{}, pgerror.New(pgcode.DataException, "infinite value not allowed in vector")
		}
		if math.IsNaN(val) {
			return T{}, pgerror.New(pgcode.DataException, "NaN not allowed in vector")
		}
		vector[i] = float32(val)
	}

	return vector, nil
}

// AsSet returns this vector a set of one vector.
func (v T) AsSet() Set {
	return Set{
		Dims:  len(v),
		Count: 1,
		Data:  slices.Clip(v),
	}
}

// String implements the fmt.Stringer interface.
func (v T) String() string {
	var sb strings.Builder
	// Pre-grow by a reasonable amount to avoid multiple allocations.
	sb.Grow(len(v)*8 + 2)
	sb.WriteString("[")
	for i, v := range v {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(strconv.FormatFloat(float64(v), 'g', -1, 32))
	}
	sb.WriteString("]")
	return sb.String()
}

// Size returns the size of the vector in bytes.
func (v T) Size() uintptr {
	return 24 + uintptr(cap(v))*4
}

// Compare returns -1 if v < v2, 1 if v > v2, and 0 if v == v2.
func (v T) Compare(v2 T) (int, error) {
	n := min(len(v), len(v2))
	for i := 0; i < n; i++ {
		if v[i] < v2[i] {
			return -1, nil
		} else if v[i] > v2[i] {
			return 1, nil
		}
	}
	if len(v) < len(v2) {
		return -1, nil
	} else if len(v) > len(v2) {
		return 1, nil
	}
	return 0, nil
}

// Encode encodes the vector as a byte array suitable for storing in KV.
func Encode(appendTo []byte, t T) ([]byte, error) {
	appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(len(t)))
	for i := range t {
		appendTo = encoding.EncodeUntaggedFloat32Value(appendTo, t[i])
	}
	return appendTo, nil
}

// Decode decodes the byte array into a vector and returns any remaining bytes.
func Decode(b []byte) (remaining []byte, ret T, err error) {
	var n uint32
	b, n, err = encoding.DecodeUint32Ascending(b)
	if err != nil {
		return nil, nil, err
	}
	ret = make(T, n)
	for i := range ret {
		b, ret[i], err = encoding.DecodeUntaggedFloat32Value(b)
		if err != nil {
			return nil, nil, err
		}
	}
	return b, ret, nil
}

// L1Distance returns the L1 (Manhattan) distance between t and t2.
func L1Distance(t T, t2 T) (float64, error) {
	if err := checkDims(t, t2); err != nil {
		return 0, err
	}
	return float64(num32.L1Distance(t, t2)), nil
}

// L2Distance returns the Euclidean distance between t and t2.
func L2Distance(t T, t2 T) (float64, error) {
	if err := checkDims(t, t2); err != nil {
		return 0, err
	}
	// TODO(queries): check for overflow and validate intermediate result if needed.
	// NOTE: This does not use the num32.L2Distance function because it needs to
	// return a float64 value.
	return math.Sqrt(float64(num32.L2SquaredDistance(t, t2))), nil
}

// CosDistance returns the cosine distance between t and t2. This represents the
// similarity between the two vectors, ranging from 0 (most similar) to 2 (least
// similar). Only the angle between the vectors matters; the norms (magnitudes)
// are irrelevant.
func CosDistance(t T, t2 T) (float64, error) {
	if err := checkDims(t, t2); err != nil {
		return 0, err
	}

	// Compute the cosine of the angle between the two vectors as their dot
	// product divided by the product of their norms:
	//      t·t2
	//  -----------
	//  ||t|| ||t2||
	var dot, normA, normB float32
	for i := range t {
		dot += t[i] * t2[i]
		normA += t[i] * t[i]
		normB += t2[i] * t2[i]
	}

	// Use sqrt(a * b) over sqrt(a) * sqrt(b) to compute norms.
	similarity := float64(dot) / math.Sqrt(float64(normA)*float64(normB))

	// Cosine distance = 1 - cosine similarity. Ensure that similarity always
	// stays within [-1, 1] despite any floating point arithmetic error.
	if similarity > 1 {
		similarity = 1
	} else if similarity < -1 {
		similarity = -1
	}
	return 1 - similarity, nil
}

// InnerProduct returns the inner product of t1 and t2.
func InnerProduct(t T, t2 T) (float64, error) {
	if err := checkDims(t, t2); err != nil {
		return 0, err
	}
	return float64(num32.Dot(t, t2)), nil
}

// NegInnerProduct returns the negative inner product of t1 and t2.
func NegInnerProduct(t T, t2 T) (float64, error) {
	p, err := InnerProduct(t, t2)
	return -p, err
}

// Norm returns the L2 norm of t.
func Norm(t T) float64 {
	var norm float64
	for i := range t {
		norm += float64(t[i]) * float64(t[i])
	}
	// TODO(queries): check for overflow and validate intermediate result if needed.
	// NOTE: This does not use the num32.Norm function because it needs to return
	// a float64 value.
	return math.Sqrt(norm)
}

// Add returns t+t2, pointwise.
func Add(t T, t2 T) (T, error) {
	if err := checkDims(t, t2); err != nil {
		return nil, err
	}
	ret := make(T, len(t))
	for i := range t {
		ret[i] = t[i] + t2[i]
	}
	for i := range ret {
		if math.IsInf(float64(ret[i]), 0) {
			return nil, pgerror.New(pgcode.NumericValueOutOfRange, "value out of range: overflow")
		}
	}
	return ret, nil
}

// Minus returns t-t2, pointwise.
func Minus(t T, t2 T) (T, error) {
	if err := checkDims(t, t2); err != nil {
		return nil, err
	}
	ret := make(T, len(t))
	for i := range t {
		ret[i] = t[i] - t2[i]
	}
	for i := range ret {
		if math.IsInf(float64(ret[i]), 0) {
			return nil, pgerror.New(pgcode.NumericValueOutOfRange, "value out of range: overflow")
		}
	}
	return ret, nil
}

// Mult returns t*t2, pointwise.
func Mult(t T, t2 T) (T, error) {
	if err := checkDims(t, t2); err != nil {
		return nil, err
	}
	ret := make(T, len(t))
	for i := range t {
		ret[i] = t[i] * t2[i]
	}
	for i := range ret {
		if math.IsInf(float64(ret[i]), 0) {
			return nil, pgerror.New(pgcode.NumericValueOutOfRange, "value out of range: overflow")
		}
		if ret[i] == 0 && !(t[i] == 0 || t2[i] == 0) {
			return nil, pgerror.New(pgcode.NumericValueOutOfRange, "value out of range: underflow")
		}
	}
	return ret, nil
}

// Random returns a random vector with the number of dimensions in [1, maxDim]
// range.
func Random(rng *rand.Rand, maxDim int) T {
	n := 1 + rng.Intn(maxDim)
	v := make(T, n)
	for i := range v {
		for {
			v[i] = math.Float32frombits(rng.Uint32())
			if math.IsNaN(float64(v[i])) || math.IsInf(float64(v[i]), 0) {
				continue
			}
			break
		}
	}
	return v
}

func checkDims(t T, t2 T) error {
	if len(t) != len(t2) {
		return pgerror.Newf(pgcode.DataException, "different vector dimensions %d and %d", len(t), len(t2))
	}
	return nil
}
