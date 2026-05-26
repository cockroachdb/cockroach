// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rspb

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Type constraint definitions for generic mergeSpans.
type (
	// ratchetingValue is a value that can be ratcheted to a higher value. All
	// updates to the value are monotonic.
	ratchetingValue[V any] interface {
		// Both partialCompare and ratchet take a receiver called 'a' and a
		// parameter called 'b'.

		// partialCompare returns an integer comparing the two values.
		// The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
		//
		// The comparison is partial, meaning that not every pair of values is
		// comparable. The boolean return value indicates whether the comparison is
		// supported; if not, false is returned.
		partialCompare(V) (int, bool)

		// ratchet returns the greater of the two values. If the values are not
		// comparable then a third, larger value is returned which is greater than
		// either of the two inputs.
		//
		// ratchet is guaranteed to return the receiver if the receiver is equal to
		// or greater than the parameter, as defined by partialCompare.
		ratchet(V) V
	}

	// span is a range of keys with an associated value.
	span[V ratchetingValue[V]] interface {
		// key returns the start key of the span.
		key() []byte

		// endKey returns the (exclusive) end key of the span, or nil if the span is
		// a point span.
		endKey() []byte

		// value returns the value associated with the span.
		value() V
	}

	// spanPtr is a span that can be mutated.
	spanPtr[V ratchetingValue[V], S span[V]] interface {
		// spanPtr is a pointer to a span.
		*S

		// spanPtr includes the method set of span.
		span[V]

		// setKey sets the start key of the span.
		setKey([]byte)

		// setEndKey sets the (exclusive) end key of the span.
		setEndKey([]byte)

		// setValue sets the value associated with the span.
		setValue(V)
	}
)

// mergeSpans merges the two ordered slices of spans in linear time (O(a+b)) and
// returns a new slice of ordered spans without overlaps. May mutate a and b in
// the process.
//
// The function is generic over the type of span and the value contained in each
// span, to permit testing, future reuse, and clear documentation.
func mergeSpans[V ratchetingValue[V], S span[V], SPtr spanPtr[V, S]](a, b []S) []S {
	// Algorithm:
	// (a) Iterate over a and b in parallel.
	// (b) Compare the starting keys of a and b.
	// (c) If the starting keys are equal:
	//  (c1) Compare the ending keys of a and b.
	//  (c2) Merge the overlapping portion of the spans, ratcheting the value, and
	//       add to the result.
	//  (c3) If non-overlapping portions remain, truncate the longer span to the
	//       end of the shorter span and preserve it for the next iteration.
	// (d) If the starting keys are not equal:
	//  (d1) Add the span with the lesser starting key to the result, up to either
	//       the end of that span or the start of the first overlapping span in
	//       the other slice that has a greater or incomparable value, whichever
	//       comes first.
	//  (d2) Discard overlapping spans in the other slice that have a lesser or
	//       equal value.
	res := make([]S, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		aKey, bKey := a[i].key(), b[j].key()
		cmp := bytes.Compare(aKey, bKey)
		if cmp == 0 {
			// Same starting key. Compare end keys.
			var endCmp int
			aEndKey, bEndKey := a[i].endKey(), b[j].endKey()
			isPoint := func(key, endKey []byte) bool {
				if len(endKey) == 0 {
					return true
				}
				// endKey is non-empty. Check if endKey == key.Next(), which we'll call
				// an "effective" point span.
				// We do this so that a span with an empty endKey is considered equal to
				// a span with a non-empty endKey which is equal to key.Next(), for the
				// purposes of endCmp.
				return endKey[len(endKey)-1] == 0 && bytes.Equal(key, endKey[:len(endKey)-1])
			}
			aIsPoint, bIsPoint := isPoint(aKey, aEndKey), isPoint(bKey, bEndKey)
			if aIsPoint && bIsPoint {
				// Both are point spans.
				endCmp = 0
			} else if aIsPoint {
				// a is a point span. a < b.
				endCmp = -1
			} else if bIsPoint {
				// b is a point span. a > b.
				endCmp = +1
			} else {
				// Neither are point spans. Compare end keys.
				endCmp = bytes.Compare(aEndKey, bEndKey)
			}

			if endCmp == 0 {
				// Same ending key. Ratchet value and append.
				mergedVal := a[i].value().ratchet(b[j].value())
				res = append(res, a[i])
				merged := SPtr(&res[len(res)-1])
				merged.setValue(mergedVal)
				if aIsPoint {
					// If a is a point span, set the merged span's end key to nil. This
					// doesn't look like it should be needed, but it avoids confusion when
					// a is an "effective" point span (see above). This ensures that
					// mergedSpans is symmetric with respect to the order of its
					// arguments.
					merged.setEndKey(nil)
				}
				i++
				j++
			} else {
				// Different ending key.
				short, shortIdx, long := SPtr(&a[i]), &i, SPtr(&b[j])
				if endCmp > 0 {
					short, shortIdx, long = SPtr(&b[j]), &j, SPtr(&a[i])
				}
				// short ends before long.
				if valCmp, ok := long.value().partialCompare(short.value()); ok && valCmp >= 0 {
					// short can be discarded entirely.
					*shortIdx++
					// Preserve longIdx for the next iteration.
				} else {
					// Merge short and long together and truncate to short's end key.
					shortEndKey := short.endKey()
					mergedVal := short.value().ratchet(long.value())
					res = append(res, *long)
					longTrunc := SPtr(&res[len(res)-1])
					longTrunc.setEndKey(shortEndKey)
					longTrunc.setValue(mergedVal)
					// short has been consumed.
					*shortIdx++
					// Update long to start at to the exclusive end key of the merged span
					// and preserve it for the next iteration. Take care with point spans.
					if len(shortEndKey) != 0 {
						// short is a ranged span, so start long at the end of short.
						long.setKey(shortEndKey)
					} else {
						// short is a point span, so start long at the next key.
						long.setKey(encoding.BytesNext(short.key())) // Key.Next()
					}
				}
			}
		} else {
			// Different starting key.
			left, leftIdx, rightSlice, rightIdx := SPtr(&a[i]), &i, b, &j
			if cmp > 0 {
				left, leftIdx, rightSlice, rightIdx = SPtr(&b[j]), &j, a, &i
			}
			// left starts before right.
			addLeft := true
			leftEndKey := left.endKey()
			if len(leftEndKey) != 0 {
				// If left is ranged, add as much of left as possible.
				for *rightIdx < len(rightSlice) {
					right := SPtr(&rightSlice[*rightIdx])
					rightKey := right.key()
					overlap := bytes.Compare(leftEndKey, rightKey) > 0
					if !overlap {
						// right starts after left ends.
						break
					}
					// left and right overlap.
					if valCmp, ok := left.value().partialCompare(right.value()); !ok || valCmp < 0 {
						// Left's value is less than or incomparable to right's. Truncate
						// left to the start of right and add to result.
						res = append(res, *left)
						leftTrunc := SPtr(&res[len(res)-1])
						leftTrunc.setEndKey(rightKey)
						// Preserve the right half of left for the next iteration. This
						// guarantees we hit the cmp == 0 case above next time around.
						addLeft = false
						left.setKey(rightKey)
						break
					}
					// left's value is equal or greater than right's and can replace its
					// overlapping portions without a value merge. This comparison works
					// even if right is a point span, where EndKey is zero-length (nil or
					// empty).
					contains := bytes.Compare(leftEndKey, right.endKey()) >= 0
					if !contains {
						// right is partially contained in left. Truncate right to the end
						// of left and stop iterating over rightSlice.
						right.setKey(leftEndKey)
						break
					}
					// right can be discarded entirely. It has an equal or smaller value
					// than left and is fully contained in left.
					*rightIdx++
				}
			}
			// If we still want to add left, do so and advance leftIdx.
			if addLeft {
				res = append(res, *left)
				*leftIdx++
			}
		}
	}
	// Consume remaining spans.
	for ; i < len(a); i++ {
		res = append(res, a[i])
	}
	for ; j < len(b); j++ {
		res = append(res, b[j])
	}
	return res
}

// readSpanVal implements ratchetingValue[readSpanVal].
type readSpanVal struct {
	ts    hlc.Timestamp
	txnID uuid.UUID
}

// The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
func (a readSpanVal) partialCompare(b readSpanVal) (int, bool) {
	// Compare timestamp.
	if a.ts.Less(b.ts) {
		return -1, true
	}
	if b.ts.Less(a.ts) {
		return +1, true
	}
	// Same timestamp, compare txn IDs.
	if a.txnID == b.txnID {
		return 0, true
	}
	if a.txnID == uuid.Nil {
		return +1, true
	}
	if b.txnID == uuid.Nil {
		return -1, true
	}
	// Same timestamp, different non-nil txn IDs. Incomparable.
	return 0, false
}

func (a readSpanVal) ratchet(b readSpanVal) readSpanVal {
	// Compare timestamp.
	if a.ts.Less(b.ts) {
		return b
	}
	if b.ts.Less(a.ts) {
		return a
	}
	// Same timestamp, compare txn IDs.
	if a.txnID == b.txnID {
		return a
	}
	noTxn := a
	noTxn.txnID = uuid.Nil
	return noTxn
}

var _ ratchetingValue[readSpanVal] = readSpanVal{}

// ReadSpan implements span[readSpanVal].
func (s ReadSpan) key() []byte        { return s.Key }
func (s ReadSpan) endKey() []byte     { return s.EndKey }
func (s ReadSpan) value() readSpanVal { return readSpanVal{ts: s.Timestamp, txnID: s.TxnID} }

var _ span[readSpanVal] = ReadSpan{}

// *ReadSpan implements spanPtr[readSpanVal, ReadSpan].
func (s *ReadSpan) setKey(k []byte)        { s.Key = k }
func (s *ReadSpan) setEndKey(k []byte)     { s.EndKey = k }
func (s *ReadSpan) setValue(v readSpanVal) { s.Timestamp = v.ts; s.TxnID = v.txnID }

// Go won't let us perform this compile-time assertion.
// var _ spanPtr[readSpanVal, ReadSpan] = &ReadSpan{}
