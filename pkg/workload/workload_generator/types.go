// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// defaultUniqueCap is the default history size for UniqueWrapper.
	defaultUniqueCap = 100_000

	// defaultJSONMinLen/MaxLen are fallbacks when no min/max are provided.
	defaultJSONMinLen = 10
	defaultJSONMaxLen = 50
)

// Generator is the interface for any workload value generator.
// Each call to Next() returns the next string value (or "" to signal NULL)
type Generator interface {
	Next() string
}

// ─── Basic Generators ──────────────────────────────────────────────────

// SequenceGen yields a sequence of integers starting from 0.
type SequenceGen struct {
	cur int
}

func (g *SequenceGen) Next() string {
	v := g.cur
	g.cur++
	return strconv.Itoa(v)
}

// IntegerGen yields random integers in a given range.
type IntegerGen struct {
	r        *rand.Rand
	min, max int
	nullPct  float64
}

func (g *IntegerGen) Next() string {
	if g.r.Float64() < g.nullPct {
		return ""
	}
	return strconv.Itoa(g.r.Intn(g.max-g.min+1) + g.min)
}

// FloatGen yields random floating-point numbers in a given range and rounds off to a decided precision.
type FloatGen struct {
	r        *rand.Rand
	min, max float64
	round    int
	nullPct  float64
}

func (g *FloatGen) Next() string {
	if g.r.Float64() < g.nullPct {
		return ""
	}
	v := g.r.Float64()*(g.max-g.min) + g.min
	return fmt.Sprintf("%.*f", g.round, v)
}

// StringGen yields random ASCII strings of a given length range.
type StringGen struct {
	r        *rand.Rand
	min, max int
	nullPct  float64
}

func (g *StringGen) Next() string {
	if g.r.Float64() < g.nullPct {
		return ""
	}
	length := g.r.Intn(g.max-g.min+1) + g.min
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = byte('a' + g.r.Intn(26))
	}
	return string(buf)
}

// TimestampGen yields random timestamps between a start and end.
type TimestampGen struct {
	r       *rand.Rand
	startNS int64
	spanNS  int64
	layout  string
	nullPct float64
}

func (g *TimestampGen) Next() string {
	if g.r.Float64() < g.nullPct {
		return ""
	}
	// pick an offset in [0, spanNS)
	offset := g.r.Int63n(g.spanNS)
	t := timeutil.Unix(0, g.startNS+offset)
	return t.Format(g.layout)
}

// UUIDGen yields a deterministic “UUID” hex string from the RNG.
type UUIDGen struct {
	r *rand.Rand
}

func (g *UUIDGen) Next() string {
	buf := make([]byte, 16)
	for i := range buf {
		buf[i] = byte(g.r.Intn(256))
	}
	// hex without dashes is fine for now
	return hex.EncodeToString(buf)
}

// BoolGen yields random boolean values as "1" or "0", with a chance of null.
type BoolGen struct {
	r       *rand.Rand
	nullPct float64
}

func (g *BoolGen) Next() string {
	// nullability
	if g.nullPct > 0 && g.r.Float64() < g.nullPct {
		return ""
	}
	// 1 if random>0.5 else 0
	if g.r.Float64() > 0.5 {
		return "1"
	}
	return "0"
}

// JsonGen wraps a StringGen to produce JSON-like strings.
type JsonGen struct {
	strGen *StringGen
}

func (g *JsonGen) Next() string {
	// get the underlying ASCII string
	v := g.strGen.Next()
	if v == "" {
		return "" // preserve null_pct from the StringGen
	}
	// wrap it in {"k":"…"}
	return fmt.Sprintf(`{"k":"%s"}`, v)
}

// BitGen emits either nil (per nullPct) or a string like "101001".
type BitGen struct {
	r       *rand.Rand
	size    int
	nullPct float64
}

func (b *BitGen) Next() string {
	if b.r.Float64() < b.nullPct {
		return ""
	}
	var sb strings.Builder
	for i := 0; i < b.size; i++ {
		if b.r.Intn(2) == 0 {
			sb.WriteByte('0')
		} else {
			sb.WriteByte('1')
		}
	}
	return sb.String()
}

type BytesGen struct {
	r        *rand.Rand
	min, max int
	nullPct  float64
}

func (b *BytesGen) Next() string {
	if b.r.Float64() < b.nullPct {
		return ""
	}

	length := b.min
	buf := make([]byte, length)
	b.r.Read(buf) // fill with random bytes
	return string(buf)
}

// ─── Wrappers ──────────────────────────────────────────────────────────

// DefaultWrapper wraps a base Generator and emits a fixed literal
// with a given probability, otherwise delegates to the base generator.
type DefaultWrapper struct {
	base    Generator  // base is the underlying generator to delegate to.
	prob    float64    // prob is the probability of emitting the literal instead.
	literal string     // literal is the fixed value to emit when the coin flip hits.
	rng     *rand.Rand // rng set its own RNG so that calls are reproducible.
}

func NewDefaultWrapper(base Generator, prob float64, literal string, seed int64) *DefaultWrapper {
	return &DefaultWrapper{
		base:    base,
		prob:    prob,
		literal: literal,
		rng:     rand.New(rand.NewSource(seed)),
	}
}
func (w *DefaultWrapper) Next() string {
	if w.rng.Float64() < w.prob {
		// “hit” — return the literal value
		return w.literal
	}
	// “miss” — delegate to the underlying generator
	return w.base.Next()
}

// UniqueWrapper wraps a base Generator and ensures
// no value is repeated within the last `capacity` outputs.
type UniqueWrapper struct {
	base     Generator           // base is the underlying generator to delegate to.
	seen     map[string]struct{} // seen is a set of recently inserted values.
	order    []string            // order is a queue of the order in which values were inserted.
	capacity int                 // capacity is the max number of recent values to remember.
}

// NewUniqueWrapper creates a UniqueWrapper around baseGen.
// capacity is the max number of recent values to remember.
func NewUniqueWrapper(baseGen Generator, capacity int) *UniqueWrapper {
	return &UniqueWrapper{
		base:     baseGen,
		seen:     make(map[string]struct{}, capacity),
		order:    make([]string, 0, capacity),
		capacity: capacity,
	}
}

// Next returns the next unique value, re-rolling until it
// finds one not in the recent window.
func (u *UniqueWrapper) Next() string {
	for {
		v := u.base.Next()
		if _, exists := u.seen[v]; !exists {
			// accept it
			u.seen[v] = struct{}{}
			u.order = append(u.order, v)
			// evict oldest value if over capacity
			if len(u.order) > u.capacity {
				oldest := u.order[0]
				u.order = u.order[1:]
				delete(u.seen, oldest)
			}
			return v
		}
		// otherwise: value was recently seen → retry
	}
}

// FkWrapper takes a parent Generator and repeats each parent value
// exactly fanout times before advancing.
type FkWrapper struct {
	parent  Generator // parent is the base generator to repeat values from.
	fanout  int       // fanout decides how many repeats of each parent value.
	counter int       // counter keeps track of how many times we've already returned current.
	current string    // the current value from the parent generator.
}

// NewFkWrapper wraps a baseGen so that each value it produces
// is repeated fanout times.
func NewFkWrapper(baseGen Generator, fanout int) *FkWrapper {
	if fanout < 1 {
		fanout = 1
	}
	w := &FkWrapper{parent: baseGen, fanout: fanout}
	// prime the pump
	w.current = w.parent.Next()
	w.counter = 0
	return w
}

// Next returns the current parent value, and only when
// it has been returned fanout times does it advance to the next.
func (w *FkWrapper) Next() string {
	if w.counter >= w.fanout {
		w.current = w.parent.Next()
		w.counter = 0
	}
	w.counter++
	return w.current
}
