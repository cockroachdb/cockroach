// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rsg

import (
	"fmt"
	"math"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/rsg/yacc"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RSG is a random syntax generator.
type RSG struct {
	Rnd *rand.Rand

	lock  syncutil.Mutex
	seen  map[string]bool
	prods map[string][]*yacc.ExpressionNode
}

// NewRSG creates a random syntax generator from the given random seed and
// yacc file.
func NewRSG(seed int64, y string, allowDuplicates bool) (*RSG, error) {
	tree, err := yacc.Parse("sql", y)
	if err != nil {
		return nil, err
	}
	rsg := RSG{
		Rnd:   rand.New(&lockedSource{src: rand.NewSource(seed).(rand.Source64)}),
		prods: make(map[string][]*yacc.ExpressionNode),
	}
	if !allowDuplicates {
		rsg.seen = make(map[string]bool)
	}
	for _, prod := range tree.Productions {
		rsg.prods[prod.Name] = prod.Expressions
	}
	return &rsg, nil
}

// Generate generates a unique random syntax from the root node. At most depth
// levels of token expansion are performed. An empty string is returned on
// error or if depth is exceeded. Generate is safe to call from mulitipule
// goroutines. If Generate is called more times than it can generate unique
// output, it will block forever.
func (r *RSG) Generate(root string, depth int) string {
	for i := 0; i < 100000; i++ {
		s := strings.Join(r.generate(root, depth), " ")
		if r.seen != nil {
			r.lock.Lock()
			if !r.seen[s] {
				r.seen[s] = true
			} else {
				s = ""
			}
			r.lock.Unlock()
		}
		if s != "" {
			s = strings.Replace(s, "_LA", "", -1)
			s = strings.Replace(s, " AS OF SYSTEM TIME \"string\"", "", -1)
			return s
		}
	}
	panic("couldn't find unique string")
}

func (r *RSG) generate(root string, depth int) []string {
	// Initialize to an empty slice instead of nil because nil is the signal
	// that the depth has been exceeded.
	ret := make([]string, 0)
	prods := r.prods[root]
	if len(prods) == 0 {
		return []string{root}
	}
	prod := prods[r.Intn(len(prods))]
	for _, item := range prod.Items {
		switch item.Typ {
		case yacc.TypLiteral:
			v := item.Value[1 : len(item.Value)-1]
			ret = append(ret, v)
		case yacc.TypToken:
			var v []string
			switch item.Value {
			case "IDENT":
				v = []string{"ident"}
			case "c_expr":
				v = r.generate(item.Value, 30)
			case "SCONST":
				v = []string{`'string'`}
			case "ICONST":
				v = []string{fmt.Sprint(r.Intn(1000) - 500)}
			case "FCONST":
				v = []string{fmt.Sprint(r.Float64())}
			case "BCONST":
				v = []string{`b'bytes'`}
			case "BITCONST":
				v = []string{`B'10010'`}
			case "substr_from":
				v = []string{"FROM", `'string'`}
			case "substr_for":
				v = []string{"FOR", `'string'`}
			case "overlay_placing":
				v = []string{"PLACING", `'string'`}
			default:
				if depth == 0 {
					return nil
				}
				v = r.generate(item.Value, depth-1)
			}
			if v == nil {
				return nil
			}
			ret = append(ret, v...)
		default:
			panic("unknown item type")
		}
	}
	return ret
}

// Intn returns a random int.
func (r *RSG) Intn(n int) int {
	return r.Rnd.Intn(n)
}

// Int63 returns a random int64.
func (r *RSG) Int63() int64 {
	return r.Rnd.Int63()
}

// Float64 returns a random float. It is sometimes +/-Inf, NaN, and attempts to
// be distributed among very small, large, and normal scale numbers.
func (r *RSG) Float64() float64 {
	v := r.Rnd.Float64()*2 - 1
	switch r.Rnd.Intn(10) {
	case 0:
		v = 0
	case 1:
		v = math.Inf(1)
	case 2:
		v = math.Inf(-1)
	case 3:
		v = math.NaN()
	case 4, 5:
		i := r.Rnd.Intn(50)
		v *= math.Pow10(i)
	case 6, 7:
		i := r.Rnd.Intn(50)
		v *= math.Pow10(-i)
	}
	return v
}

// GenerateRandomArg generates a random, valid, SQL function argument of
// the specified type.
func (r *RSG) GenerateRandomArg(typ *types.T) string {
	switch r.Intn(20) {
	case 0:
		return "NULL"
	case 1:
		return fmt.Sprintf("NULL::%s", typ)
	case 2:
		return fmt.Sprintf("(SELECT NULL)::%s", typ)
	}

	r.lock.Lock()
	datum := randgen.RandDatumWithNullChance(r.Rnd, typ, 0)
	r.lock.Unlock()

	return tree.Serialize(datum)
}

// lockedSource is a thread safe math/rand.Source. See math/rand/rand.go.
type lockedSource struct {
	lk  syncutil.Mutex
	src rand.Source64
}

func (r *lockedSource) Int63() (n int64) {
	r.lk.Lock()
	n = r.src.Int63()
	r.lk.Unlock()
	return
}

func (r *lockedSource) Uint64() (n uint64) {
	r.lk.Lock()
	n = r.src.Uint64()
	r.lk.Unlock()
	return
}

func (r *lockedSource) Seed(seed int64) {
	r.lk.Lock()
	r.src.Seed(seed)
	r.lk.Unlock()
}
