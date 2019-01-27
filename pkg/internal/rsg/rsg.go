// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rsg

import (
	"fmt"
	"math"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/rsg/yacc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RSG is a random syntax generator.
type RSG struct {
	lock  syncutil.Mutex
	src   *rand.Rand
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
		src:   rand.New(rand.NewSource(seed)),
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

// Int63n returns a random int64 in [0,n).
func (r *RSG) Int63n(n int64) int64 {
	r.lock.Lock()
	v := r.src.Int63n(n)
	r.lock.Unlock()
	return v
}

// Intn returns a random int.
func (r *RSG) Intn(n int) int {
	r.lock.Lock()
	v := r.src.Intn(n)
	r.lock.Unlock()
	return v
}

// Int63 returns a random int64.
func (r *RSG) Int63() int64 {
	r.lock.Lock()
	v := r.src.Int63()
	r.lock.Unlock()
	return v
}

// Int returns a random int. It attempts to distribute results among small,
// large, and normal scale numbers.
func (r *RSG) Int() int64 {
	r.lock.Lock()
	var i int64
	switch r.src.Intn(9) {
	case 0:
		i = 0
	case 1:
		i = 1
	case 2:
		i = -1
	case 3:
		i = 2
	case 4:
		i = -2
	case 5:
		i = math.MaxInt64
	case 6:
		// math.MinInt64 isn't a valid integer in SQL
		i = math.MinInt64 + 1
	case 7:
		i = r.src.Int63()
		if r.src.Intn(2) == 1 {
			i = -i
		}
	case 8:
		for v := r.src.Intn(10) + 1; v > 0; v-- {
			i *= 10
			i += r.src.Int63n(10)
		}
		if r.src.Intn(2) == 1 {
			i = -i
		}
	}
	r.lock.Unlock()
	return i
}

// Float64 returns a random float. It is sometimes +/-Inf, NaN, and attempts to
// be distributed among very small, large, and normal scale numbers.
func (r *RSG) Float64() float64 {
	r.lock.Lock()
	v := r.src.Float64()*2 - 1
	switch r.src.Intn(10) {
	case 0:
		v = 0
	case 1:
		v = math.Inf(1)
	case 2:
		v = math.Inf(-1)
	case 3:
		v = math.NaN()
	case 4, 5:
		i := r.src.Intn(50)
		v *= math.Pow10(i)
	case 6, 7:
		i := r.src.Intn(50)
		v *= math.Pow10(-i)
	}
	r.lock.Unlock()
	return v
}

// GenerateRandomArg generates a random, valid, SQL function argument of
// the specified type.
func (r *RSG) GenerateRandomArg(typ types.T) string {
	switch r.Intn(20) {
	case 0:
		return "NULL"
	case 1:
		return fmt.Sprintf("NULL::%s", typ)
	case 2:
		return fmt.Sprintf("(SELECT NULL)::%s", typ)
	}
	coltype, err := sqlbase.DatumTypeToColumnType(typ)
	if err != nil {
		return "NULL"
	}

	r.lock.Lock()
	datum := sqlbase.RandDatumWithNullChance(r.src, coltype, 0)
	r.lock.Unlock()

	return fmt.Sprintf("%s::%s", datum, typ.String())
}
