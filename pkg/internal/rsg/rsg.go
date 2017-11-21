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
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/rsg/yacc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
func NewRSG(seed int64, y string) (*RSG, error) {
	tree, err := yacc.Parse("sql", y)
	if err != nil {
		return nil, err
	}
	rsg := RSG{
		src:   rand.New(rand.NewSource(seed)),
		seen:  make(map[string]bool),
		prods: make(map[string][]*yacc.ExpressionNode),
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
	for {
		s := strings.Join(r.generate(root, depth), " ")
		r.lock.Lock()
		if !r.seen[s] {
			r.seen[s] = true
		} else {
			s = ""
		}
		r.lock.Unlock()
		if s != "" {
			s = strings.Replace(s, "_LA", "", -1)
			s = strings.Replace(s, " AS OF SYSTEM TIME \"string\"", "", -1)
			return s
		}
	}
}

func (r *RSG) generate(root string, depth int) []string {
	var ret []string
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
	if r.Intn(10) == 0 {
		return "NULL"
	}
	var v interface{}
	switch types.UnwrapType(typ) {
	case types.Int:
		v = r.Int()
	case types.Float, types.Decimal:
		v = r.Float64()
	case types.String:
		v = stringArgs[r.Intn(len(stringArgs))]
	case types.Bytes:
		v = fmt.Sprintf("b%s", stringArgs[r.Intn(len(stringArgs))])
	case types.Timestamp, types.TimestampTZ:
		t := timeutil.Unix(0, r.Int63())
		v = fmt.Sprintf(`'%s'`, t.Format(time.RFC3339Nano))
	case types.Bool:
		v = boolArgs[r.Intn(2)]
	case types.Date:
		i := r.Int63()
		i -= r.Int63()
		d := tree.NewDDate(tree.DDate(i))
		v = fmt.Sprintf(`'%s'`, d)
	case types.Time:
		i := r.Int63n(int64(timeofday.Max))
		d := tree.MakeDTime(timeofday.FromInt(i))
		v = fmt.Sprintf(`'%s'`, d)
	case types.Interval:
		d := duration.Duration{Nanos: r.Int63()}
		v = fmt.Sprintf(`'%s'`, &tree.DInterval{Duration: d})
	case types.UUID:
		u := uuid.MakeV4()
		v = fmt.Sprintf(`'%s'`, u)
	case types.INet:
		r.lock.Lock()
		ipAddr := ipaddr.RandIPAddr(r.src)
		r.lock.Unlock()
		v = fmt.Sprintf(`'%s'`, ipAddr)
	case types.Oid,
		types.RegClass,
		types.RegNamespace,
		types.RegProc,
		types.RegProcedure,
		types.RegType,
		types.AnyArray,
		types.Any:
		v = "NULL"
	case types.JSON:
		r.lock.Lock()
		j, err := json.Random(20, r.src)
		r.lock.Unlock()
		if err != nil {
			panic(err)
		}
		v = fmt.Sprintf(`'%s'`, tree.DJSON{JSON: j})
	default:
		// Check types that can't be compared using equality
		switch types.UnwrapType(typ).(type) {
		case types.TTuple,
			types.TArray:
			v = "NULL"
		default:
			panic(fmt.Errorf("unknown arg type: %s (%T)", typ, typ))
		}
	}
	return fmt.Sprintf("%v::%s", v, typ.String())
}

var stringArgs = map[int]string{
	0: `''`,
	1: `'1'`,
	2: `'12345'`,
	3: `'1234567890'`,
	4: `'12345678901234567890'`,
	5: `'123456789123456789123456789123456789123456789123456789123456789123456789'`,
}

var boolArgs = map[int]string{
	0: "false",
	1: "true",
}
