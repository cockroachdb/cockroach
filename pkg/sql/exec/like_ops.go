// Copyright 2019 The Cockroach Authors.
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

package exec

import (
	"bytes"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// GetLikeOperator returns a selection operator which applies the specified LIKE
// pattern. The implementation varies depending on the complexity of the
// pattern.
func GetLikeOperator(
	ctx *tree.EvalContext, input Operator, colIdx int, pattern string,
) (Operator, error) {
	if pattern == "" {
		return &selEQBytesBytesConstOp{
			input:    input,
			colIdx:   colIdx,
			constArg: []byte{},
		}, nil
	}
	if pattern == "%" {
		// Matches everything.
		return NewNoop(input), nil
	}
	if len(pattern) > 1 && !strings.ContainsAny(pattern[1:len(pattern)-1], "_%") {
		// Special cases for patterns which are just a prefix or suffix.
		if pattern[0] == '%' {
			return &selBytesSuffixOp{
				input:  input,
				colIdx: colIdx,
				suffix: []byte(pattern[1:]),
			}, nil
		}
		if pattern[len(pattern)-1] == '%' {
			return &selBytesPrefixOp{
				input:  input,
				colIdx: colIdx,
				prefix: []byte(pattern[:len(pattern)-1]),
			}, nil
		}
	}
	// Default (slow) case: execute as a regular expression match.
	re, err := tree.ConvertLikeToRegexp(ctx, pattern, false, '\\')
	if err != nil {
		return nil, err
	}
	return &selBytesRegexpOp{
		input:   input,
		colIdx:  colIdx,
		pattern: re,
	}, nil
}

// TODO(solon): The following operators should ideally be templated along with
// the other selection operators in selection_ops_gen.go. This is a bit awkward
// to do because they don't map one-to-one onto ComparisonOperators like the
// other operators.

type selBytesPrefixOp struct {
	input Operator

	colIdx int
	prefix []byte
}

func (p selBytesPrefixOp) Init() {
	p.input.Init()
}

func (p *selBytesPrefixOp) Next() coldata.Batch {
	for {
		batch := p.input.Next()
		if batch.Length() == 0 {
			return batch
		}

		coldata := batch.ColVec(p.colIdx).Bytes()[:coldata.BatchSize]
		var idx uint16
		n := batch.Length()
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				cmp := bytes.HasPrefix(coldata[i], p.prefix)
				if cmp {
					sel[idx] = i
					idx++
				}
			}
		} else {
			batch.SetSelection(true)
			sel := batch.Selection()
			for i := uint16(0); i < n; i++ {
				cmp := bytes.HasPrefix(coldata[i], p.prefix)
				if cmp {
					sel[idx] = i
					idx++
				}
			}
		}
		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}

type selBytesSuffixOp struct {
	input Operator

	colIdx int
	suffix []byte
}

func (p selBytesSuffixOp) Init() {
	p.input.Init()
}

func (p *selBytesSuffixOp) Next() coldata.Batch {
	for {
		batch := p.input.Next()
		if batch.Length() == 0 {
			return batch
		}

		coldata := batch.ColVec(p.colIdx).Bytes()[:coldata.BatchSize]
		var idx uint16
		n := batch.Length()
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				cmp := bytes.HasSuffix(coldata[i], p.suffix)
				if cmp {
					sel[idx] = i
					idx++
				}
			}
		} else {
			batch.SetSelection(true)
			sel := batch.Selection()
			for i := uint16(0); i < n; i++ {
				cmp := bytes.HasSuffix(coldata[i], p.suffix)
				if cmp {
					sel[idx] = i
					idx++
				}
			}
		}
		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}

type selBytesRegexpOp struct {
	input Operator

	colIdx  int
	pattern *regexp.Regexp
}

func (p selBytesRegexpOp) Init() {
	p.input.Init()
}

func (p *selBytesRegexpOp) Next() coldata.Batch {
	for {
		batch := p.input.Next()
		if batch.Length() == 0 {
			return batch
		}

		coldata := batch.ColVec(p.colIdx).Bytes()[:coldata.BatchSize]
		var idx uint16
		n := batch.Length()
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				cmp := p.pattern.Match(coldata[i])
				if cmp {
					sel[idx] = i
					idx++
				}
			}
		} else {
			batch.SetSelection(true)
			sel := batch.Selection()
			for i := uint16(0); i < n; i++ {
				cmp := p.pattern.Match(coldata[i])
				if cmp {
					sel[idx] = i
					idx++
				}
			}
		}
		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}
