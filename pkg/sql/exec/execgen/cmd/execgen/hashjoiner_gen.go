// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"io"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

const hashJoinerTemplate = `
package exec

import (
  "fmt"
	"bytes"
	"math"
	
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// rehash takes a element of a key (tuple representing a row of equality
// column values) at a given column and computes a new hash by applying a
// transformation to the existing hash.
func (ht *hashTable) rehash(
	buckets []uint64, keyIdx int, t types.T, col ColVec, nKeys uint64, sel []uint16,
) {
	switch t {
	{{range .}}
		case types.{{.ExecType}}:
			keys := col.{{.ExecType}}()
			if sel != nil {
		  	for i := uint64(0); i < nKeys; i++ {
					{{if eq .ExecType "Bool"}}
						if keys[sel[i]] {
							buckets[i] = buckets[i]*31 + 1
						}
					{{else if eq .ExecType "Bytes"}}
					  hash := 1
					  for b := range keys[sel[i]] {
					  	hash = hash*31 + b
					  }
					  buckets[i] = buckets[i]*31 + uint64(hash)
					{{else if (eq .ExecType "Float32")}}
						buckets[i] = buckets[i]*31 + uint64(math.Float32bits(keys[sel[i]]))
					{{else if (eq .ExecType "Float64")}}
						buckets[i] = buckets[i]*31 + math.Float64bits(keys[sel[i]])
					{{else if (eq .ExecType "Decimal")}}
						d, err := keys[sel[i]].Float64()
						if err != nil {
							panic(fmt.Sprintf("%v", err))
						}
						buckets[i] = buckets[i]*31 + math.Float64bits(d)
					{{else}}
					  buckets[i] = buckets[i]*31 + uint64(keys[sel[i]])
					{{end}}
				}
			} else {
		  	for i := uint64(0); i < nKeys; i++ {
					{{if eq .ExecType "Bool"}}
						if keys[i] {
							buckets[i] = buckets[i]*31 + 1
						}
					{{else if eq .ExecType "Bytes"}}
						hash := 1
						for b := range keys[i] {
							hash = hash*31 + b
						}
						buckets[i] = buckets[i]*31 + uint64(hash)
					{{else if (eq .ExecType "Float32")}}
						buckets[i] = buckets[i]*31 + uint64(math.Float32bits(keys[i]))
					{{else if (eq .ExecType "Float64")}}
						buckets[i] = buckets[i]*31 + math.Float64bits(keys[i])
					{{else if (eq .ExecType "Decimal")}}
						d, err := keys[i].Float64()
						if err != nil {
							panic(fmt.Sprintf("%v", err))
						}
						buckets[i] = buckets[i]*31 + math.Float64bits(d)
					{{else}}
						buckets[i] = buckets[i]*31 + uint64(keys[i])
					{{end}}
				}
			}
	{{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", t))
	}
}

// checkCol determines if the current key column in the groupID buckets matches
// the specified equality column key. If there is a match, then the key is added
// to differs. If the bucket has reached the end, the key is rejected. If any
// element in the key is null, then there is no match.
func (prober *hashJoinProber) checkCol(t types.T, keyColIdx int, nToCheck uint16, sel []uint16) {
	switch t {
	{{range .}}
		case types.{{.ExecType}}:
			buildVec := prober.ht.vals[prober.ht.keyCols[keyColIdx]]
			probeVec := prober.keys[keyColIdx]

			buildKeys := buildVec.{{.ExecType}}()
			probeKeys := probeVec.{{.ExecType}}()

			if sel != nil {
				for i := uint16(0); i < nToCheck; i++ {
					// keyID of 0 is reserved to represent the end of the next chain.
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						// the build table key (calculated using keys[keyID - 1] = key) is
						// compared to the corresponding probe table to determine if a match is
						// found.

						if probeVec.NullAt(sel[prober.toCheck[i]]) {
							prober.groupID[prober.toCheck[i]] = 0
						} else if buildVec.NullAt64(keyID-1) {
							prober.differs[prober.toCheck[i]] = true
						} else {
						{{if eq .ExecType "Bytes"}}
							if !bytes.Equal(buildKeys[keyID-1], probeKeys[sel[prober.toCheck[i]]]) {
								prober.differs[prober.toCheck[i]] = true
							}
						{{else if (eq .ExecType "Decimal")}}
							if buildKeys[keyID-1].Cmp(&probeKeys[sel[prober.toCheck[i]]]) != 0 {
								prober.differs[prober.toCheck[i]] = true
							}
						{{else}}
							if buildKeys[keyID-1] != probeKeys[sel[prober.toCheck[i]]] {
								prober.differs[prober.toCheck[i]] = true
							}
						{{end}}
						}
					}
				}
			} else {
				for i := uint16(0); i < nToCheck; i++ {
					// keyID of 0 is reserved to represent the end of the next chain.
					if keyID := prober.groupID[prober.toCheck[i]]; keyID != 0 {
						// the build table key (calculated using keys[keyID - 1] = key) is
						// compared to the corresponding probe table to determine if a match is
						// found.

						if probeVec.NullAt(prober.toCheck[i]) {
							prober.groupID[prober.toCheck[i]] = 0
						} else if buildVec.NullAt64(keyID-1) {
							prober.differs[prober.toCheck[i]] = true
						} else {
						{{if eq .ExecType "Bytes"}}
							if !bytes.Equal(buildKeys[keyID-1], probeKeys[prober.toCheck[i]]) {
								prober.differs[prober.toCheck[i]] = true
							}
						{{else if (eq .ExecType "Decimal")}}
							if buildKeys[keyID-1].Cmp(&probeKeys[prober.toCheck[i]]) != 0 {
								prober.differs[prober.toCheck[i]] = true
							}
						{{else}}
							if buildKeys[keyID-1] != probeKeys[prober.toCheck[i]] {
								prober.differs[prober.toCheck[i]] = true
							}
						{{end}}
						}
					}
				}
			}
	{{end}}
	default:
		panic(fmt.Sprintf("unhandled type %d", t))
	}
}
`

type hashJoinerGen struct {
	ExecType string
	GoType   string
}

func genHashJoinerDistinct(wr io.Writer) error {
	// Build list of all exec types.
	var gens []hashJoinerGen
	for _, t := range types.AllTypes {
		gen := hashJoinerGen{
			ExecType: t.String(),
			GoType:   t.GoTypeName(),
		}
		gens = append(gens, gen)
	}

	tmpl, err := template.New("hashJoinerTemplate").Parse(hashJoinerTemplate)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, gens)
}

func init() {
	registerGenerator(genHashJoinerDistinct, "hashjoiner.eg.go")
}
