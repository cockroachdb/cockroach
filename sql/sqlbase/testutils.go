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
//
// Author: Andrei Matei (andreimatei1@gmail.com)
// Author: Radu Berinde (radu@cockroachlabs.com)

package sqlbase

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/duration"
	"gopkg.in/inf.v0"
)

// This file contains utility functions for tests (in other packages).

// GetTableDescriptor retrieves a table descriptor directly from the KV layer.
func GetTableDescriptor(kvDB *client.DB, database string, table string) *TableDescriptor {
	dbNameKey := MakeNameMetadataKey(keys.RootNamespaceID, database)
	gr, err := kvDB.Get(dbNameKey)
	if err != nil {
		panic(err)
	}
	if !gr.Exists() {
		panic("database missing")
	}
	dbDescID := ID(gr.ValueInt())

	tableNameKey := MakeNameMetadataKey(dbDescID, table)
	gr, err = kvDB.Get(tableNameKey)
	if err != nil {
		panic(err)
	}
	if !gr.Exists() {
		panic("table missing")
	}

	descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
	desc := &Descriptor{}
	if err := kvDB.GetProto(descKey, desc); err != nil {
		panic("proto missing")
	}
	return desc.GetTable()
}

// RandDatum generates a random Datum of the given type.
// If null is true, the datum can be DNull.
func RandDatum(rng *rand.Rand, typ ColumnType_Kind, null bool) parser.Datum {
	if null && rng.Intn(10) == 0 {
		return parser.DNull
	}
	switch typ {
	case ColumnType_BOOL:
		return parser.MakeDBool(rng.Intn(2) == 1)
	case ColumnType_INT:
		return parser.NewDInt(parser.DInt(rng.Int63()))
	case ColumnType_FLOAT:
		return parser.NewDFloat(parser.DFloat(rng.NormFloat64()))
	case ColumnType_DECIMAL:
		d := &parser.DDecimal{}
		d.Dec.SetScale(inf.Scale(rng.Intn(40) - 20))
		d.Dec.SetUnscaled(rng.Int63())
		return d
	case ColumnType_DATE:
		return parser.NewDDate(parser.DDate(rng.Intn(10000)))
	case ColumnType_TIMESTAMP:
		return &parser.DTimestamp{Time: time.Unix(rng.Int63n(1000000), rng.Int63n(1000000))}
	case ColumnType_INTERVAL:
		return &parser.DInterval{Duration: duration.Duration{Months: rng.Int63n(1000),
			Days:  rng.Int63n(1000),
			Nanos: rng.Int63n(1000000),
		}}
	case ColumnType_STRING:
		// Generate a random ASCII string.
		p := make([]byte, rng.Intn(10))
		for i := range p {
			p[i] = byte(1 + rng.Intn(127))
		}
		return parser.NewDString(string(p))
	case ColumnType_BYTES:
		p := make([]byte, rng.Intn(10))
		_, _ = rng.Read(p)
		return parser.NewDBytes(parser.DBytes(p))
	case ColumnType_TIMESTAMPTZ:
		return &parser.DTimestampTZ{Time: time.Unix(rng.Int63n(1000000), rng.Int63n(1000000))}
	default:
		panic(fmt.Sprintf("invalid type %s", typ))
	}
}

// RandColumnType returns a random ColumnType_Kind value.
func RandColumnType(rng *rand.Rand) ColumnType_Kind {
	return ColumnType_Kind(rng.Intn(len(ColumnType_Kind_value)))
}

// RandDatumEncoding returns a random DatumEncoding value.
func RandDatumEncoding(rng *rand.Rand) DatumEncoding {
	return DatumEncoding(rng.Intn(len(DatumEncoding_value)))
}

// RandEncDatum generates a random EncDatum (of a random type).
func RandEncDatum(rng *rand.Rand) EncDatum {
	typ := RandColumnType(rng)
	datum := RandDatum(rng, typ, true)
	var ed EncDatum
	ed.SetDatum(typ, datum)
	return ed
}
