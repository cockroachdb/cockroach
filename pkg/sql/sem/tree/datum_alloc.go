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

package tree

// DatumAlloc provides batch allocation of datum pointers, amortizing the cost
// of the allocations.
type DatumAlloc struct {
	datumAlloc        []Datum
	dintAlloc         []DInt
	dfloatAlloc       []DFloat
	dstringAlloc      []DString
	dbytesAlloc       []DBytes
	dbitArrayAlloc    []DBitArray
	ddecimalAlloc     []DDecimal
	ddateAlloc        []DDate
	dtimeAlloc        []DTime
	dtimestampAlloc   []DTimestamp
	dtimestampTzAlloc []DTimestampTZ
	dintervalAlloc    []DInterval
	duuidAlloc        []DUuid
	dipnetAlloc       []DIPAddr
	djsonAlloc        []DJSON
	dtupleAlloc       []DTuple
	doidAlloc         []DOid
	scratch           []byte
	env               CollationEnvironment
}

const datumAllocSize = 16      // Arbitrary, could be tuned.
const datumAllocMultiplier = 4 // Arbitrary, could be tuned.

// NewDatums allocates Datums of the specified size.
func (a *DatumAlloc) NewDatums(num int) Datums {
	buf := &a.datumAlloc
	if len(*buf) < num {
		extensionSize := datumAllocSize
		if extTupleLen := num * datumAllocMultiplier; extensionSize < extTupleLen {
			extensionSize = extTupleLen
		}
		*buf = make(Datums, extensionSize)
	}
	r := (*buf)[:num]
	*buf = (*buf)[num:]
	return r
}

// NewDInt allocates a DInt.
func (a *DatumAlloc) NewDInt(v DInt) *DInt {
	buf := &a.dintAlloc
	if len(*buf) == 0 {
		*buf = make([]DInt, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDFloat allocates a DFloat.
func (a *DatumAlloc) NewDFloat(v DFloat) *DFloat {
	buf := &a.dfloatAlloc
	if len(*buf) == 0 {
		*buf = make([]DFloat, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDString allocates a DString.
func (a *DatumAlloc) NewDString(v DString) *DString {
	buf := &a.dstringAlloc
	if len(*buf) == 0 {
		*buf = make([]DString, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDName allocates a DName.
func (a *DatumAlloc) NewDName(v DString) Datum {
	return NewDNameFromDString(a.NewDString(v))
}

// NewDBytes allocates a DBytes.
func (a *DatumAlloc) NewDBytes(v DBytes) *DBytes {
	buf := &a.dbytesAlloc
	if len(*buf) == 0 {
		*buf = make([]DBytes, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDBitArray allocates a DBitArray.
func (a *DatumAlloc) NewDBitArray(v DBitArray) *DBitArray {
	buf := &a.dbitArrayAlloc
	if len(*buf) == 0 {
		*buf = make([]DBitArray, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDecimal allocates a DDecimal.
func (a *DatumAlloc) NewDDecimal(v DDecimal) *DDecimal {
	buf := &a.ddecimalAlloc
	if len(*buf) == 0 {
		*buf = make([]DDecimal, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDate allocates a DDate.
func (a *DatumAlloc) NewDDate(v DDate) *DDate {
	buf := &a.ddateAlloc
	if len(*buf) == 0 {
		*buf = make([]DDate, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTime allocates a DTime.
func (a *DatumAlloc) NewDTime(v DTime) *DTime {
	buf := &a.dtimeAlloc
	if len(*buf) == 0 {
		*buf = make([]DTime, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestamp allocates a DTimestamp.
func (a *DatumAlloc) NewDTimestamp(v DTimestamp) *DTimestamp {
	buf := &a.dtimestampAlloc
	if len(*buf) == 0 {
		*buf = make([]DTimestamp, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestampTZ allocates a DTimestampTZ.
func (a *DatumAlloc) NewDTimestampTZ(v DTimestampTZ) *DTimestampTZ {
	buf := &a.dtimestampTzAlloc
	if len(*buf) == 0 {
		*buf = make([]DTimestampTZ, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDInterval allocates a DInterval.
func (a *DatumAlloc) NewDInterval(v DInterval) *DInterval {
	buf := &a.dintervalAlloc
	if len(*buf) == 0 {
		*buf = make([]DInterval, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDUuid allocates a DUuid.
func (a *DatumAlloc) NewDUuid(v DUuid) *DUuid {
	buf := &a.duuidAlloc
	if len(*buf) == 0 {
		*buf = make([]DUuid, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDIPAddr allocates a DIPAddr.
func (a *DatumAlloc) NewDIPAddr(v DIPAddr) *DIPAddr {
	buf := &a.dipnetAlloc
	if len(*buf) == 0 {
		*buf = make([]DIPAddr, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDJSON allocates a DJSON.
func (a *DatumAlloc) NewDJSON(v DJSON) *DJSON {
	buf := &a.djsonAlloc
	if len(*buf) == 0 {
		*buf = make([]DJSON, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTuple allocates a DTuple.
func (a *DatumAlloc) NewDTuple(v DTuple) *DTuple {
	buf := &a.dtupleAlloc
	if len(*buf) == 0 {
		*buf = make([]DTuple, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDOid allocates a DOid.
func (a *DatumAlloc) NewDOid(v DOid) Datum {
	buf := &a.doidAlloc
	if len(*buf) == 0 {
		*buf = make([]DOid, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// CollationEnvironment returns this DatumAlloc's cached CollationEnvironment.
func (a *DatumAlloc) CollationEnvironment() *CollationEnvironment {
	return &a.env
}

// Scratch returns this DatumAlloc's scratch byte space.
func (a *DatumAlloc) Scratch() []byte {
	return a.scratch
}
