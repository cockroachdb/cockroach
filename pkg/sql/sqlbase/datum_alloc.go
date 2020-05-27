// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// DatumAlloc provides batch allocation of datum pointers, amortizing the cost
// of the allocations.
// NOTE: it *must* be passed in by a pointer.
type DatumAlloc struct {
	_ util.NoCopy

	datumAlloc        []tree.Datum
	dintAlloc         []tree.DInt
	dfloatAlloc       []tree.DFloat
	dstringAlloc      []tree.DString
	dbytesAlloc       []tree.DBytes
	dbitArrayAlloc    []tree.DBitArray
	ddecimalAlloc     []tree.DDecimal
	ddateAlloc        []tree.DDate
	denumAlloc        []tree.DEnum
	dgeometryAlloc    []tree.DGeometry
	dgeographyAlloc   []tree.DGeography
	dtimeAlloc        []tree.DTime
	dtimetzAlloc      []tree.DTimeTZ
	dtimestampAlloc   []tree.DTimestamp
	dtimestampTzAlloc []tree.DTimestampTZ
	dintervalAlloc    []tree.DInterval
	duuidAlloc        []tree.DUuid
	dipnetAlloc       []tree.DIPAddr
	djsonAlloc        []tree.DJSON
	dtupleAlloc       []tree.DTuple
	doidAlloc         []tree.DOid
	scratch           []byte
	env               tree.CollationEnvironment
}

const datumAllocSize = 16      // Arbitrary, could be tuned.
const datumAllocMultiplier = 4 // Arbitrary, could be tuned.

// NewDatums allocates Datums of the specified size.
func (a *DatumAlloc) NewDatums(num int) tree.Datums {
	buf := &a.datumAlloc
	if len(*buf) < num {
		extensionSize := datumAllocSize
		if extTupleLen := num * datumAllocMultiplier; extensionSize < extTupleLen {
			extensionSize = extTupleLen
		}
		*buf = make(tree.Datums, extensionSize)
	}
	r := (*buf)[:num]
	*buf = (*buf)[num:]
	return r
}

// NewDInt allocates a DInt.
func (a *DatumAlloc) NewDInt(v tree.DInt) *tree.DInt {
	buf := &a.dintAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DInt, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDFloat allocates a DFloat.
func (a *DatumAlloc) NewDFloat(v tree.DFloat) *tree.DFloat {
	buf := &a.dfloatAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DFloat, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDString allocates a DString.
func (a *DatumAlloc) NewDString(v tree.DString) *tree.DString {
	buf := &a.dstringAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DString, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDName allocates a DName.
func (a *DatumAlloc) NewDName(v tree.DString) tree.Datum {
	return tree.NewDNameFromDString(a.NewDString(v))
}

// NewDBytes allocates a DBytes.
func (a *DatumAlloc) NewDBytes(v tree.DBytes) *tree.DBytes {
	buf := &a.dbytesAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DBytes, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDBitArray allocates a DBitArray.
func (a *DatumAlloc) NewDBitArray(v tree.DBitArray) *tree.DBitArray {
	buf := &a.dbitArrayAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DBitArray, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDecimal allocates a DDecimal.
func (a *DatumAlloc) NewDDecimal(v tree.DDecimal) *tree.DDecimal {
	buf := &a.ddecimalAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DDecimal, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDate allocates a DDate.
func (a *DatumAlloc) NewDDate(v tree.DDate) *tree.DDate {
	buf := &a.ddateAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DDate, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDEnum allocates a DEnum.
func (a *DatumAlloc) NewDEnum(v tree.DEnum) *tree.DEnum {
	buf := &a.denumAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DEnum, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDGeography allocates a DGeography.
func (a *DatumAlloc) NewDGeography(v tree.DGeography) *tree.DGeography {
	buf := &a.dgeographyAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DGeography, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDGeometry allocates a DGeometry.
func (a *DatumAlloc) NewDGeometry(v tree.DGeometry) *tree.DGeometry {
	buf := &a.dgeometryAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DGeometry, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTime allocates a DTime.
func (a *DatumAlloc) NewDTime(v tree.DTime) *tree.DTime {
	buf := &a.dtimeAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTime, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimeTZ allocates a DTimeTZ.
func (a *DatumAlloc) NewDTimeTZ(v tree.DTimeTZ) *tree.DTimeTZ {
	buf := &a.dtimetzAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTimeTZ, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestamp allocates a DTimestamp.
func (a *DatumAlloc) NewDTimestamp(v tree.DTimestamp) *tree.DTimestamp {
	buf := &a.dtimestampAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTimestamp, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestampTZ allocates a DTimestampTZ.
func (a *DatumAlloc) NewDTimestampTZ(v tree.DTimestampTZ) *tree.DTimestampTZ {
	buf := &a.dtimestampTzAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTimestampTZ, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDInterval allocates a DInterval.
func (a *DatumAlloc) NewDInterval(v tree.DInterval) *tree.DInterval {
	buf := &a.dintervalAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DInterval, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDUuid allocates a DUuid.
func (a *DatumAlloc) NewDUuid(v tree.DUuid) *tree.DUuid {
	buf := &a.duuidAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DUuid, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDIPAddr allocates a DIPAddr.
func (a *DatumAlloc) NewDIPAddr(v tree.DIPAddr) *tree.DIPAddr {
	buf := &a.dipnetAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DIPAddr, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDJSON allocates a DJSON.
func (a *DatumAlloc) NewDJSON(v tree.DJSON) *tree.DJSON {
	buf := &a.djsonAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DJSON, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTuple allocates a DTuple.
func (a *DatumAlloc) NewDTuple(v tree.DTuple) *tree.DTuple {
	buf := &a.dtupleAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTuple, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDOid allocates a DOid.
func (a *DatumAlloc) NewDOid(v tree.DOid) tree.Datum {
	buf := &a.doidAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DOid, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}
