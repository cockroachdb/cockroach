// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowenc

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// DatumAlloc provides batch allocation of datum pointers, amortizing the cost
// of the allocations.
// NOTE: it *must* be passed in by a pointer.
type DatumAlloc struct {
	_ util.NoCopy

	// AllocSize determines the number of objects allocated whenever we've used
	// up previously allocated ones. This field is exported so that the caller
	// could adjust it dynamically. If it is left unchanged by the caller, then
	// it will be set to defaultDatumAllocSize automatically.
	AllocSize int

	datumAlloc        []tree.Datum
	dintAlloc         []tree.DInt
	dfloatAlloc       []tree.DFloat
	dstringAlloc      []tree.DString
	dbytesAlloc       []tree.DBytes
	dbitArrayAlloc    []tree.DBitArray
	ddecimalAlloc     []tree.DDecimal
	ddateAlloc        []tree.DDate
	denumAlloc        []tree.DEnum
	dbox2dAlloc       []tree.DBox2D
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

	// Allocations for geopb.SpatialObject.EWKB
	ewkbAlloc               []byte
	curEWKBAllocSize        int
	lastEWKBBeyondAllocSize bool
}

const defaultDatumAllocSize = 16  // Arbitrary, could be tuned.
const datumAllocMultiplier = 4    // Arbitrary, could be tuned.
const defaultEWKBAllocSize = 4096 // Arbitrary, could be tuned.
const maxEWKBAllocSize = 16384    // Arbitrary, could be tuned.

// NewDatums allocates Datums of the specified size.
func (a *DatumAlloc) NewDatums(num int) tree.Datums {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.datumAlloc
	if len(*buf) < num {
		extensionSize := a.AllocSize
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
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dintAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DInt, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDFloat allocates a DFloat.
func (a *DatumAlloc) NewDFloat(v tree.DFloat) *tree.DFloat {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dfloatAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DFloat, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDString allocates a DString.
func (a *DatumAlloc) NewDString(v tree.DString) *tree.DString {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dstringAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DString, a.AllocSize)
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
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dbytesAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DBytes, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDBitArray allocates a DBitArray.
func (a *DatumAlloc) NewDBitArray(v tree.DBitArray) *tree.DBitArray {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dbitArrayAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DBitArray, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDecimal allocates a DDecimal.
func (a *DatumAlloc) NewDDecimal(v tree.DDecimal) *tree.DDecimal {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.ddecimalAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DDecimal, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDate allocates a DDate.
func (a *DatumAlloc) NewDDate(v tree.DDate) *tree.DDate {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.ddateAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DDate, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDEnum allocates a DEnum.
func (a *DatumAlloc) NewDEnum(v tree.DEnum) *tree.DEnum {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.denumAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DEnum, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDBox2D allocates a DBox2D.
func (a *DatumAlloc) NewDBox2D(v tree.DBox2D) *tree.DBox2D {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dbox2dAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DBox2D, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDGeography allocates a DGeography.
func (a *DatumAlloc) NewDGeography(v tree.DGeography) *tree.DGeography {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dgeographyAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DGeography, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDGeographyEmpty allocates a new empty DGeography for unmarshalling.
// After unmarshalling, DoneInitNewDGeo must be called to return unused
// pre-allocated space to the DatumAlloc.
func (a *DatumAlloc) NewDGeographyEmpty() *tree.DGeography {
	r := a.NewDGeography(tree.DGeography{})
	a.giveBytesToEWKB(r.SpatialObjectRef())
	return r
}

// DoneInitNewDGeo is called after unmarshalling a SpatialObject allocated via
// NewDGeographyEmpty/NewDGeometryEmpty, to return space to the DatumAlloc.
func (a *DatumAlloc) DoneInitNewDGeo(so *geopb.SpatialObject) {
	// Don't allocate next time if the allocation was wasted and there is no way
	// to pre-allocate enough. This is just a crude heuristic to avoid wasting
	// allocations if the EWKBs are very large.
	a.lastEWKBBeyondAllocSize = len(so.EWKB) > maxEWKBAllocSize
	c := cap(so.EWKB)
	l := len(so.EWKB)
	if (c - l) > l {
		a.ewkbAlloc = so.EWKB[l:l:c]
		so.EWKB = so.EWKB[:l:l]
	}
}

// NewDGeometry allocates a DGeometry.
func (a *DatumAlloc) NewDGeometry(v tree.DGeometry) *tree.DGeometry {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dgeometryAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DGeometry, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDGeometryEmpty allocates a new empty DGeometry for unmarshalling. After
// unmarshalling, DoneInitNewDGeo must be called to return unused
// pre-allocated space to the DatumAlloc.
func (a *DatumAlloc) NewDGeometryEmpty() *tree.DGeometry {
	r := a.NewDGeometry(tree.DGeometry{})
	a.giveBytesToEWKB(r.SpatialObjectRef())
	return r
}

func (a *DatumAlloc) giveBytesToEWKB(so *geopb.SpatialObject) {
	if a.ewkbAlloc == nil && !a.lastEWKBBeyondAllocSize {
		if a.curEWKBAllocSize == 0 {
			a.curEWKBAllocSize = defaultEWKBAllocSize
		} else if a.curEWKBAllocSize < maxEWKBAllocSize {
			a.curEWKBAllocSize *= 2
		}
		so.EWKB = make([]byte, 0, a.curEWKBAllocSize)
	} else {
		so.EWKB = a.ewkbAlloc
		a.ewkbAlloc = nil
	}
}

// NewDTime allocates a DTime.
func (a *DatumAlloc) NewDTime(v tree.DTime) *tree.DTime {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dtimeAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTime, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimeTZ allocates a DTimeTZ.
func (a *DatumAlloc) NewDTimeTZ(v tree.DTimeTZ) *tree.DTimeTZ {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dtimetzAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTimeTZ, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestamp allocates a DTimestamp.
func (a *DatumAlloc) NewDTimestamp(v tree.DTimestamp) *tree.DTimestamp {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dtimestampAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTimestamp, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestampTZ allocates a DTimestampTZ.
func (a *DatumAlloc) NewDTimestampTZ(v tree.DTimestampTZ) *tree.DTimestampTZ {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dtimestampTzAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTimestampTZ, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDInterval allocates a DInterval.
func (a *DatumAlloc) NewDInterval(v tree.DInterval) *tree.DInterval {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dintervalAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DInterval, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDUuid allocates a DUuid.
func (a *DatumAlloc) NewDUuid(v tree.DUuid) *tree.DUuid {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.duuidAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DUuid, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDIPAddr allocates a DIPAddr.
func (a *DatumAlloc) NewDIPAddr(v tree.DIPAddr) *tree.DIPAddr {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dipnetAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DIPAddr, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDJSON allocates a DJSON.
func (a *DatumAlloc) NewDJSON(v tree.DJSON) *tree.DJSON {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.djsonAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DJSON, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTuple allocates a DTuple.
func (a *DatumAlloc) NewDTuple(v tree.DTuple) *tree.DTuple {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dtupleAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTuple, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDOid allocates a DOid.
func (a *DatumAlloc) NewDOid(v tree.DOid) tree.Datum {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.doidAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DOid, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}
