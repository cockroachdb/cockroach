// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// DatumAlloc provides batch allocation of datum pointers, amortizing the cost
// of the allocations. nil value can be used to indicate that no batching is
// needed.
// NOTE: it *must* be passed in by a pointer.
type DatumAlloc struct {
	_ util.NoCopy

	// AllocSize determines the number of objects allocated whenever we've used
	// up previously allocated ones. This field is exported so that the caller
	// could adjust it dynamically. If it is left unchanged by the caller, then
	// it will be set to defaultDatumAllocSize automatically.
	AllocSize int

	datumAlloc        []Datum
	dintAlloc         []DInt
	dfloatAlloc       []DFloat
	dbitArrayAlloc    []DBitArray
	ddecimalAlloc     []DDecimal
	ddateAlloc        []DDate
	denumAlloc        []DEnum
	dbox2dAlloc       []DBox2D
	dgeometryAlloc    []DGeometry
	dgeographyAlloc   []DGeography
	dtimeAlloc        []DTime
	dtimetzAlloc      []DTimeTZ
	dtimestampAlloc   []DTimestamp
	dtimestampTzAlloc []DTimestampTZ
	dintervalAlloc    []DInterval
	duuidAlloc        []DUuid
	dipnetAlloc       []DIPAddr
	djsonAlloc        []DJSON
	dtupleAlloc       []DTuple
	doidAlloc         []DOid
	dvoidAlloc        []DVoid
	dpglsnAlloc       []DPGLSN
	// stringAlloc is used by all datum types that are strings (DBytes, DString, DEncodedKey).
	stringAlloc []string
	env         CollationEnvironment

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
func (a *DatumAlloc) NewDatums(num int) Datums {
	if a == nil {
		return make(Datums, num)
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.datumAlloc
	if len(*buf) < num {
		extensionSize := a.AllocSize
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
	if a == nil {
		r := new(DInt)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dintAlloc
	if len(*buf) == 0 {
		*buf = make([]DInt, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDPGLSN allocates a DPGLSN.
func (a *DatumAlloc) NewDPGLSN(v DPGLSN) *DPGLSN {
	if a == nil {
		r := new(DPGLSN)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dpglsnAlloc
	if len(*buf) == 0 {
		*buf = make([]DPGLSN, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDFloat allocates a DFloat.
func (a *DatumAlloc) NewDFloat(v DFloat) *DFloat {
	if a == nil {
		r := new(DFloat)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dfloatAlloc
	if len(*buf) == 0 {
		*buf = make([]DFloat, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

func (a *DatumAlloc) newString() *string {
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.stringAlloc
	if len(*buf) == 0 {
		*buf = make([]string, a.AllocSize)
	}
	r := &(*buf)[0]
	*buf = (*buf)[1:]
	return r
}

// NewDString allocates a DString.
func (a *DatumAlloc) NewDString(v DString) *DString {
	if a == nil {
		r := new(DString)
		*r = v
		return r
	}
	r := (*DString)(a.newString())
	*r = v
	return r
}

// NewDCollatedString allocates a DCollatedString.
func (a *DatumAlloc) NewDCollatedString(contents string, locale string) (*DCollatedString, error) {
	if a == nil {
		return NewDCollatedString(contents, locale, &CollationEnvironment{})
	}
	return NewDCollatedString(contents, locale, &a.env)
}

// NewDName allocates a DName.
func (a *DatumAlloc) NewDName(v DString) Datum {
	return NewDNameFromDString(a.NewDString(v))
}

// NewDRefCursor allocates a DRefCursor.
func (a *DatumAlloc) NewDRefCursor(v DString) Datum {
	return NewDRefCursorFromDString(a.NewDString(v))
}

// NewDBytes allocates a DBytes.
func (a *DatumAlloc) NewDBytes(v DBytes) *DBytes {
	if a == nil {
		r := new(DBytes)
		*r = v
		return r
	}
	r := (*DBytes)(a.newString())
	*r = v
	return r
}

// NewDEncodedKey allocates a DEncodedKey.
func (a *DatumAlloc) NewDEncodedKey(v DEncodedKey) *DEncodedKey {
	if a == nil {
		r := new(DEncodedKey)
		*r = v
		return r
	}
	r := (*DEncodedKey)(a.newString())
	*r = v
	return r
}

// NewDBitArray allocates a DBitArray.
func (a *DatumAlloc) NewDBitArray(v DBitArray) *DBitArray {
	if a == nil {
		r := new(DBitArray)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dbitArrayAlloc
	if len(*buf) == 0 {
		*buf = make([]DBitArray, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDecimal allocates a DDecimal.
func (a *DatumAlloc) NewDDecimal(v DDecimal) *DDecimal {
	if a == nil {
		r := new(DDecimal)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.ddecimalAlloc
	if len(*buf) == 0 {
		*buf = make([]DDecimal, a.AllocSize)
	}
	r := &(*buf)[0]
	r.Set(&v.Decimal)
	*buf = (*buf)[1:]
	return r
}

// NewDDate allocates a DDate.
func (a *DatumAlloc) NewDDate(v DDate) *DDate {
	if a == nil {
		r := new(DDate)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.ddateAlloc
	if len(*buf) == 0 {
		*buf = make([]DDate, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDEnum allocates a DEnum.
func (a *DatumAlloc) NewDEnum(v DEnum) *DEnum {
	if a == nil {
		r := new(DEnum)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.denumAlloc
	if len(*buf) == 0 {
		*buf = make([]DEnum, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDBox2D allocates a DBox2D.
func (a *DatumAlloc) NewDBox2D(v DBox2D) *DBox2D {
	if a == nil {
		r := new(DBox2D)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dbox2dAlloc
	if len(*buf) == 0 {
		*buf = make([]DBox2D, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDGeography allocates a DGeography.
func (a *DatumAlloc) NewDGeography(v DGeography) *DGeography {
	if a == nil {
		r := new(DGeography)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dgeographyAlloc
	if len(*buf) == 0 {
		*buf = make([]DGeography, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDVoid allocates a new DVoid.
func (a *DatumAlloc) NewDVoid() *DVoid {
	if a == nil {
		return &DVoid{}
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dvoidAlloc
	if len(*buf) == 0 {
		*buf = make([]DVoid, a.AllocSize)
	}
	r := &(*buf)[0]
	*buf = (*buf)[1:]
	return r
}

// NewDGeographyEmpty allocates a new empty DGeography for unmarshalling.
// After unmarshalling, DoneInitNewDGeo must be called to return unused
// pre-allocated space to the DatumAlloc.
func (a *DatumAlloc) NewDGeographyEmpty() *DGeography {
	r := a.NewDGeography(DGeography{})
	a.giveBytesToEWKB(r.SpatialObjectRef())
	return r
}

// DoneInitNewDGeo is called after unmarshalling a SpatialObject allocated via
// NewDGeographyEmpty/NewDGeometryEmpty, to return space to the DatumAlloc.
func (a *DatumAlloc) DoneInitNewDGeo(so *geopb.SpatialObject) {
	if a == nil {
		return
	}
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
func (a *DatumAlloc) NewDGeometry(v DGeometry) *DGeometry {
	if a == nil {
		r := new(DGeometry)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dgeometryAlloc
	if len(*buf) == 0 {
		*buf = make([]DGeometry, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDGeometryEmpty allocates a new empty DGeometry for unmarshalling. After
// unmarshalling, DoneInitNewDGeo must be called to return unused
// pre-allocated space to the DatumAlloc.
func (a *DatumAlloc) NewDGeometryEmpty() *DGeometry {
	r := a.NewDGeometry(DGeometry{})
	a.giveBytesToEWKB(r.SpatialObjectRef())
	return r
}

func (a *DatumAlloc) giveBytesToEWKB(so *geopb.SpatialObject) {
	if a == nil {
		return
	}
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
func (a *DatumAlloc) NewDTime(v DTime) *DTime {
	if a == nil {
		r := new(DTime)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dtimeAlloc
	if len(*buf) == 0 {
		*buf = make([]DTime, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimeTZ allocates a DTimeTZ.
func (a *DatumAlloc) NewDTimeTZ(v DTimeTZ) *DTimeTZ {
	if a == nil {
		r := new(DTimeTZ)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dtimetzAlloc
	if len(*buf) == 0 {
		*buf = make([]DTimeTZ, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestamp allocates a DTimestamp.
func (a *DatumAlloc) NewDTimestamp(v DTimestamp) *DTimestamp {
	if a == nil {
		r := new(DTimestamp)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dtimestampAlloc
	if len(*buf) == 0 {
		*buf = make([]DTimestamp, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestampTZ allocates a DTimestampTZ.
func (a *DatumAlloc) NewDTimestampTZ(v DTimestampTZ) *DTimestampTZ {
	if a == nil {
		r := new(DTimestampTZ)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dtimestampTzAlloc
	if len(*buf) == 0 {
		*buf = make([]DTimestampTZ, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDInterval allocates a DInterval.
func (a *DatumAlloc) NewDInterval(v DInterval) *DInterval {
	if a == nil {
		r := new(DInterval)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dintervalAlloc
	if len(*buf) == 0 {
		*buf = make([]DInterval, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDUuid allocates a DUuid.
func (a *DatumAlloc) NewDUuid(v DUuid) *DUuid {
	if a == nil {
		r := new(DUuid)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.duuidAlloc
	if len(*buf) == 0 {
		*buf = make([]DUuid, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDIPAddr allocates a DIPAddr.
func (a *DatumAlloc) NewDIPAddr(v DIPAddr) *DIPAddr {
	if a == nil {
		r := new(DIPAddr)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dipnetAlloc
	if len(*buf) == 0 {
		*buf = make([]DIPAddr, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDJSON allocates a DJSON.
func (a *DatumAlloc) NewDJSON(v DJSON) *DJSON {
	if a == nil {
		r := new(DJSON)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.djsonAlloc
	if len(*buf) == 0 {
		*buf = make([]DJSON, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTuple allocates a DTuple.
func (a *DatumAlloc) NewDTuple(v DTuple) *DTuple {
	if a == nil {
		r := new(DTuple)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.dtupleAlloc
	if len(*buf) == 0 {
		*buf = make([]DTuple, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDOid allocates a DOid.
func (a *DatumAlloc) NewDOid(v DOid) Datum {
	if a == nil {
		r := new(DOid)
		*r = v
		return r
	}
	if a.AllocSize == 0 {
		a.AllocSize = defaultDatumAllocSize
	}
	buf := &a.doidAlloc
	if len(*buf) == 0 {
		*buf = make([]DOid, a.AllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}
