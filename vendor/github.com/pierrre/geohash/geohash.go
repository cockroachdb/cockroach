/*
Package geohash provides an implementation of geohash.

http://en.wikipedia.com/wiki/Geohash

http://geohash.org
*/
package geohash

import (
	"fmt"
	"math"
)

const (
	base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
)

var (
	base32Lookup [1 << 8]int
	defaultBox   = Box{Lat: Range{Min: -90, Max: 90}, Lon: Range{Min: -180, Max: 180}}
)

func init() {
	for i := range base32Lookup {
		base32Lookup[i] = -1
	}
	for i := 0; i < len(base32); i++ {
		base32Lookup[base32[i]] = i
	}
}

// EncodeAuto encodes a location to a geohash using the most suitable precision.
func EncodeAuto(lat, lon float64) string {
	var gh string
	for precision := 1; precision <= encodeMaxPrecision; precision++ {
		gh = Encode(lat, lon, precision)
		b, _ := Decode(gh)
		p := b.Round()
		if p.Lat == lat && p.Lon == lon {
			break
		}
	}
	return gh
}

const encodeMaxPrecision = 32

// Encode encodes a location to a geohash.
//
// The maximum supported precision is 32.
func Encode(lat, lon float64, precision int) string {
	if precision > encodeMaxPrecision {
		precision = encodeMaxPrecision
	}
	var buf [encodeMaxPrecision]byte
	box := defaultBox
	even := true
	for i := 0; i < precision; i++ {
		ci := 0
		for mask := 1 << 4; mask != 0; mask >>= 1 {
			var r *Range
			var u float64
			if even {
				r = &box.Lon
				u = lon
			} else {
				r = &box.Lat
				u = lat
			}
			if mid := r.Mid(); u >= mid {
				ci += mask
				r.Min = mid
			} else {
				r.Max = mid
			}
			even = !even
		}
		buf[i] = base32[ci]
	}
	return string(buf[:precision])
}

// Decode decode a geohash to a Box.
func Decode(gh string) (Box, error) {
	box := defaultBox
	even := true
	for i := 0; i < len(gh); i++ {
		ci := base32Lookup[gh[i]]
		if ci == -1 {
			return box, fmt.Errorf("geohash decode '%s': invalid character at index %d", gh, i)
		}
		for mask := 1 << 4; mask != 0; mask >>= 1 {
			var r *Range
			if even {
				r = &box.Lon
			} else {
				r = &box.Lat
			}
			if mid := r.Mid(); ci&mask != 0 {
				r.Min = mid
			} else {
				r.Max = mid
			}
			even = !even
		}
	}
	return box, nil
}

//Box is a spatial data structure.
//
//It is defined by 2 ranges of latitude/longitude.
type Box struct {
	Lat, Lon Range
}

// Center returns the Box's center as a Point.
func (b Box) Center() Point {
	return Point{Lat: b.Lat.Mid(), Lon: b.Lon.Mid()}
}

// Round returns the Box's approximate location as a Point.
//
// It uses decimal rounding and is in general more useful than Center.
func (b Box) Round() Point {
	return Point{Lat: b.Lat.Round(), Lon: b.Lon.Round()}
}

// Point represents a location (latitude and longitude).
type Point struct {
	Lat, Lon float64
}

// Range represents a range (min/max) on latitude or longitude.
type Range struct {
	Min, Max float64
}

// Val returns the difference between Min and Max.
func (r Range) Val() float64 {
	return math.Abs(r.Max - r.Min)
}

// Mid return the middle value between Min and Max.
func (r Range) Mid() float64 {
	return (r.Min + r.Max) / 2
}

// Round returns the rounded value between Min and Max.
//
// It uses decimal rounding.
func (r Range) Round() float64 {
	dec := int(math.Floor(-math.Log10(r.Val())))
	if dec < 0 {
		dec = 0
	}
	return roundDecimal(r.Mid(), dec)
}

// Neighbors will contain the geohashes for the neighbors of the supplied
// geohash in each of the cardinal and intercardinal directions.
type Neighbors struct {
	North     string
	NorthEast string
	East      string
	SouthEast string
	South     string
	SouthWest string
	West      string
	NorthWest string
}

// GetNeighbors returns a struct representing the neighbors of the supplied
// geohash in each of the cardinal and intercardinal directions.
func GetNeighbors(gh string) (Neighbors, error) {
	box, err := Decode(gh)
	if err != nil {
		return Neighbors{}, err
	}
	latMid := box.Lat.Mid()
	lonMid := box.Lon.Mid()
	latVal := box.Lat.Val()
	lonVal := box.Lon.Val()
	precision := len(gh)
	encode := func(lat, lon float64) string {
		lat, lon = normalize(lat, lon)
		return Encode(lat, lon, precision)
	}
	return Neighbors{
		North:     encode(latMid+latVal, lonMid),
		NorthEast: encode(latMid+latVal, lonMid+lonVal),
		East:      encode(latMid, lonMid+lonVal),
		SouthEast: encode(latMid-latVal, lonMid+lonVal),
		South:     encode(latMid-latVal, lonMid),
		SouthWest: encode(latMid-latVal, lonMid-lonVal),
		West:      encode(latMid, lonMid-lonVal),
		NorthWest: encode(latMid+latVal, lonMid-lonVal),
	}, nil
}
