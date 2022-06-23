package geohash

import (
	"math"
)

func round(val float64) float64 {
	if val < 0 {
		return math.Ceil(val - 0.5)
	}
	return math.Floor(val + 0.5)
}

func roundDecimal(val float64, dec int) float64 {
	factor := math.Pow10(dec)
	return round(val*factor) / factor
}

func normalize(lat, lon float64) (float64, float64) {
	if lat > 90 || lat < -90 {
		lat = center360(lat)
		invertLon := true
		if lat < -90 {
			lat = -180 - lat
		} else if lat > 90 {
			lat = 180 - lat
		} else {
			invertLon = false
		}
		if invertLon {
			if lon > 0 {
				lon -= 180
			} else {
				lon += 180
			}
		}
	}
	if lon > 180 || lon <= -180 {
		lon = center360(lon)
	}
	return lat, lon
}

func center360(v float64) float64 {
	v = math.Mod(v, 360)
	if v <= 0 {
		v += 360
	}
	if v > 180 {
		v -= 360
	}
	return v
}
