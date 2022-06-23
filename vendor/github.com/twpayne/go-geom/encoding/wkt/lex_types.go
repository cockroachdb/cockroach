package wkt

type geomFlatCoordsRepr struct {
	flatCoords []float64
	ends       []int
}

func makeGeomFlatCoordsRepr(flatCoords []float64) geomFlatCoordsRepr {
	return geomFlatCoordsRepr{flatCoords: flatCoords, ends: []int{len(flatCoords)}}
}

func appendGeomFlatCoordsReprs(p1, p2 geomFlatCoordsRepr) geomFlatCoordsRepr {
	if len(p1.ends) > 0 {
		p1LastEnd := p1.ends[len(p1.ends)-1]
		for i := range p2.ends {
			p2.ends[i] += p1LastEnd
		}
	}
	return geomFlatCoordsRepr{flatCoords: append(p1.flatCoords, p2.flatCoords...), ends: append(p1.ends, p2.ends...)}
}

type multiPolygonFlatCoordsRepr struct {
	flatCoords []float64
	endss      [][]int
}

func makeMultiPolygonFlatCoordsRepr(p geomFlatCoordsRepr) multiPolygonFlatCoordsRepr {
	if p.flatCoords == nil {
		return multiPolygonFlatCoordsRepr{flatCoords: nil, endss: [][]int{nil}}
	}
	return multiPolygonFlatCoordsRepr{flatCoords: p.flatCoords, endss: [][]int{p.ends}}
}

func appendMultiPolygonFlatCoordsRepr(
	p1, p2 multiPolygonFlatCoordsRepr,
) multiPolygonFlatCoordsRepr {
	p1LastEndsLastEnd := 0
	for i := len(p1.endss) - 1; i >= 0; i-- {
		if len(p1.endss[i]) > 0 {
			p1LastEndsLastEnd = p1.endss[i][len(p1.endss[i])-1]
			break
		}
	}
	if p1LastEndsLastEnd > 0 {
		for i := range p2.endss {
			for j := range p2.endss[i] {
				p2.endss[i][j] += p1LastEndsLastEnd
			}
		}
	}
	return multiPolygonFlatCoordsRepr{
		flatCoords: append(p1.flatCoords, p2.flatCoords...), endss: append(p1.endss, p2.endss...),
	}
}
