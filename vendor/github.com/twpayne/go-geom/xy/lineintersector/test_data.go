package lineintersector

import (
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/lineintersection"
)

type pointOnLineTestData struct {
	P, LineEnd1, LineEnd2 geom.Coord
	Result                bool
}

type lineIntersectsLinesTestData struct {
	Desc           string
	P1, P2, P3, P4 geom.Coord
	Result         lineintersection.Result
}

var pointOnLineIntersectionTestData = []pointOnLineTestData{
	{
		P: geom.Coord{0, 0}, LineEnd1: geom.Coord{-1, 0}, LineEnd2: geom.Coord{1, 0}, Result: true,
	},
	{
		P: geom.Coord{0, 1}, LineEnd1: geom.Coord{-1, 1}, LineEnd2: geom.Coord{1, 1}, Result: true,
	},
	{
		P: geom.Coord{0, 0}, LineEnd1: geom.Coord{-1, 1}, LineEnd2: geom.Coord{1, 0}, Result: false,
	},
	{
		P: geom.Coord{0, 0}, LineEnd1: geom.Coord{-1, -1}, LineEnd2: geom.Coord{1, 1}, Result: true,
	},
	{
		P: geom.Coord{-1, -1}, LineEnd1: geom.Coord{-1, -1}, LineEnd2: geom.Coord{1, 1}, Result: true,
	},
	{
		P: geom.Coord{1, 1}, LineEnd1: geom.Coord{-1, -1}, LineEnd2: geom.Coord{1, 1}, Result: true,
	},
}

var lineOnLineIntersectionTestData = []lineIntersectsLinesTestData{
	{
		Desc: "A perfect X at 0",
		P1:   geom.Coord{-1, 0}, P2: geom.Coord{1, 0}, P3: geom.Coord{0, -1}, P4: geom.Coord{0, 1},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{0, 0}}),
	},
	{
		Desc: "A perfect X at 15, 15",
		P1:   geom.Coord{10, 10}, P2: geom.Coord{20, 20}, P3: geom.Coord{10, 20}, P4: geom.Coord{20, 10},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{15, 15}}),
	},
	{
		Desc: "Same coordinates opposite vectors",
		P1:   geom.Coord{10, 10}, P2: geom.Coord{20, 20}, P3: geom.Coord{20, 20}, P4: geom.Coord{10, 10},
		Result: lineintersection.NewResult(lineintersection.CollinearIntersection, []geom.Coord{{10, 10}, {20, 20}}),
	},
	{
		Desc: "Parallel lines opposite directions, Line2End2 and Line1End2 within bounds of other line",
		P1:   geom.Coord{10, 10}, P2: geom.Coord{20, 20}, P3: geom.Coord{30, 20}, P4: geom.Coord{20, 10},
		Result: lineintersection.NewResult(lineintersection.NoIntersection, []geom.Coord{}),
	},
	{
		Desc: "Parallel lines opposite directions, Disjointed Bounds",
		P1:   geom.Coord{10, 10}, P2: geom.Coord{20, 20}, P3: geom.Coord{-30, -20}, P4: geom.Coord{-20, -10},
		Result: lineintersection.NewResult(lineintersection.NoIntersection, []geom.Coord{}),
	},
	{
		Desc: "Disjointed lines, line2 within line1 bounds",
		P1:   geom.Coord{1, 1}, P2: geom.Coord{5, 5}, P3: geom.Coord{0, 0}, P4: geom.Coord{1, 2},
		Result: lineintersection.NewResult(lineintersection.NoIntersection, []geom.Coord{}),
	},
	{
		Desc: "Disjointed lines, line2End1 within line1 bounds, line2End2 outside of line1 bounds",
		P1:   geom.Coord{0, 0}, P2: geom.Coord{5, 5}, P3: geom.Coord{0, 1}, P4: geom.Coord{5, 6},
		Result: lineintersection.NewResult(lineintersection.NoIntersection, []geom.Coord{}),
	},
	{
		Desc: "Collinear disjointed lines",
		P1:   geom.Coord{1, 1}, P2: geom.Coord{5, 5}, P3: geom.Coord{-1, -1}, P4: geom.Coord{-5, -5},
		Result: lineintersection.NewResult(lineintersection.NoIntersection, []geom.Coord{}),
	},
	{
		Desc: "line1End1 and line2End1 are same point, diverging lines, line2 within bounds of line1",
		P1:   geom.Coord{0, 0}, P2: geom.Coord{5, 5}, P3: geom.Coord{0, 0}, P4: geom.Coord{4, 5},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{0, 0}}),
	},
	{
		Desc: "line1End2 and line2End1 are same point, connected lines line1 -> line2, line2 within bounds of line1",
		P1:   geom.Coord{0, 0}, P2: geom.Coord{5, 5}, P3: geom.Coord{5, 5}, P4: geom.Coord{0, 1},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{5, 5}}),
	},
	{
		Desc: "line2End1 lies on line1, line2 within bounds of line1",
		P1:   geom.Coord{0, 0}, P2: geom.Coord{5, 5}, P3: geom.Coord{1, 1}, P4: geom.Coord{4, 5},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{1, 1}}),
	},
	{
		Desc: "line2End2 lies on line1, line2 within bounds of line1",
		P1:   geom.Coord{0, 0}, P2: geom.Coord{5, 5}, P3: geom.Coord{0, 1}, P4: geom.Coord{4, 4},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{4, 4}}),
	},
	{
		Desc: "line1End1 lies on line2, line1 within bounds of line2",
		P1:   geom.Coord{1, 1}, P2: geom.Coord{4, 5}, P3: geom.Coord{0, 0}, P4: geom.Coord{5, 5},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{1, 1}}),
	},
	{
		Desc: "line1End2 lies on line2, line1 within bounds of line2",
		P1:   geom.Coord{0, 1}, P2: geom.Coord{4, 4}, P3: geom.Coord{0, 0}, P4: geom.Coord{5, 5},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{4, 4}}),
	},
}

var robustLineOnLineIntersectionData = append(lineOnLineIntersectionTestData, []lineIntersectsLinesTestData{
	{
		Desc: "Collinear lines, line1End1 and line2End1 are same point",
		P1:   geom.Coord{1, 1}, P2: geom.Coord{5, 5}, P3: geom.Coord{1, 1}, P4: geom.Coord{-5, -5},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{1, 1}}),
	},
	{
		Desc: "Collinear lines, line1End1 and line2End2 are same point",
		P1:   geom.Coord{1, 1}, P2: geom.Coord{5, 5}, P3: geom.Coord{-5, -5}, P4: geom.Coord{1, 1},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{1, 1}}),
	},
	{
		Desc: "Collinear lines, line1End2 and line2End1 are same point",
		P1:   geom.Coord{1, 1}, P2: geom.Coord{5, 5}, P3: geom.Coord{5, 5}, P4: geom.Coord{10, 10},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{5, 5}}),
	},
	{
		Desc: "Collinear lines, line1End2 and line2End2 are same point",
		P1:   geom.Coord{1, 1}, P2: geom.Coord{5, 5}, P3: geom.Coord{10, 10}, P4: geom.Coord{5, 5},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{5, 5}}),
	},
	{
		Desc: "Collinear lines, line1End2 and line2End2 are same point",
		P1:   geom.Coord{2089426.5233462777, 1180182.3877339689}, P2: geom.Coord{2085646.6891757075, 1195618.7333999649},
		P3: geom.Coord{1889281.8148903656, 1997547.0560044837}, P4: geom.Coord{2259977.3672235999, 483675.17050843034},
		Result: lineintersection.NewResult(lineintersection.PointIntersection, []geom.Coord{{2087536.6062609926, 1187900.560566967}}),
	},
}...)
