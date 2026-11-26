// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geo

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGeometryToLatLonText(t *testing.T) {
	// Test cases ported from PostGIS liblwgeom/cunit/cu_print.c and PostGIS docs.
	testCases := []struct {
		name     string
		ewkt     string
		format   string
		expected string
	}{
		// Default format and global coordinate normalization.
		{"origin", "POINT(0 0)", "", "0°0'0.000\"N 0°0'0.000\"E"},
		{"pos_lat_pos_lon", "POINT(45.4545 12.34567)", "", "12°20'44.412\"N 45°27'16.200\"E"},
		{"max_pos_bounds", "POINT(180 90)", "", "90°0'0.000\"N 180°0'0.000\"E"},
		{"wrap_pos_bounds", "POINT(181 91)", "", "89°0'0.000\"N 1°0'0.000\"E"},
		{"wrap_pos_bounds_frac", "POINT(180.0001 90.0001)", "", "89°59'59.640\"N 0°0'0.360\"E"},
		{"neg_lat_pos_lon", "POINT(45.4545 -12.34567)", "", "12°20'44.412\"S 45°27'16.200\"E"},
		{"south_pole_east", "POINT(180 -90)", "", "90°0'0.000\"S 180°0'0.000\"E"},
		{"wrap_south_east", "POINT(181 -91)", "", "89°0'0.000\"S 1°0'0.000\"E"},
		{"wrap_south_east_frac", "POINT(180.0001 -90.0001)", "", "89°59'59.640\"S 0°0'0.360\"E"},
		{"pos_lat_neg_lon", "POINT(-45.4545 12.34567)", "", "12°20'44.412\"N 45°27'16.200\"W"},
		{"north_pole_west", "POINT(-180 90)", "", "90°0'0.000\"N 180°0'0.000\"W"},
		{"wrap_north_west", "POINT(-181 91)", "", "89°0'0.000\"N 1°0'0.000\"W"},
		{"wrap_north_west_frac", "POINT(-180.0001 90.0001)", "", "89°59'59.640\"N 0°0'0.360\"W"},
		{"neg_lat_neg_lon", "POINT(-45.4545 -12.34567)", "", "12°20'44.412\"S 45°27'16.200\"W"},
		{"south_pole_west", "POINT(-180 -90)", "", "90°0'0.000\"S 180°0'0.000\"W"},
		{"wrap_south_west", "POINT(-181 -91)", "", "89°0'0.000\"S 1°0'0.000\"W"},
		{"wrap_south_west_frac", "POINT(-180.0001 -90.0001)", "", "89°59'59.640\"S 0°0'0.360\"W"},
		{"large_negative_wrap", "POINT(-2348982391.123456 -238749827.34879)", "", "12°39'4.356\"N 31°7'24.442\"W"},
		// PostGIS ticket #3688 (seconds rounding to 60 carrying into minutes).
		{"ticket_3688", "POINT(76.6 -76.6)", "", "76°36'0.000\"S 76°36'0.000\"E"},
		// 3D/4D points and SRIDs.
		{"point_z", "POINT Z (45.4545 12.34567 100)", "", "12°20'44.412\"N 45°27'16.200\"E"},
		{"point_m", "POINT M (45.4545 12.34567 200)", "", "12°20'44.412\"N 45°27'16.200\"E"},
		{"point_zm", "SRID=4326;POINT ZM (45.4545 12.34567 100 200)", "", "12°20'44.412\"N 45°27'16.200\"E"},
		{"negative_zero_wrap", "POINT(-360 -360)", "", "0°0'0.000\"N 0°0'0.000\"E"},
		{"lat_270", "POINT(0 270)", "", "90°0'0.000\"S 180°0'0.000\"E"},
		{"lat_neg_270", "POINT(0 -270)", "", "90°0'0.000\"N 180°0'0.000\"E"},
		{"lat_271", "POINT(0 271)", "", "89°0'0.000\"S 0°0'0.000\"E"},
		{"lat_neg_271", "POINT(0 -271)", "", "89°0'0.000\"N 0°0'0.000\"E"},

		// PostGIS documentation examples.
		{"doc_default", "POINT(-3.2342342 -2.32498)", "", "2°19'29.928\"S 3°14'3.243\"W"},
		{
			"doc_custom_literals",
			"POINT(-3.2342342 -2.32498)",
			"D degrees, M minutes, S seconds to the C",
			"2 degrees, 19 minutes, 30 seconds to the S 3 degrees, 14 minutes, 3 seconds to the W",
		},
		{"doc_signed", "POINT(-3.2342342 -2.32498)", "D°M'S.SSS\"", "-2°19'29.928\" -3°14'3.243\""},
		{"doc_decimal_deg", "POINT(-3.2342342 -2.32498)", "D.DDDD degrees C", "2.3250 degrees S 3.2342 degrees W"},

		// All 24 permutations of D, M, S, C.
		{"order_C_D_M_S", "POINT(-45.4545 -12.34567)", "C DD MM SS", "S 12 20 44 W 45 27 16"},
		{"order_C_D_S_M", "POINT(-45.4545 -12.34567)", "C DD SS MM", "S 12 44 20 W 45 16 27"},
		{"order_C_M_D_S", "POINT(-45.4545 -12.34567)", "C MM DD SS", "S 20 12 44 W 27 45 16"},
		{"order_C_M_S_D", "POINT(-45.4545 -12.34567)", "C MM SS DD", "S 20 44 12 W 27 16 45"},
		{"order_C_S_D_M", "POINT(-45.4545 -12.34567)", "C SS DD MM", "S 44 12 20 W 16 45 27"},
		{"order_C_S_M_D", "POINT(-45.4545 -12.34567)", "C SS MM DD", "S 44 20 12 W 16 27 45"},
		{"order_D_C_M_S", "POINT(-45.4545 -12.34567)", "DD C MM SS", "12 S 20 44 45 W 27 16"},
		{"order_D_C_S_M", "POINT(-45.4545 -12.34567)", "DD C SS MM", "12 S 44 20 45 W 16 27"},
		{"order_M_C_D_S", "POINT(-45.4545 -12.34567)", "MM C DD SS", "20 S 12 44 27 W 45 16"},
		{"order_M_C_S_D", "POINT(-45.4545 -12.34567)", "MM C SS DD", "20 S 44 12 27 W 16 45"},
		{"order_S_C_D_M", "POINT(-45.4545 -12.34567)", "SS C DD MM", "44 S 12 20 16 W 45 27"},
		{"order_S_C_M_D", "POINT(-45.4545 -12.34567)", "SS C MM DD", "44 S 20 12 16 W 27 45"},
		{"order_D_M_C_S", "POINT(-45.4545 -12.34567)", "DD MM C SS", "12 20 S 44 45 27 W 16"},
		{"order_D_S_C_M", "POINT(-45.4545 -12.34567)", "DD SS C MM", "12 44 S 20 45 16 W 27"},
		{"order_M_D_C_S", "POINT(-45.4545 -12.34567)", "MM DD C SS", "20 12 S 44 27 45 W 16"},
		{"order_M_S_C_D", "POINT(-45.4545 -12.34567)", "MM SS C DD", "20 44 S 12 27 16 W 45"},
		{"order_S_D_C_M", "POINT(-45.4545 -12.34567)", "SS DD C MM", "44 12 S 20 16 45 W 27"},
		{"order_S_M_C_D", "POINT(-45.4545 -12.34567)", "SS MM C DD", "44 20 S 12 16 27 W 45"},
		{"order_D_M_S_C", "POINT(-45.4545 -12.34567)", "DD MM SS C", "12 20 44 S 45 27 16 W"},
		{"order_D_S_M_C", "POINT(-45.4545 -12.34567)", "DD SS MM C", "12 44 20 S 45 16 27 W"},
		{"order_M_D_S_C", "POINT(-45.4545 -12.34567)", "MM DD SS C", "20 12 44 S 27 45 16 W"},
		{"order_M_S_D_C", "POINT(-45.4545 -12.34567)", "MM SS DD C", "20 44 12 S 27 16 45 W"},
		{"order_S_D_M_C", "POINT(-45.4545 -12.34567)", "SS DD MM C", "44 12 20 S 16 45 27 W"},
		{"order_S_M_D_C", "POINT(-45.4545 -12.34567)", "SS MM DD C", "44 20 12 S 16 27 45 W"},

		// Optional components and rounding carry-over.
		{"carry_sec_and_min_1", "POINT(21.9999999 1)", "D-M-S-C", "1-0-0-N 22-0-0-E"},
		{"carry_sec_and_min_2", "POINT(21.99999999999 1)", "D-M-S-C", "1-0-0-N 22-0-0-E"},
		{"opt_D", "POINT(-45.4545 -12.34567)", "DD.DDD", "-12.346 -45.455"},
		{"opt_D_C", "POINT(-45.4545 -12.34567)", "DD.DDD C", "12.346 S 45.455 W"},
		{"opt_D_M", "POINT(-45.4545 -12.34567)", "DD.DDD MM.MMM", "-12.000 20.740 -45.000 27.270"},
		{"opt_D_M_C", "POINT(-45.4545 -12.34567)", "DD.DDD MM.MMM C", "12.000 20.740 S 45.000 27.270 W"},
		{"opt_D_M_S", "POINT(-45.4545 -12.34567)", "DD.DDD MM.MMM SS.SSS", "-12.000 20.000 44.412 -45.000 27.000 16.200"},
		{"opt_D_M_S_C", "POINT(-45.4545 -12.34567)", "DD.DDD MM.MMM SS.SSS C", "12.000 20.000 44.412 S 45.000 27.000 16.200 W"},

		// Oddball formats from PostGIS cu_print.c.
		{"oddball_no_sep", "POINT(-45.4545 -12.34567)", "DD.DDDMM.MMMSS.SSSC", "12.00020.00044.412S 45.00027.00016.200W"},
		{"oddball_dm_no_sep", "POINT(-45.4545 -12.34567)", "DDMM.MMM", "-1220.740 -4527.270"},
		{"oddball_trailing_dot", "POINT(-45.4545 -12.34567)", "DD.MM.MMM", "-1220.740 -4527.270"},
		{"oddball_repeated_dot", "POINT(1.25 2.5)", "D.D.D", "2.50 1.25"},

		// Width padding, negative zero degrees, literal dots, and trailing UTF-8.
		{"width_padding", "POINT(5 1)", "DDD MM SS C", "  1  0  0 N   5  0  0 E"},
		{"width_padding_decimal", "POINT(5 1)", "DDDDDD.DD", "  1.00   5.00"},
		{"neg_zero_deg_no_compass", "POINT(-0.5 -0.5)", "D M", "-0 30 -0 30"},
		{"literal_dots", "POINT(1 2)", ".D. .C.", ".2 .N. .1 .E."},
		{"literal_dots_separated", "POINT(1 2)", ".D . .C.", ".2 . .N. .1 . .E."},
		{"trailing_multibyte_utf8", "POINT(1 2)", "D°", "2° 1°"},
		{"max_nine_pieces", "POINT(1 2)", "a D b M c S d C e", "a 2 b 0 c 0 d N e a 1 b 0 c 0 d E e"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := MustParseGeometry(tc.ewkt)
			actual, err := GeometryToLatLonText(g, tc.format)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}

	t.Run("huge_finite_coordinates_do_not_hang", func(t *testing.T) {
		g, err := MakeGeometryFromPointCoords(1e300, -1e300)
		require.NoError(t, err)
		res, err := GeometryToLatLonText(g, "")
		require.NoError(t, err)
		require.NotEmpty(t, res)
	})
}

func TestGeometryToLatLonTextErrors(t *testing.T) {
	t.Run("non_point_geometries", func(t *testing.T) {
		nonPoints := []string{
			"LINESTRING(0 0, 1 1)",
			"POLYGON((0 0, 1 0, 1 1, 0 0))",
			"MULTIPOINT((0 0))",
			"MULTILINESTRING((0 0, 1 1))",
			"MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))",
			"GEOMETRYCOLLECTION(POINT(0 0))",
			"LINESTRING EMPTY",
			"GEOMETRYCOLLECTION EMPTY",
		}
		for _, wkt := range nonPoints {
			t.Run(wkt, func(t *testing.T) {
				g := MustParseGeometry(wkt)
				_, err := GeometryToLatLonText(g, "")
				require.Error(t, err)
				require.Contains(t, err.Error(), "only points are supported")
			})
		}
	})

	t.Run("empty_point", func(t *testing.T) {
		for _, wkt := range []string{"POINT EMPTY", "POINT Z EMPTY", "POINT M EMPTY", "POINT ZM EMPTY"} {
			t.Run(wkt, func(t *testing.T) {
				g := MustParseGeometry(wkt)
				_, err := GeometryToLatLonText(g, "")
				require.Error(t, err)
				require.Contains(t, err.Error(), "cannot convert an empty point into formatted text")
			})
		}
	})

	t.Run("non_finite_coordinates", func(t *testing.T) {
		// PostGIS ticket #5342: SRID=4326;POINT(+Inf +Inf).
		gInf := MustParseGeometry("0101000020E6100000000000000000F07F000000000000F07F")
		_, err := GeometryToLatLonText(gInf, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid coordinate")

		gNegInf, err := MakeGeometryFromPointCoords(math.Inf(-1), 0)
		require.NoError(t, err)
		_, err = GeometryToLatLonText(gNegInf, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid coordinate")

		gNaN, err := MakeGeometryFromPointCoords(math.NaN(), 1)
		require.NoError(t, err)
		_, err = GeometryToLatLonText(gNaN, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid coordinate")
	})

	t.Run("bad_formats", func(t *testing.T) {
		pt := MustParseGeometry("POINT(1.23456 7.89012)")
		badFormats := []struct {
			name   string
			format string
			errSub string
		}{
			{"seconds_without_minutes", "DD.DDD SS.SSS", "cannot include seconds"},
			{"missing_degrees", "MM.MMM SS.SSS", "degrees (DD.DDD) must be included"},
			{"duplicate_degrees", "DD.DDD SS.SSS DD", "cannot include degrees"},
			{"duplicate_minutes", "DD MM SS MM", "cannot include minutes"},
			{"duplicate_seconds", "DD MM SS SS", "cannot include seconds"},
			{"duplicate_compass", "C DD.DDD C", "cannot include compass dir"},
			{"duplicate_deg_after_all_pieces", "a D b M c S d C e D", "cannot include degrees"},
			{"duplicate_compass_after_all_pieces", "a D b M c S d C e C", "cannot include compass dir"},
			{"invalid_utf8_lead_before_D", "C \xC2DD.DDD", "valid UTF-8"},
			{"invalid_utf8_trailing_lead", "C DD.DDD \xC2", "valid UTF-8"},
			{"invalid_utf8_continuation", "C DD\x80MM ", "valid UTF-8"},
			{"invalid_utf8_0xFF", "C DD \xFFMM", "valid UTF-8"},
			{"invalid_utf8_0xB0", "C DD \xB0MM", "valid UTF-8"},
			{"exceeds_max_length_digits", "DD." + strings.Repeat("D", 1024), "exceeds maximum length"},
			{"exceeds_max_length_literals", "DD.DDD " + strings.Repeat("j", 1024), "exceeds maximum length"},
			{"deg_digits_exceed_working_limit", strings.Repeat("D", 1023), "number of digits exceeds working limit"},
			{"min_digits_exceed_working_limit", "D" + strings.Repeat("M", 1023), "number of digits exceeds working limit"},
			{"only_compass", "C", "degrees (DD.DDD) must be included"},
			{"only_literals", "hello world", "degrees (DD.DDD) must be included"},
		}
		for _, tc := range badFormats {
			t.Run(tc.name, func(t *testing.T) {
				_, err := GeometryToLatLonText(pt, tc.format)
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errSub)
			})
		}
	})
}

func BenchmarkGeometryToLatLonText(b *testing.B) {
	benchCases := []struct {
		name   string
		ewkt   string
		format string
	}{
		{"default_format", "POINT(-3.2342342 -2.32498)", ""},
		{"custom_dms", "POINT(-3.2342342 -2.32498)", "D degrees, M minutes, S seconds to the C"},
		{"decimal_degrees", "POINT(-3.2342342 -2.32498)", "D.DDDD degrees C"},
		{"wrap_and_carry", "POINT(180.0001 90.0001)", "D-M-S-C"},
	}

	for _, bc := range benchCases {
		g := MustParseGeometry(bc.ewkt)
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := GeometryToLatLonText(g, bc.format); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
