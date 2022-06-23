// Package robustdeterminate implements an algorithm to compute the
// sign of a 2x2 determinant for double precision values robustly.
// It is a direct translation of code developed by Olivier Devillers.
//
// The original code carries the following copyright notice:
////////////////////////////////////////////////////////////////////////////////
//
//  Author : Olivier Devillers
//  Olivier.Devillers@sophia.inria.fr
//  http:/www.inria.fr:/prisme/personnel/devillers/anglais/determinant.html
//
//  Relicensed under EDL and EPL with Permission from Olivier Devillers
//
//
//               Copyright (c) 1995  by  INRIA Prisme Project
//                   BP 93 06902 Sophia Antipolis Cedex, France.
//                            All rights reserved
//
//
////////////////////////////////////////////////////////////////////////////////
package robustdeterminate

import "math"

// Sign enumerates the different possible signs
type Sign int

const (
	// Negative indicates a negative determinate
	Negative Sign = iota - 1
	// Zero indicates the determinate is 0
	Zero
	// Positive indicates a positive determinate
	Positive
)

// SignOfDet2x2 computes the sign of the determinant of the 2x2 matrix
// with the given entries, in a robust way.
//
// return -1 if the determinant is negative,
//  return  1 if the determinant is positive,
// return  0 if the determinant is 0.
func SignOfDet2x2(x1, y1, x2, y2 float64) Sign {
	var swap float64
	var k float64
	var count int64

	/*
	 *  testing null entries
	 */
	if x1 == 0.0 || y2 == 0.0 {
		switch {
		case y1 == 0.0 || x2 == 0.0:
			return Zero
		case y1 > 0:
			if x2 > 0 {
				return Negative
			}
			return Positive
		default:
			if x2 > 0 {
				return Positive
			}
			return Negative
		}
	}
	if y1 == 0.0 || x2 == 0.0 {
		if y2 > 0 {
			if x1 > 0 {
				return Positive
			}
			return Negative
		}
		if x1 > 0 {
			return Negative
		}
		return Positive
	}

	sign := Positive

	/*
	 *  making y coordinates positive and permuting the entries
	 */
	/*
	 *  so that y2 is the biggest one
	 */
	if 0.0 < y1 {
		if 0.0 < y2 {
			if y1 <= y2 {
			} else {
				sign = Negative
				swap = x1
				x1 = x2
				x2 = swap
				swap = y1
				y1 = y2
				y2 = swap
			}
		} else {
			if y1 <= -y2 {
				sign = Negative
				x2 = -x2
				y2 = -y2
			} else {
				swap = x1
				x1 = -x2
				x2 = swap
				swap = y1
				y1 = -y2
				y2 = swap
			}
		}
	} else {
		if 0.0 < y2 {
			if -y1 <= y2 {
				sign = Negative
				x1 = -x1
				y1 = -y1
			} else {
				swap = -x1
				x1 = x2
				x2 = swap
				swap = -y1
				y1 = y2
				y2 = swap
			}
		} else {
			if y1 >= y2 {
				x1 = -x1
				y1 = -y1
				x2 = -x2
				y2 = -y2
			} else {
				sign = Negative
				swap = -x1
				x1 = -x2
				x2 = swap
				swap = -y1
				y1 = -y2
				y2 = swap
			}
		}
	}

	/*
	 *  making x coordinates positive
	 */
	/*
	 *  if |x2| < |x1| one can conclude
	 */
	if 0.0 < x1 {
		if 0.0 < x2 {
			if x1 <= x2 {
			} else {
				return sign
			}
		} else {
			return sign
		}
	} else {
		if 0.0 < x2 {
			return -sign
		}
		if x1 >= x2 {
			sign = -sign
			x1 = -x1
			x2 = -x2
		} else {
			return -sign
		}
	}

	/*
	 *  all entries strictly positive   x1 <= x2 and y1 <= y2
	 */
	for {
		count++
		// MD - UNSAFE HACK for testing only!
		//      k = (int) (x2 / x1);
		k = math.Floor(x2 / x1)
		x2 -= k * x1
		y2 -= k * y1

		/*
		 *  testing if R (new U2) is in U1 rectangle
		 */
		if y2 < 0.0 {
			return -sign
		}
		if y2 > y1 {
			return sign
		}

		/*
		 *  finding R'
		 */
		if x1 > x2+x2 {
			if y1 < y2+y2 {
				return sign
			}
		} else {
			if y1 > y2+y2 {
				return -sign
			}
			x2 = x1 - x2
			y2 = y1 - y2
			sign = -sign
		}
		if y2 == 0.0 {
			if x2 == 0.0 {
				return 0
			}
			return -sign
		}
		if x2 == 0.0 {
			return sign
		}

		/*
		 *  exchange 1 and 2 role.
		 */
		// MD - UNSAFE HACK for testing only!
		//      k = (int) (x1 / x2);
		k = math.Floor(x1 / x2)
		x1 -= k * x2
		y1 -= k * y2

		/*
		 *  testing if R (new U1) is in U2 rectangle
		 */
		if y1 < 0.0 {
			return sign
		}
		if y1 > y2 {
			return -sign
		}

		/*
		 *  finding R'
		 */
		if x2 > x1+x1 {
			if y2 < y1+y1 {
				return -sign
			}
		} else {
			if y2 > y1+y1 {
				return sign
			}
			x1 = x2 - x1
			y1 = y2 - y1
			sign = -sign
		}
		if y1 == 0.0 {
			if x1 == 0.0 {
				return 0
			}
			return sign
		}
		if x1 == 0.0 {
			return -sign
		}
	}
}
