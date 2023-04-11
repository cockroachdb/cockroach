// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package plsql

import (
	"fmt"
	"testing"
)

func TestParser(t *testing.T) {
	for _, in := range []string{
		//"DECLARE foo INT := (SELECT 0); BEGIN foo := (SELECT 1); END",
		//"DECLARE ; BEGIN IF (SELECT True) THEN foo := (SELECT 1); END IF; END",
		//"DECLARE i INT := (SELECT 0); sum INT := (SELECT 0); BEGIN LOOP IF (SELECT i >= 10) THEN RETURN (SELECT sum); ELSE sum := (SELECT sum + i); END IF; END LOOP; END",
		//"DECLARE x INT := (0); y INT := (x + 1); BEGIN IF (x < 0) THEN RETURN (0); ELSE RETURN (x); END IF; END",
		//"DECLARE x INT := (0); BEGIN; END",
		//"DECLARE x INT := (0); BEGIN LOOP x := (x + 1); IF (x >= 10) THEN EXIT; END IF; END LOOP; RETURN (x); END",
		//"  DECLARE\n    x INT := (0);\n  BEGIN\n    LOOP x := (x + 1);\n      IF (x >= 10)\n        THEN EXIT;\n      END IF;\n    END LOOP;\n    RETURN (x);\n  END",
		"  DECLARE\n    n INT := (SELECT count(*) FROM edges);\n    i INT := (0);\n    count INT := (0);\n    dist INT[] := (ARRAY[]::INT[]);\n    sptSet BOOL[] := (ARRAY[]::BOOL[]);\n    u INT;\n  BEGIN\n    LOOP IF (i > n) THEN EXIT; END IF;\n      dist := (dist || max_int());\n      sptSet := (sptSet || False);\n      i := (i + 1);\n    END LOOP;\n    dist := (array_replace(dist, 0, 1));\n    i := (0);\n    LOOP IF (count >= n) THEN EXIT; END IF;\n      u := (min_distance(dist, sptSet));\n      sptSet := (array_replace(sptSet, False, u+1));\n      LOOP IF (i > n) THEN EXIT; END IF;\n        IF (\n          (NOT sptSet[i]) AND\n          graph(u, i) > 0 AND\n          dist[u] <> max_int() AND\n          dist[u] + graph(u, i) < dist[i]\n        )\n        THEN\n          dist := (array_replace(dist, dist[u] + graph(u, i), i+1));\n        END IF;\n        i := (i + 1);\n      END LOOP;\n      count := (count + 1);\n    END LOOP;\n    RETURN (dist);\n  END",
	} {
		v, err := Parse(in)
		fmt.Println("======================================================")
		fmt.Println(in)
		fmt.Println(v)
		fmt.Println(err)
		fmt.Println()
	}
}
