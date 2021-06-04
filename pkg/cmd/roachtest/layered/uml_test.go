// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package layered

import (
	"testing"
	"time"
)

func TestUMLGenerator(t *testing.T) {
	t0 := time.Time{}
	var r UMLRecorder
	r.Record(t0, "Worker 1", "idle")
	r.Record(t0, "Worker 2", "idle")
	r.Record(t0.Add(10*time.Second), "Worker 1", "Foo Foo")
	r.Record(t0.Add(13*time.Second), "Worker 2", "Bar Bar")
	r.Record(t0.Add(20*time.Second), "Worker 1", "Idle")
	r.Record(t0.Add(19*time.Second), "Worker 2", "Idle")
	t.Log(r.String())
}
