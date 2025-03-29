// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

type LogIdent FullLogID

func (l *LogIdent) LogKeyKind() LogKeyKind {
	return LKKIdent
}
