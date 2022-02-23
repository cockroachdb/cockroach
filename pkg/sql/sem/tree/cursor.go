// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "strconv"

// DeclareCursor represents a DECLARE statement.
type DeclareCursor struct {
	Name        Name
	Select      *Select
	Binary      bool
	Scroll      CursorScrollOption
	Sensitivity CursorSensitivity
	Hold        bool
}

// Format implements the NodeFormatter interface.
func (node *DeclareCursor) Format(ctx *FmtCtx) {
	ctx.WriteString("DECLARE ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" ")
	if node.Binary {
		ctx.WriteString("BINARY ")
	}
	if node.Sensitivity != UnspecifiedSensitivity {
		ctx.WriteString(node.Sensitivity.String())
		ctx.WriteString(" ")
	}
	if node.Scroll != UnspecifiedScroll {
		ctx.WriteString(node.Scroll.String())
		ctx.WriteString(" ")
	}
	ctx.WriteString("CURSOR ")
	if node.Hold {
		ctx.WriteString("WITH HOLD ")
	}
	ctx.WriteString("FOR ")
	ctx.FormatNode(node.Select)
}

// CursorScrollOption represents the scroll option, if one was given, for a
// DECLARE statement.
type CursorScrollOption int8

const (
	// UnspecifiedScroll represents no SCROLL option having been given. In
	// Postgres, this is like NO SCROLL, but the returned cursor also supports
	// some simple cases of backward seeking. For CockroachDB, this is the same
	// as NO SCROLL.
	UnspecifiedScroll CursorScrollOption = iota
	// Scroll represents the SCROLL option. It is supposed to indicate that the
	// declared cursor is "scrollable", meaning it can be seeked backward.
	Scroll
	// NoScroll represents the NO SCROLL option, which means that the declared
	// cursor can only be moved forward.
	NoScroll
)

func (o CursorScrollOption) String() string {
	switch o {
	case Scroll:
		return "SCROLL"
	case NoScroll:
		return "NO SCROLL"
	}
	return ""
}

// CursorSensitivity represents the "sensitivity" of a cursor, which describes
// whether it sees writes that occur within the transaction after it was
// declared.
// CockroachDB, like Postgres, only supports "insensitive" cursors, and all
// three variants of sensitivity here resolve to insensitive. SENSITIVE cursors
// are not supported.
type CursorSensitivity int

const (
	// UnspecifiedSensitivity indicates that no sensitivity was specified. This
	// is the same as INSENSITIVE.
	UnspecifiedSensitivity CursorSensitivity = iota
	// Insensitive indicates that the cursor is "insensitive" to subsequent
	// writes, meaning that it sees a snapshot of data from the moment it was
	// declared, and won't see subsequent writes within the transaction.
	Insensitive
	// Asensitive indicates that "the cursor is implementation dependent".
	Asensitive
)

func (o CursorSensitivity) String() string {
	switch o {
	case Insensitive:
		return "INSENSITIVE"
	case Asensitive:
		return "ASENSITIVE"
	}
	return ""
}

// FetchCursor represents a FETCH statement.
type FetchCursor struct {
	Name      Name
	FetchType FetchType
	Count     int64
}

// FetchType represents the type of a FETCH statement.
type FetchType int

const (
	// FetchNormal represents a FETCH statement that doesn't have a special
	// qualifier. It's used for FORWARD, BACKWARD, NEXT, and PRIOR.
	FetchNormal FetchType = iota
	// FetchRelative represents a FETCH RELATIVE statement.
	FetchRelative
	// FetchAbsolute represents a FETCH ABSOLUTE statement.
	FetchAbsolute
	// FetchFirst represents a FETCH FIRST statement.
	FetchFirst
	// FetchLast represents a FETCH LAST statement.
	FetchLast
	// FetchAll represents a FETCH ALL statement.
	FetchAll
	// FetchBackwardAll represents a FETCH BACKWARD ALL statement.
	FetchBackwardAll
)

func (o FetchType) String() string {
	switch o {
	case FetchNormal:
		return ""
	case FetchRelative:
		return "RELATIVE"
	case FetchAbsolute:
		return "ABSOLUTE"
	case FetchFirst:
		return "FIRST"
	case FetchLast:
		return "LAST"
	case FetchAll:
		return "ALL"
	case FetchBackwardAll:
		return "BACKWARD ALL"
	}
	return ""
}

// HasCount returns true if the given fetch type should be printed with an
// associated count.
func (o FetchType) HasCount() bool {
	switch o {
	case FetchNormal, FetchRelative, FetchAbsolute:
		return true
	}
	return false
}

// Format implements the NodeFormatter interface.
func (f FetchCursor) Format(ctx *FmtCtx) {
	ctx.WriteString("FETCH ")
	fetchType := f.FetchType.String()
	if fetchType != "" {
		ctx.WriteString(fetchType)
		ctx.WriteString(" ")
	}
	if f.FetchType.HasCount() {
		ctx.WriteString(strconv.Itoa(int(f.Count)))
		ctx.WriteString(" ")
	}
	ctx.FormatNode(&f.Name)
}

// CloseCursor represents a CLOSE statement.
type CloseCursor struct {
	Name Name
	All  bool
}

// Format implements the NodeFormatter interface.
func (c CloseCursor) Format(ctx *FmtCtx) {
	ctx.WriteString("CLOSE ")
	if c.All {
		ctx.WriteString("ALL")
	} else {
		ctx.FormatNode(&c.Name)
	}
}
