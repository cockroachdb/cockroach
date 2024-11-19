// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgformat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// pp is used to store a printer's state.
type pp struct {
	evalCtx *eval.Context
	buf     *tree.FmtCtx

	// padRight records whether the '-' flag is currently in effect.
	padRight bool
	// num records an integer that could be a width or position, we won't know
	// until we hit a $ or verb.
	num *int
	// width gives the minimum width of the next value
	width int
}

func (p *pp) clearState() {
	p.padRight = false
	p.num = nil
	p.width = 0
}

func (p *pp) popInt() (v int, ok bool) {
	if p.num == nil {
		return
	}
	v = *p.num
	ok = true
	p.num = nil
	return
}

// Format formats according to a format specifier in the style of postgres format()
// and returns the resulting string.
func Format(
	ctx context.Context, evalCtx *eval.Context, format string, a ...tree.Datum,
) (string, error) {
	p := pp{
		evalCtx: evalCtx,
		buf:     evalCtx.FmtCtx(tree.FmtArrayToString),
	}
	err := p.doPrintf(ctx, format, a)
	if err != nil {
		return "", err
	}
	s := p.buf.CloseAndGetString()
	return s, nil
}

// tooLarge reports whether the magnitude of the integer is
// too large to be used as a formatting width or precision.
func tooLarge(x int) bool {
	const max int = 1e6
	return x > max || x < -max
}

// parsenum converts ASCII to integer.  num is 0 (and isnum is false) if no number present.
func parsenum(s string, start, end int) (num int, isnum bool, newi int) {
	if start >= end {
		return 0, false, end
	}
	for newi = start; newi < end && '0' <= s[newi] && s[newi] <= '9'; newi++ {
		if tooLarge(num) {
			return 0, false, end
		}
		num = num*10 + int(s[newi]-'0')
		isnum = true
	}
	return
}

func (p *pp) printArg(ctx context.Context, arg tree.Datum, verb rune) (err error) {
	var writeFunc func(*tree.FmtCtx) (numBytesWritten int)
	if arg == tree.DNull {
		switch verb {
		case 's':
			writeFunc = func(_ *tree.FmtCtx) int { return 0 }
		case 'I':
			return errors.New("NULL cannot be formatted as a SQL identifier")
		case 'L':
			writeFunc = func(buf *tree.FmtCtx) int { buf.WriteString("NULL"); return 4 }
		}
	} else {
		switch verb {
		case 's':
			writeFunc = func(buf *tree.FmtCtx) int {
				lenBefore := buf.Len()
				buf.FormatNode(arg)
				return buf.Len() - lenBefore
			}
		case 'I':
			writeFunc = func(buf *tree.FmtCtx) int {
				lenBefore := buf.Len()
				bare := p.evalCtx.FmtCtx(tree.FmtArrayToString)
				bare.FormatNode(arg)
				str := bare.CloseAndGetString()
				lexbase.EncodeRestrictedSQLIdent(&buf.Buffer, str, lexbase.EncNoFlags)
				return buf.Len() - lenBefore
			}
		case 'L':
			writeFunc = func(buf *tree.FmtCtx) int {
				lenBefore := buf.Len()
				var dStr tree.Datum
				dStr, err = eval.PerformCast(ctx, p.evalCtx, arg, types.String)
				// This shouldn't be possible--anything can be cast to
				// a string. err will be returned by printArg().
				if err != nil {
					return 0
				}
				lexbase.EncodeSQLString(&buf.Buffer, string(tree.MustBeDString(dStr)))
				return buf.Len() - lenBefore
			}
		}
	}
	if p.width == 0 {
		writeFunc(p.buf)
		return
	}

	// negative width passed via * sets the - flag,
	// does not toggle it.
	if p.width < 0 {
		p.width = -p.width
		p.padRight = true
	}

	if p.padRight {
		for n := writeFunc(p.buf); n < p.width; n++ {
			p.buf.WriteRune(' ')
		}
		return
	}

	scratch := p.evalCtx.FmtCtx(tree.FmtArrayToString)
	for n := writeFunc(scratch); n < p.width; n++ {
		p.buf.WriteRune(' ')
	}
	_, err = scratch.WriteTo(p.buf)
	scratch.Close()
	return
}

// intFromArg gets the argNumth element of a. On return, isInt reports whether the argument has integer type.
func intFromArg(
	ctx context.Context, evalCtx *eval.Context, a []tree.Datum, argNum int,
) (num int, isInt bool, newArgNum int) {
	newArgNum = argNum
	if argNum < len(a) && argNum >= 0 {
		datum := a[argNum]
		// null is interpreted as 0 as in postgres.
		if datum == tree.DNull {
			return 0, true, argNum + 1
		}
		if cast.ValidCast(datum.ResolvedType(), types.Int, cast.ContextImplicit) {
			dInt, err := eval.PerformCast(ctx, evalCtx, datum, types.Int)
			if err == nil {
				num = int(tree.MustBeDInt(dInt))
				isInt = true
				newArgNum = argNum + 1
			}
			if tooLarge(num) {
				num = 0
				isInt = false
			}
		}
	}
	return
}

// doPrintf is copied from golang's internal implementation of fmt,
// but modified to use the sql function format()'s syntax for width
// and positional arguments.
func (p *pp) doPrintf(ctx context.Context, format string, a []tree.Datum) error {
	end := len(format)
	argNum := 0 // we process one argument per non-trivial format
formatLoop:
	for i := 0; i < end; {
		lasti := i
		for i < end && format[i] != '%' {
			i++
		}
		if i > lasti {
			p.buf.WriteString(format[lasti:i])
		}
		if i >= end {
			// done processing format string
			break
		}

		// Process one verb
		i++

		p.clearState()
		for ; i < end; i++ {
			c := format[i]
			switch c {
			case '-':
				p.padRight = true
			case 's', 'I', 'L':
				if p.width == 0 {
					p.width, _ = p.popInt()
				}
				if argNum >= len(a) {
					return errors.New("not enough arguments")
				}
				if argNum < 0 {
					return errors.New("positions must be positive and 1-indexed")
				}
				err := p.printArg(ctx, a[argNum], rune(c))
				if err != nil {
					return err
				}
				argNum++
				i++
				continue formatLoop
			case '%':
				p.buf.WriteByte(c)
				i++
				continue formatLoop
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				var n int
				n, _, i = parsenum(format, i, end)
				p.num = &n
				i--
			case '$':
				rawArgNum, ok := p.popInt()
				if !ok {
					return errors.New("empty positional argument")
				}
				argNum = rawArgNum - 1
				if p.width != 0 {
					return errors.New("positional argument flag must precede width flag")
				}
			case '*':
				var rawArgNum int
				var isNum bool
				rawArgNum, isNum, afterNum := parsenum(format, i+1, end)
				if isNum {
					i = afterNum
					if i == end || format[i] != '$' {
						return errors.New(`width argument position must be ended by "$"`)
					}
					if rawArgNum < 1 {
						return errors.New("positions must be positive and 1-indexed")
					}
					p.width, isNum, argNum = intFromArg(ctx, p.evalCtx, a, rawArgNum-1)
					if !isNum {
						return errors.New("non-numeric width")
					}
				} else {
					p.width, isNum, argNum = intFromArg(ctx, p.evalCtx, a, argNum)
					if !isNum {
						return errors.New("non-numeric width")
					}
				}
			default:
				return errors.Newf("unrecognized verb %c", c)
			}

		}
		return errors.New("unterminated format specifier")
	}
	return nil
}
