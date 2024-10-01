// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgreplparser

import (
	"fmt"
	"go/constant"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/pgrepltree"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

const (
	singleQuote = '\''
	doubleQuote = '"'
)

func IsReplicationProtocolCommand(q string) bool {
	l := newLexer(q)
	var lval pgreplSymType
	switch l.Lex(&lval) {
	case K_CREATE_REPLICATION_SLOT, K_DROP_REPLICATION_SLOT, K_READ_REPLICATION_SLOT, K_START_REPLICATION,
		K_IDENTIFY_SYSTEM,
		K_TIMELINE_HISTORY, K_BASE_BACKUP:
		return true
	}
	return false
}

// lexer implements the Lexer goyacc interface.
// It differs from the sql/scanner package as replication has unique, case
// sensitive behavior with unique keywords.
// See: https://github.com/postgres/postgres/blob/4327f6c7480fea9348ea6825a9d38a71b2ef8785/src/backend/replication/repl_scanner.l
type lexer struct {
	in  string
	pos int

	lastToken pgreplSymType
	lastError error
	stmt      pgrepltree.ReplicationStatement
}

func newLexer(str string) *lexer {
	return &lexer{in: str}
}

func (l *lexer) Lex(lval *pgreplSymType) int {
	if l.done() {
		lval.SetID(0)
		lval.SetPos(int32(len(l.in)))
		lval.SetStr("EOF")
		return 0
	}
	l.lex(lval)
	l.lastToken = *lval
	return int(lval.id)
}

func (l *lexer) Error(e string) {
	e = strings.TrimPrefix(e, "syntax error: ") // we'll add it again below.
	err := pgerror.WithCandidateCode(errors.Newf("%s", e), pgcode.Syntax)
	lastTok := l.lastToken
	l.lastError = parser.PopulateErrorDetails(lastTok.id, lastTok.str, lastTok.pos, err, l.in)
}

func (l *lexer) lex(lval *pgreplSymType) {
	l.skipSpace()

	lval.SetUnionVal(nil)
	startPos := l.pos
	lval.SetPos(int32(startPos))
	ch := l.next()
	switch {
	case ch == singleQuote:
		l.scanUntilEndQuote(lval, singleQuote, nil, SCONST)
	case ch == doubleQuote:
		l.scanUntilEndQuote(lval, doubleQuote, lexbase.NormalizeString, IDENT)
	case unicode.IsDigit(ch):
		curPos := l.pos
	scanNumLoop:
		for curPos < len(l.in) {
			nextCh, sz := utf8.DecodeRuneInString(l.in[curPos:])
			switch {
			case unicode.IsDigit(nextCh):
				// We are either a number or part of a LSN.
				// We only enter the LSN check once we detect a forward slash.
			case isHexDigit(nextCh) || nextCh == '/':
				// Any presence of a hex digit or / may make it an LSN.
				if l.checkLSN(lval, startPos) {
					return
				}
				// Otherwise, we are done - anything before this position is a number.
				break scanNumLoop
			default:
				break scanNumLoop
			}
			curPos += sz
		}
		lval.SetStr(l.in[startPos:curPos])
		lval.SetID(UCONST)
		l.pos = curPos
		val, err := strconv.ParseUint(lval.Str(), 10, 64)
		if err != nil {
			l.lexerError(lval, err)
			return
		}
		lval.SetUnionVal(tree.NewNumVal(constant.MakeUint64(val), lval.Str(), false))
	case isIdentifier(ch, true):
		curPos := l.pos
	scanChLoop:
		for curPos < len(l.in) {
			nextCh, sz := utf8.DecodeRuneInString(l.in[curPos:])
			switch {
			case isIdentifier(nextCh, false):
			case nextCh == '/':
				// See if it is an LSN.
				if l.checkLSN(lval, startPos) {
					return
				}
				// Otherwise, we are done.
				break scanChLoop
			default:
				break scanChLoop
			}
			curPos += sz
		}
		str := l.in[startPos:curPos]
		id := l.getKeywordID(str)
		lval.SetID(id)
		if id == IDENT {
			str = lexbase.NormalizeName(str)
		}
		lval.SetStr(str)
		l.pos = curPos
	default:
		// Otherwise, it is the character itself.
		lval.SetID(ch)
		lval.SetStr(string(ch))
	}
}

func (l *lexer) done() bool {
	return l.pos >= len(l.in)
}

const (
	identifierUnicodeMin = 200
	identifierUnicodeMax = 377
)

func isIdentifier(ch rune, firstCh bool) bool {
	if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= identifierUnicodeMin && ch <= identifierUnicodeMax) || ch == '_' {
		return true
	}
	return !firstCh && unicode.IsDigit(ch)
}

func isHexDigit(nextCh rune) bool {
	return (nextCh >= 'A' && nextCh <= 'F') || (nextCh >= 'a' && nextCh <= 'f') || (nextCh >= '0' && nextCh <= '9')
}

// checkLSN checks that the current symbol is an LSN type.
// This is represented by XXXXXXXX/XXXXXXXX, where X is a hexadecimal digit.
func (l *lexer) checkLSN(lval *pgreplSymType, startPos int) bool {
	curPos := startPos

	// Read up to '/'.
	for curPos < len(l.in) && l.in[curPos] != '/' {
		nextCh, sz := utf8.DecodeRuneInString(l.in[curPos:])
		if !isHexDigit(nextCh) {
			return false
		}
		curPos += sz
	}

	// Check we have a '/'.
	if curPos < len(l.in) && l.in[curPos] != '/' {
		return false
	}
	curPos++

	afterSlashPos := curPos
	for curPos < len(l.in) {
		nextCh, sz := utf8.DecodeRuneInString(l.in[curPos:])
		if !isHexDigit(nextCh) {
			break
		}
		curPos += sz
	}
	// If we haven't moved, we're not an LSN.
	if afterSlashPos == curPos {
		return false
	}

	str := l.in[startPos:curPos]
	lval.SetStr(str)
	lval.SetID(RECPTR)
	l.pos = curPos
	lsnVal, err := lsn.ParseLSN(str)
	if err != nil {
		l.lexerError(lval, errors.Wrap(err, "error decoding LSN"))
		return true
	}
	lval.SetUnionVal(lsnVal)
	return true
}

func (l *lexer) lexerError(lval *pgreplSymType, err error) {
	l.lastToken = *lval
	lval.id = ERROR
	l.Error(err.Error())
}

func (l *lexer) setErr(err error) {
	l.lastError = err
}

func (l *lexer) scanUntilEndQuote(
	lval *pgreplSymType, quoteCh rune, normFunc func(string) string, id int32,
) {
	str := ""
	for l.pos < len(l.in) {
		nextCh := l.next()
		// Double quotes = 1 single quote.
		if nextCh == quoteCh {
			if l.peek() == quoteCh {
				l.next()
			} else {
				lval.SetID(id)
				if normFunc != nil {
					str = normFunc(str)
				}
				lval.SetStr(str)
				return
			}
		}
		str += string(nextCh)
	}
	lval.SetID(pgreplErrCode)
	lval.SetStr(fmt.Sprintf("unfinished quote: %c", quoteCh))
}

func (l *lexer) peek() rune {
	ch, _ := utf8.DecodeRuneInString(l.in[l.pos:])
	return ch
}

func (l *lexer) next() rune {
	ch, sz := utf8.DecodeRuneInString(l.in[l.pos:])
	l.pos += sz
	return ch
}

func (l *lexer) skipSpace() {
	if inc := strings.IndexFunc(l.in[l.pos:], func(r rune) bool { return !unicode.IsSpace(r) }); inc > 0 {
		l.pos += inc
	}
}

func (l *lexer) getKeywordID(str string) int32 {
	switch str {
	case "BASE_BACKUP":
		return K_BASE_BACKUP
	case "IDENTIFY_SYSTEM":
		return K_IDENTIFY_SYSTEM
	case "READ_REPLICATION_SLOT":
		return K_READ_REPLICATION_SLOT
	case "CREATE_REPLICATION_SLOT":
		return K_CREATE_REPLICATION_SLOT
	case "START_REPLICATION":
		return K_START_REPLICATION
	case "DROP_REPLICATION_SLOT":
		return K_DROP_REPLICATION_SLOT
	case "TIMELINE_HISTORY":
		return K_TIMELINE_HISTORY
	case "WAIT":
		return K_WAIT
	case "TIMELINE":
		return K_TIMELINE
	case "PHYSICAL":
		return K_PHYSICAL
	case "LOGICAL":
		return K_LOGICAL
	case "RESERVE_WAL":
		return K_RESERVE_WAL
	case "TEMPORARY":
		return K_TEMPORARY
	case "TWO_PHASE":
		return K_TWO_PHASE
	case "EXPORT_SNAPSHOT":
		return K_EXPORT_SNAPSHOT
	case "NOEXPORT_SNAPSHOT":
		return K_NOEXPORT_SNAPSHOT
	case "SLOT":
		return K_SLOT
	case "USE_SNAPSHOT":
		return K_USE_SNAPSHOT
	}
	return IDENT
}
