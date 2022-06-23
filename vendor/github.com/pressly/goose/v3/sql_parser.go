package goose

import (
	"bufio"
	"bytes"
	"io"
	"regexp"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

type parserState int

const (
	start                   parserState = iota // 0
	gooseUp                                    // 1
	gooseStatementBeginUp                      // 2
	gooseStatementEndUp                        // 3
	gooseDown                                  // 4
	gooseStatementBeginDown                    // 5
	gooseStatementEndDown                      // 6
)

type stateMachine parserState

func (s *stateMachine) Get() parserState {
	return parserState(*s)
}
func (s *stateMachine) Set(new parserState) {
	verboseInfo("StateMachine: %v => %v", *s, new)
	*s = stateMachine(new)
}

const scanBufSize = 4 * 1024 * 1024

var matchEmptyLines = regexp.MustCompile(`^\s*$`)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, scanBufSize)
	},
}

// Split given SQL script into individual statements and return
// SQL statements for given direction (up=true, down=false).
//
// The base case is to simply split on semicolons, as these
// naturally terminate a statement.
//
// However, more complex cases like pl/pgsql can have semicolons
// within a statement. For these cases, we provide the explicit annotations
// 'StatementBegin' and 'StatementEnd' to allow the script to
// tell us to ignore semicolons.
func parseSQLMigration(r io.Reader, direction bool) (stmts []string, useTx bool, err error) {
	var buf bytes.Buffer
	scanBuf := bufferPool.Get().([]byte)
	defer bufferPool.Put(scanBuf)

	scanner := bufio.NewScanner(r)
	scanner.Buffer(scanBuf, scanBufSize)

	stateMachine := stateMachine(start)
	useTx = true

	for scanner.Scan() {
		line := scanner.Text()
		if verbose {
			log.Println(line)
		}

		if strings.HasPrefix(line, "--") {
			cmd := strings.TrimSpace(strings.TrimPrefix(line, "--"))

			switch cmd {
			case "+goose Up":
				switch stateMachine.Get() {
				case start:
					stateMachine.Set(gooseUp)
				default:
					return nil, false, errors.Errorf("duplicate '-- +goose Up' annotations; stateMachine=%v, see https://github.com/pressly/goose#sql-migrations", stateMachine)
				}
				continue

			case "+goose Down":
				switch stateMachine.Get() {
				case gooseUp, gooseStatementEndUp:
					stateMachine.Set(gooseDown)
				default:
					return nil, false, errors.Errorf("must start with '-- +goose Up' annotation, stateMachine=%v, see https://github.com/pressly/goose#sql-migrations", stateMachine)
				}
				continue

			case "+goose StatementBegin":
				switch stateMachine.Get() {
				case gooseUp, gooseStatementEndUp:
					stateMachine.Set(gooseStatementBeginUp)
				case gooseDown, gooseStatementEndDown:
					stateMachine.Set(gooseStatementBeginDown)
				default:
					return nil, false, errors.Errorf("'-- +goose StatementBegin' must be defined after '-- +goose Up' or '-- +goose Down' annotation, stateMachine=%v, see https://github.com/pressly/goose#sql-migrations", stateMachine)
				}
				continue

			case "+goose StatementEnd":
				switch stateMachine.Get() {
				case gooseStatementBeginUp:
					stateMachine.Set(gooseStatementEndUp)
				case gooseStatementBeginDown:
					stateMachine.Set(gooseStatementEndDown)
				default:
					return nil, false, errors.New("'-- +goose StatementEnd' must be defined after '-- +goose StatementBegin', see https://github.com/pressly/goose#sql-migrations")
				}

			case "+goose NO TRANSACTION":
				useTx = false
				continue

			default:
				// Ignore comments.
				verboseInfo("StateMachine: ignore comment")
				continue
			}
		}

		// Ignore empty lines.
		if matchEmptyLines.MatchString(line) {
			verboseInfo("StateMachine: ignore empty line")
			continue
		}

		// Write SQL line to a buffer.
		if _, err := buf.WriteString(line + "\n"); err != nil {
			return nil, false, errors.Wrap(err, "failed to write to buf")
		}

		// Read SQL body one by line, if we're in the right direction.
		//
		// 1) basic query with semicolon; 2) psql statement
		//
		// Export statement once we hit end of statement.
		switch stateMachine.Get() {
		case gooseUp, gooseStatementBeginUp, gooseStatementEndUp:
			if !direction /*down*/ {
				buf.Reset()
				verboseInfo("StateMachine: ignore down")
				continue
			}
		case gooseDown, gooseStatementBeginDown, gooseStatementEndDown:
			if direction /*up*/ {
				buf.Reset()
				verboseInfo("StateMachine: ignore up")
				continue
			}
		default:
			return nil, false, errors.Errorf("failed to parse migration: unexpected state %q on line %q, see https://github.com/pressly/goose#sql-migrations", stateMachine, line)
		}

		switch stateMachine.Get() {
		case gooseUp:
			if endsWithSemicolon(line) {
				stmts = append(stmts, buf.String())
				buf.Reset()
				verboseInfo("StateMachine: store simple Up query")
			}
		case gooseDown:
			if endsWithSemicolon(line) {
				stmts = append(stmts, buf.String())
				buf.Reset()
				verboseInfo("StateMachine: store simple Down query")
			}
		case gooseStatementEndUp:
			stmts = append(stmts, buf.String())
			buf.Reset()
			verboseInfo("StateMachine: store Up statement")
			stateMachine.Set(gooseUp)
		case gooseStatementEndDown:
			stmts = append(stmts, buf.String())
			buf.Reset()
			verboseInfo("StateMachine: store Down statement")
			stateMachine.Set(gooseDown)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, false, errors.Wrap(err, "failed to scan migration")
	}
	// EOF

	switch stateMachine.Get() {
	case start:
		return nil, false, errors.New("failed to parse migration: must start with '-- +goose Up' annotation, see https://github.com/pressly/goose#sql-migrations")
	case gooseStatementBeginUp, gooseStatementBeginDown:
		return nil, false, errors.New("failed to parse migration: missing '-- +goose StatementEnd' annotation")
	}

	if bufferRemaining := strings.TrimSpace(buf.String()); len(bufferRemaining) > 0 {
		return nil, false, errors.Errorf("failed to parse migration: state %q, direction: %v: unexpected unfinished SQL query: %q: missing semicolon?", stateMachine, direction, bufferRemaining)
	}

	return stmts, useTx, nil
}

// Checks the line to see if the line has a statement-ending semicolon
// or if the line contains a double-dash comment.
func endsWithSemicolon(line string) bool {
	scanBuf := bufferPool.Get().([]byte)
	defer bufferPool.Put(scanBuf)

	prev := ""
	scanner := bufio.NewScanner(strings.NewReader(line))
	scanner.Buffer(scanBuf, scanBufSize)
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		word := scanner.Text()
		if strings.HasPrefix(word, "--") {
			break
		}
		prev = word
	}

	return strings.HasSuffix(prev, ";")
}
