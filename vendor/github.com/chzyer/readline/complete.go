package readline

import (
	"bytes"
	"fmt"
	"io"

	"github.com/chzyer/readline/runes"
)

type AutoCompleter interface {
	// Readline will pass the whole line and current offset to it
	// Completer need to pass all the candidates, and how long they shared the same characters in line
	// Example:
	//   Do("g", 1) => ["o", "it", "it-shell", "rep"], 1
	//   Do("gi", 2) => ["t", "t-shell"], 1
	//   Do("git", 3) => ["", "-shell"], 0
	Do(line []rune, pos int) (newLine [][]rune, length int)
}

type opCompleter struct {
	w  io.Writer
	op *Operation
	ac AutoCompleter

	inCompleteMode  bool
	inSelectMode    bool
	candidate       [][]rune
	candidateSource []rune
	candidateOff    int
	candidateChoise int
	candidateColNum int
}

func newOpCompleter(w io.Writer, op *Operation) *opCompleter {
	return &opCompleter{
		w:  w,
		op: op,
		ac: op.cfg.AutoComplete,
	}
}

func (o *opCompleter) doSelect() {
	if len(o.candidate) == 1 {
		o.op.buf.WriteRunes(o.candidate[0])
		o.ExitCompleteMode(false)
		return
	}
	o.nextCandidate(1)
	o.CompleteRefresh()
}

func (o *opCompleter) nextCandidate(i int) {
	o.candidateChoise += i
	o.candidateChoise = o.candidateChoise % len(o.candidate)
	if o.candidateChoise < 0 {
		o.candidateChoise = len(o.candidate) + o.candidateChoise
	}
}

func (o *opCompleter) OnComplete() {
	if o.IsInCompleteSelectMode() {
		o.doSelect()
		return
	}

	buf := o.op.buf
	rs := buf.Runes()

	if o.IsInCompleteMode() && runes.Equal(rs, o.candidateSource) {
		o.EnterCompleteSelectMode()
		o.doSelect()
		return
	}

	o.ExitCompleteSelectMode()
	o.candidateSource = rs
	newLines, offset := o.ac.Do(rs, buf.idx)
	if len(newLines) == 0 {
		o.ExitCompleteMode(false)
		return
	}

	// only Aggregate candidates in non-complete mode
	if !o.IsInCompleteMode() {
		if len(newLines) == 1 {
			buf.WriteRunes(newLines[0])
			o.ExitCompleteMode(false)
			return
		}

		same, size := runes.Aggregate(newLines)
		if size > 0 {
			buf.WriteRunes(same)
			o.ExitCompleteMode(false)
			return
		}
	}

	o.EnterCompleteMode(offset, newLines)
}

func (o *opCompleter) IsInCompleteSelectMode() bool {
	return o.inSelectMode
}

func (o *opCompleter) IsInCompleteMode() bool {
	return o.inCompleteMode
}

func (o *opCompleter) HandleCompleteSelect(r rune) bool {
	next := true
	switch r {
	case CharEnter, CharCtrlJ:
		next = false
		o.op.buf.WriteRunes(o.op.candidate[o.op.candidateChoise])
		o.ExitCompleteMode(false)
	case CharLineStart:
		num := o.candidateChoise % o.candidateColNum
		o.nextCandidate(-num)
	case CharLineEnd:
		num := o.candidateColNum - o.candidateChoise%o.candidateColNum - 1
		o.candidateChoise += num
		if o.candidateChoise >= len(o.candidate) {
			o.candidateChoise = len(o.candidate) - 1
		}
	case CharBackspace:
		o.ExitCompleteSelectMode()
		next = false
	case CharTab, CharForward:
		o.doSelect()
	case CharBell, CharInterrupt:
		o.ExitCompleteMode(true)
		next = false
	case CharNext:
		tmpChoise := o.candidateChoise + o.candidateColNum
		if tmpChoise >= o.getMatrixSize() {
			tmpChoise -= o.getMatrixSize()
		} else if tmpChoise >= len(o.candidate) {
			tmpChoise += o.candidateColNum
			tmpChoise -= o.getMatrixSize()
		}
		o.candidateChoise = tmpChoise
	case CharBackward:
		o.nextCandidate(-1)
	case CharPrev:
		tmpChoise := o.candidateChoise - o.candidateColNum
		if tmpChoise < 0 {
			tmpChoise += o.getMatrixSize()
			if tmpChoise >= len(o.candidate) {
				tmpChoise -= o.candidateColNum
			}
		}
		o.candidateChoise = tmpChoise
	default:
		next = false
		o.ExitCompleteSelectMode()
	}
	if next {
		o.CompleteRefresh()
		return true
	}
	return false
}

func (o *opCompleter) getMatrixSize() int {
	line := len(o.candidate) / o.candidateColNum
	if len(o.candidate)%o.candidateColNum != 0 {
		line++
	}
	return line * o.candidateColNum
}

func (o *opCompleter) CompleteRefresh() {
	if !o.inCompleteMode {
		return
	}
	lineCnt := o.op.buf.CursorLineCount()
	colWidth := 0
	for _, c := range o.candidate {
		w := runes.WidthAll(c)
		if w > colWidth {
			colWidth = w
		}
	}
	colNum := getWidth(o.op.cfg.StdoutFd) / (colWidth + o.candidateOff + 2)
	o.candidateColNum = colNum
	buf := bytes.NewBuffer(nil)
	buf.Write(bytes.Repeat([]byte("\n"), lineCnt))
	same := o.op.buf.RuneSlice(-o.candidateOff)
	colIdx := 0
	lines := 1
	buf.WriteString("\033[J")
	for idx, c := range o.candidate {
		inSelect := idx == o.candidateChoise && o.IsInCompleteSelectMode()
		if inSelect {
			buf.WriteString("\033[30;47m")
		}
		buf.WriteString(string(same))
		buf.WriteString(string(c))
		buf.Write(bytes.Repeat([]byte(" "), colWidth-len(c)))
		if inSelect {
			buf.WriteString("\033[0m")
		}

		buf.WriteString("  ")
		colIdx++
		if colIdx == colNum {
			buf.WriteString("\n")
			lines++
			colIdx = 0
		}
	}

	// move back
	fmt.Fprintf(buf, "\033[%dA\r", lineCnt-1+lines)
	fmt.Fprintf(buf, "\033[%dC", o.op.buf.idx+o.op.buf.PromptLen())
	o.w.Write(buf.Bytes())
}

func (o *opCompleter) aggCandidate(candidate [][]rune) int {
	offset := 0
	for i := 0; i < len(candidate[0]); i++ {
		for j := 0; j < len(candidate)-1; j++ {
			if i > len(candidate[j]) {
				goto aggregate
			}
			if candidate[j][i] != candidate[j+1][i] {
				goto aggregate
			}
		}
		offset = i
	}
aggregate:
	return offset
}

func (o *opCompleter) EnterCompleteSelectMode() {
	o.inSelectMode = true
	o.candidateChoise = -1
	o.CompleteRefresh()
}

func (o *opCompleter) EnterCompleteMode(offset int, candidate [][]rune) {
	o.inCompleteMode = true
	o.candidate = candidate
	o.candidateOff = offset
	o.CompleteRefresh()
}

func (o *opCompleter) ExitCompleteSelectMode() {
	o.inSelectMode = false
	o.candidate = nil
	o.candidateChoise = -1
	o.candidateOff = -1
	o.candidateSource = nil
}

func (o *opCompleter) ExitCompleteMode(revent bool) {
	o.inCompleteMode = false
	o.ExitCompleteSelectMode()
}
