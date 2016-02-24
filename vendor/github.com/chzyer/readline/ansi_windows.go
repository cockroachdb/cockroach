// +build windows

package readline

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
	"unsafe"
)

const (
	_                = uint16(0)
	COLOR_FBLUE      = 0x0001
	COLOR_FGREEN     = 0x0002
	COLOR_FRED       = 0x0004
	COLOR_FINTENSITY = 0x0008

	COLOR_BBLUE      = 0x0010
	COLOR_BGREEN     = 0x0020
	COLOR_BRED       = 0x0040
	COLOR_BINTENSITY = 0x0080

	COMMON_LVB_UNDERSCORE = 0x8000
)

var ColorTableFg = []word{
	0,                                       // 30: Black
	COLOR_FRED,                              // 31: Red
	COLOR_FGREEN,                            // 32: Green
	COLOR_FRED | COLOR_FGREEN,               // 33: Yellow
	COLOR_FBLUE,                             // 34: Blue
	COLOR_FRED | COLOR_FBLUE,                // 35: Magenta
	COLOR_FGREEN | COLOR_FBLUE,              // 36: Cyan
	COLOR_FRED | COLOR_FBLUE | COLOR_FGREEN, // 37: White
}

var ColorTableBg = []word{
	0,                                       // 40: Black
	COLOR_BRED,                              // 41: Red
	COLOR_BGREEN,                            // 42: Green
	COLOR_BRED | COLOR_BGREEN,               // 43: Yellow
	COLOR_BBLUE,                             // 44: Blue
	COLOR_BRED | COLOR_BBLUE,                // 45: Magenta
	COLOR_BGREEN | COLOR_BBLUE,              // 46: Cyan
	COLOR_BRED | COLOR_BBLUE | COLOR_BGREEN, // 47: White
}

type ANSIWriter struct {
	target io.Writer
	ch     chan rune
	wg     sync.WaitGroup
	sync.Mutex
}

func NewANSIWriter(w io.Writer) *ANSIWriter {
	a := &ANSIWriter{
		target: w,
		ch:     make(chan rune),
	}
	go a.ioloop()
	return a
}

func (a *ANSIWriter) Close() error {
	close(a.ch)
	a.wg.Wait()
	return nil
}

func (a *ANSIWriter) ioloop() {
	a.wg.Add(1)
	defer a.wg.Done()

	var (
		ok       bool
		isEsc    bool
		isEscSeq bool

		char rune
		arg  []string

		target = bufio.NewWriter(a.target)
	)

	peek := func() rune {
		select {
		case ch := <-a.ch:
			return ch
		default:
			return 0
		}
	}

read:
	r := char
	if char == 0 {
		r, ok = <-a.ch
		if !ok {
			target.Flush()
			return
		}
	} else {
		char = 0
	}

	if isEscSeq {
		isEscSeq = a.ioloopEscSeq(target, r, &arg)
		goto read
	}

	switch r {
	case CharEsc:
		isEsc = true
	case '[':
		if isEsc {
			arg = nil
			isEscSeq = true
			isEsc = false
			break
		}
		fallthrough
	default:
		target.WriteRune(r)
		char = peek()
		if char == 0 || char == CharEsc {
			target.Flush()
		}
	}
	goto read
}

func (a *ANSIWriter) ioloopEscSeq(w *bufio.Writer, r rune, argptr *[]string) bool {
	arg := *argptr
	var err error

	if r >= 'A' && r <= 'D' {
		count := short(GetInt(arg, 1))
		info, err := GetConsoleScreenBufferInfo()
		if err != nil {
			return false
		}
		switch r {
		case 'A': // up
			info.dwCursorPosition.y -= count
		case 'B': // down
			info.dwCursorPosition.y += count
		case 'C': // right
			info.dwCursorPosition.x += count
		case 'D': // left
			info.dwCursorPosition.x -= count
		}
		SetConsoleCursorPosition(&info.dwCursorPosition)
		return false
	}

	switch r {
	case 'J':
		killLines()
	case 'K':
		eraseLine()
	case 'm':
		color := word(0)
		for _, item := range arg {
			var c int
			c, err = strconv.Atoi(item)
			if err != nil {
				w.WriteString("[" + strings.Join(arg, ";") + "m")
				break
			}
			if c >= 30 && c < 40 {
				color ^= COLOR_FINTENSITY
				color |= ColorTableFg[c-30]
			} else if c >= 40 && c < 50 {
				color ^= COLOR_BINTENSITY
				color |= ColorTableBg[c-40]
			} else if c == 4 {
				color |= COMMON_LVB_UNDERSCORE | ColorTableFg[7]
			} else { // unknown code treat as reset
				color = ColorTableFg[7]
			}
		}
		if err != nil {
			break
		}
		kernel.SetConsoleTextAttribute(stdout, uintptr(color))
	case '\007': // set title
	case ';':
		if len(arg) == 0 || arg[len(arg)-1] != "" {
			arg = append(arg, "")
			*argptr = arg
		}
		return true
	default:
		if len(arg) == 0 {
			arg = append(arg, "")
		}
		arg[len(arg)-1] += string(r)
		*argptr = arg
		return true
	}
	*argptr = nil
	return false
}

func (a *ANSIWriter) Write(b []byte) (int, error) {
	a.Lock()
	defer a.Unlock()

	off := 0
	for len(b) > off {
		r, size := utf8.DecodeRune(b[off:])
		if size == 0 {
			return off, io.ErrShortWrite
		}
		off += size
		a.ch <- r
	}
	return off, nil
}

func killLines() error {
	sbi, err := GetConsoleScreenBufferInfo()
	if err != nil {
		return err
	}

	size := (sbi.dwCursorPosition.y - sbi.dwSize.y) * sbi.dwSize.x
	size += sbi.dwCursorPosition.x

	var written int
	kernel.FillConsoleOutputAttribute(stdout, uintptr(ColorTableFg[7]),
		uintptr(size),
		sbi.dwCursorPosition.ptr(),
		uintptr(unsafe.Pointer(&written)),
	)
	return kernel.FillConsoleOutputCharacterW(stdout, uintptr(' '),
		uintptr(size),
		sbi.dwCursorPosition.ptr(),
		uintptr(unsafe.Pointer(&written)),
	)
}

func eraseLine() error {
	sbi, err := GetConsoleScreenBufferInfo()
	if err != nil {
		return err
	}

	size := sbi.dwSize.x
	sbi.dwCursorPosition.x = 0
	var written int
	return kernel.FillConsoleOutputCharacterW(stdout, uintptr(' '),
		uintptr(size),
		sbi.dwCursorPosition.ptr(),
		uintptr(unsafe.Pointer(&written)),
	)
}
