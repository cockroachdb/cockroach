// Copyright 2017 Raphael 'kena' Poss
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package libedit_unix

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"

	common "github.com/knz/go-libedit/common"
)

// #cgo openbsd netbsd freebsd dragonfly darwin LDFLAGS: -ledit
// #cgo openbsd netbsd freebsd dragonfly darwin CPPFLAGS: -Ishim
// #cgo linux LDFLAGS: -lncurses
// #cgo linux CFLAGS: -Wno-unused-result -Wno-pointer-sign
// #cgo linux CPPFLAGS: -Isrc -Isrc/c-libedit -Isrc/c-libedit/editline -Isrc/c-libedit/linux-build -D_GNU_SOURCE
//
// #include <stdlib.h>
// #include <stdio.h>
// #include <unistd.h>
// #include <limits.h>
// #include <locale.h>
//
// #include <histedit.h>
// #include "c_editline.h"
import "C"

type EditLine int
type CompletionGenerator = common.CompletionGenerator

type state struct {
	el              *C.EditLine
	sigcfg          unsafe.Pointer
	h               *C.History
	cIn, cOut, cErr *C.FILE
	inf, outf, errf *os.File
	cPromptLeft     *C.char
	cPromptRight    *C.char
	completer       CompletionGenerator
	histFile        *C.char
	autoSaveHistory bool
	wideChars       int
}

var editors []state

var errUnknown = errors.New("unknown error")

func Init(appName string, wideChars bool) (EditLine, error) {
	return InitFiles(appName, wideChars, os.Stdin, os.Stdout, os.Stderr)
}

func InitFiles(appName string, wideChars bool, inf, outf, errf *os.File) (e EditLine, err error) {
	if err := setWideChars(wideChars); err != nil {
		return -1, err
	}
	var inFile, outFile, errFile *C.FILE
	defer func() {
		if err == nil {
			return
		}
		if inFile != nil {
			C.fclose(inFile)
		}
		if outFile != nil {
			C.fclose(outFile)
		}
		if errFile != nil {
			C.fclose(errFile)
		}
	}()

	inFile, err = C.fdopen(C.dup(C.int(inf.Fd())), C.go_libedit_mode_read)
	if err != nil {
		return -1, fmt.Errorf("fdopen(inf): %v", err)
	}
	outFile, err = C.fdopen(C.dup(C.int(outf.Fd())), C.go_libedit_mode_write)
	if err != nil {
		return -1, fmt.Errorf("fdopen(outf): %v", err)
	}
	errFile, err = C.fdopen(C.dup(C.int(errf.Fd())), C.go_libedit_mode_write)
	if err != nil {
		return -1, fmt.Errorf("fdopen(errf): %v", err)
	}
	cAppName := C.CString(appName)
	defer C.free(unsafe.Pointer(cAppName))
	var sigcfg unsafe.Pointer
	id := C.int(len(editors))
	el, err := C.go_libedit_init(id, cAppName, &sigcfg, inFile, outFile, errFile)
	// If the settings file did not exist, ignore the error.
	if err == syscall.ENOENT {
		err = nil
	}
	if el == nil || err != nil {
		if err == nil {
			err = errUnknown
		}
		return -1, fmt.Errorf("el_init: %v", err)
	}

	wc := 0
	if wideChars {
		wc = 1
	}
	st := state{
		el:     el,
		sigcfg: sigcfg,
		inf:    inf, outf: outf, errf: errf,
		cIn: inFile, cOut: outFile, cErr: errFile,
		wideChars: wc,
	}
	editors = append(editors, st)
	return EditLine(len(editors) - 1), nil
}

func setWideChars(set bool) error {
	if set {
		// We need to set the locale to something UTF-8 to enable wide char support.
		l := C.setlocale(C.LC_CTYPE, C.go_libedit_locale1)
		if l == nil {
			l = C.setlocale(C.LC_CTYPE, C.go_libedit_locale2)
		}
		if l == nil {
			return common.ErrWidecharNotSupported
		}
	}
	return nil
}

func (el EditLine) RebindControlKeys() {
	st := &editors[el]
	C.go_libedit_rebind_ctrls(st.el)
}

func (el EditLine) Close() {
	st := &editors[el]
	if st.el == nil {
		// Already closed.
		return
	}
	C.go_libedit_close(st.el, st.sigcfg)
	if st.h != nil {
		C.history_end(st.h)
	}
	if st.cPromptLeft != nil {
		C.free(unsafe.Pointer(st.cPromptLeft))
	}
	if st.cPromptRight != nil {
		C.free(unsafe.Pointer(st.cPromptRight))
	}
	if st.histFile != nil {
		C.free(unsafe.Pointer(st.histFile))
	}
	C.fclose(st.cIn)
	C.fclose(st.cOut)
	C.fclose(st.cErr)
	*st = state{}
}

var errNoHistory = errors.New("history not configured")
var errNoFileConfigured = errors.New("no savefile configured")

func (el EditLine) SaveHistory() error {
	st := &editors[el]
	if st.h == nil {
		return errNoHistory
	}
	if st.histFile == nil {
		return errNoFileConfigured
	}
	_, err := C.go_libedit_write_history(st.h, st.histFile)
	if err != nil {
		return fmt.Errorf("write_history: %v", err)
	}
	return nil
}

func (el EditLine) AddHistory(line string) error {
	st := &editors[el]
	if st.h == nil {
		return errNoHistory
	}

	cLine := C.CString(line)
	defer C.free(unsafe.Pointer(cLine))

	_, err := C.go_libedit_add_history(st.h, cLine)
	if err != nil {
		return fmt.Errorf("add_history: %v", err)
	}

	if st.autoSaveHistory && st.histFile != nil {
		_, err := C.go_libedit_write_history(st.h, st.histFile)
		if err != nil {
			return fmt.Errorf("write_history: %v", err)
		}
	}
	return nil
}

func (el EditLine) LoadHistory(file string) error {
	st := &editors[el]
	if st.h == nil {
		return errNoHistory
	}

	histFile := C.CString(file)
	defer C.free(unsafe.Pointer(histFile))
	_, err := C.go_libedit_read_history(st.h, histFile)
	if err != nil && err != syscall.ENOENT {
		return fmt.Errorf("read_history: %v", err)
	}
	return nil
}

func (el EditLine) SetAutoSaveHistory(file string, autoSave bool) {
	st := &editors[el]
	if st.h == nil {
		return
	}
	var newHistFile *C.char
	if file != "" {
		newHistFile = C.CString(file)
	}
	if st.histFile != nil {
		C.free(unsafe.Pointer(st.histFile))
		st.histFile = nil
	}
	st.histFile = newHistFile
	st.autoSaveHistory = autoSave
}

func (el EditLine) UseHistory(maxEntries int, dedup bool) error {
	st := &editors[el]

	cDedup := 0
	if dedup {
		cDedup = 1
	}

	cMaxEntries := C.int(maxEntries)
	if maxEntries < 0 {
		cMaxEntries = C.INT_MAX
	}

	h, err := C.go_libedit_setup_history(st.el, cMaxEntries, C.int(cDedup))
	if err != nil {
		return fmt.Errorf("init_history: %v", err)
	}
	if st.h != nil {
		C.history_end(st.h)
	}
	st.h = h
	return nil
}

var errUnknownError = errors.New("unknown error")

func (el EditLine) GetLine() (string, error) {
	st := &editors[el]

	var count C.int
	var interrupted C.int
	s, err := C.go_libedit_gets(st.el, st.cPromptLeft, st.cPromptRight,
		st.sigcfg, &count, &interrupted, C.int(st.wideChars))
	if interrupted > 0 {
		// Reveal the partial line.
		line, _ := el.GetLineInfo()
		C.el_reset(st.el)
		return line, common.ErrInterrupted
	}
	if count == -1 {
		if err != nil {
			return "", err
		}
		return "", errUnknownError
	}
	if s == nil {
		return "", io.EOF
	}
	return convertRunes(st.wideChars != 0, s, count), nil
}

func convertRunes(wideChars bool, s unsafe.Pointer, count C.int) string {
	if wideChars {
		var buf [C.MB_LEN_MAX]C.char
		sbuf := make([]byte, 0, int(count))
		for i := 0; i < int(count); i++ {
			wc := *(*C.wchar_t)(unsafe.Pointer(uintptr(unsafe.Pointer(s)) + uintptr(i)*C.sizeof_wchar_t))
			sz := C.wctomb(&buf[0], wc)
			for j := 0; j < int(sz); j++ {
				sbuf = append(sbuf, byte(buf[j]))
			}
		}
		return string(sbuf)
	} else {
		cs := (*C.char)(s)
		return C.GoStringN(cs, count)
	}
}

func (el EditLine) Stdin() *os.File {
	return editors[el].inf
}

func (el EditLine) Stdout() *os.File {
	return editors[el].outf
}

func (el EditLine) Stderr() *os.File {
	return editors[el].errf
}

func (el EditLine) SetCompleter(gen CompletionGenerator) {
	editors[el].completer = gen
}

func (el EditLine) SetLeftPrompt(prompt string) {
	st := &editors[el]
	if st.cPromptLeft != nil {
		C.free(unsafe.Pointer(st.cPromptLeft))
	}
	st.cPromptLeft = C.CString(prompt)
}

func (el EditLine) SetRightPrompt(prompt string) {
	st := &editors[el]
	if st.cPromptRight != nil {
		C.free(unsafe.Pointer(st.cPromptRight))
	}
	st.cPromptRight = C.CString(prompt)
}

func (el EditLine) GetLineInfo() (string, int) {
	st := &editors[el]
	var cursor int
	var count C.int
	var buf unsafe.Pointer
	if st.wideChars != 0 {
		li := C.el_wline(st.el)
		buf = unsafe.Pointer(li.buffer)
		count = C.int(uintptr(unsafe.Pointer(li.lastchar))-uintptr(buf)) / C.sizeof_wchar_t
		cursor = int(uintptr(unsafe.Pointer(li.cursor))-uintptr(buf)) / C.sizeof_wchar_t
	} else {
		li := C.el_line(st.el)
		buf = unsafe.Pointer(li.buffer)
		count = C.int(uintptr(unsafe.Pointer(li.lastchar)) - uintptr(buf))
		cursor = int(uintptr(unsafe.Pointer(li.cursor)) - uintptr(buf))
	}
	return convertRunes(st.wideChars != 0, buf, count), cursor
}

//export go_libedit_getcompletions
func go_libedit_getcompletions(cI C.int, cWord *C.char) **C.char {
	if int(cI) < 0 || int(cI) >= len(editors) {
		return nil
	}
	st := &editors[int(cI)]
	if st.completer == nil {
		return nil
	}

	word := C.GoString(cWord)
	matches := st.completer.GetCompletions(word)
	if len(matches) == 0 {
		return nil
	}

	array := (**C.char)(C.malloc(C.size_t(C.sizeof_pchar * (len(matches) + 1))))
	for i, m := range matches {
		C.go_libedit_set_string_array(array, C.int(i), C.CString(m))
	}
	C.go_libedit_set_string_array(array, C.int(len(matches)), nil)
	return array
}
