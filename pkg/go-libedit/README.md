# go-libedit 

Go wrapper around `libedit`, a replacement to GNU readline using the BSD license.

[![travis](https://travis-ci.org/knz/go-libedit.svg?branch=master)](https://travis-ci.org/knz/go-libedit)

How to use:

- `go build` / `go install`
- see `test/example.go` for a demo.
- basic idea: call `Init()` once. Then call `SetLeftPrompt()` and
  `GetLine()` as needed. Finally call `Close()`.

## How to force using the system-wide libedit on GNU/Linux

By default, the **go-libedit** package uses the bundled `libedit`
sources on GNU/Linux, so that `go get` works out of the box.

To force the package to use a system-wide libedit instead, edit `unix/editline_unix.go` as follows:

- remove the line containing `#cgo linux CFLAGS`
- change the line containing `#cgo linux CPPFLAGS` to read: `#cgo linux CPPFLAGS: -I/usr/include/editline -Ishim`
- change the line containing `#cgo linux LDFLAGS` to read: `#cgo linux LDFLAGS: -ledit`

## macOS/OSX due to restrictions due to changes macOS Mojave

macOS Mojave is shipped with a broken/restricted libedit where the
completion API is not published. Because it is not possible to
automatically detect macOS versions, **go-libedit** will use a reduced
autocompletion facility on all versions of macOS instead.

This autocompletion facility lacks the following features from libedit:

- it cannot autocomplete file and directory names.
- it does not display a list of possible completions. Only the fact
  that more than 1 completion is available is signalled using
  a terminal beep.

## How to refresh the bundled libedit sources

(Only needed when upgrading the bundled `libedit` to a newer version.)

This procedure should be ran on a Debian/Ubuntu system.

1. ensure that `/etc/apt/sources.list` contains source repositories, i.e. the `deb-src`  lines are uncommented. Run `apt-get update` as necessary.
2. run:

   ```
   $ sudo apt-get install libbsd libbsd-dev libncurses-dev`
   $ cd src
   $ bash refresh.sh
   ```
