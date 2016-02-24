// +build go1.4

package gotool

import (
	"path/filepath"
	"runtime"
)

var gorootSrcPkg = filepath.Join(runtime.GOROOT(), "src")
