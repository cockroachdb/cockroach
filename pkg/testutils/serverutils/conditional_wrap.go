// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverutils

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

const tipText = `consider replacing the test server initialization from:
    ts, ... := serverutils.StartServer(t, ...)
    defer ts.Stopper().Stop(...)
to:
    srv, ... := serverutils.StartServer(t, ...)
    defer srv.Stopper().Stop(...)
    ts := srv.ApplicationLayer()

See also: https://go.crdb.dev/p/testserver-and-cluster-virtualization
`

// When this env var is set, suspicious API calls are reported in test logs.
var reportCalls = envutil.EnvOrDefaultBool("COCKROACH_TEST_SERVER_SHIM_VERBOSE", false)

func wrapTestServer(
	raw TestServerInterfaceRaw, opts base.DefaultTestTenantOptions,
) TestServerInterface {
	wrapper := &wrap{
		// The default logger function puts the info/warning messages
		// to the standard output. This is a good default when we don't
		// have a *testing.T.
		// If we do, the logger function is overridden in StartServerOnlyE().
		loggerFn: func(format string, args ...interface{}) { fmt.Printf(format, args...) },

		// Used to implement the implicit TestServerController interface,
		// and the explicit .StorageLayer() and .TenantController() accessors.
		ts: raw,

		// Used to implement the explicit .SystemLayer() accessor.
		explicitSysLayer: raw,
	}

	// Explicit implementation of .ApplicationLayer(). Depends on whether
	// a test virtual cluster will be instantiated or not.
	if opts.TestTenantAlwaysEnabled() {
		wrapper.explicitAppLayer = func() ApplicationLayerInterface { return raw.TestTenant() }
	} else {
		wrapper.explicitAppLayer = func() ApplicationLayerInterface { return raw }
	}

	// Implicit implementation of ApplicationLayerInterface.
	if opts.WarnImplicitInterfaces() {
		wrapper.implicitAppLayerNotify = makeBenignNotifyFn(&wrapper.loggerFn, "ApplicationLayerInterface", "ApplicationLayer", true /* showTip */)
	} else {
		wrapper.implicitAppLayerNotify = func(string) {}
	}
	if opts.TestTenantAlwaysEnabled() {
		wrapper.implicitAppLayer = func() ApplicationLayerInterface { return raw.TestTenant() }
	} else {
		wrapper.implicitAppLayer = func() ApplicationLayerInterface { return raw }
	}

	// Implicit implementation of StorageLayerInterface: the methods
	// simply redirect to the underlying testServer, but a benign notice
	// message is printed to teach the user about using .StorageLayer()
	// explicitly.
	if opts.WarnImplicitInterfaces() {
		wrapper.implicitStorageLayerNotify = makeBenignNotifyFn(&wrapper.loggerFn, "StorageLayerInterface", "StorageLayer", false /* showTip */)
	} else {
		wrapper.implicitStorageLayerNotify = func(string) {}
	}
	wrapper.implicitStorageLayer = raw

	// Implicit implementation of TenantControlInterface: the methods
	// simply redirect to the underlying testServer, but a benign notice
	// message is printed to teach the user about using .TenantController()
	// explicitly.
	wrapper.implicitTenantControlNotify = makeBenignNotifyFn(&wrapper.loggerFn, "TenantControlInterface", "TenantController", false /* showTip */)
	wrapper.implicitTenantControl = raw

	return wrapper
}

type wrap struct {
	// loggerFn is used to report notices and warnings about interface use.
	loggerFn func(format string, args ...interface{})

	// ts is used to implement the implicit TestServerController interface,
	// and the explicit .StorageLayer() and .TenantController() accessors.
	ts TestServerInterfaceRaw

	// explicitSysLayer is used to implement the explicit .SystemLayer()
	// accessor.
	explicitSysLayer ApplicationLayerInterface

	// explicitAppLayer is used to implement the explicit
	// .ApplicationLayer() accessor.
	//
	// We use a function here instead of a direct reference because we
	// need to delay the retrieval of the ApplicationLayerInterface
	// until the server has started.
	explicitAppLayer func() ApplicationLayerInterface

	// implicitAppLayerNotify is called whenever a method of the
	// implicit ApplicationLayerInterface is called.
	implicitAppLayerNotify func(methodName string)
	// implicitAppLayer is used to implement the implicit
	// ApplicationLayerInterface.
	implicitAppLayer func() ApplicationLayerInterface

	// implicitStorageLayerNotify is called whenever a method of the
	// implicit StorageLayerInterface is called.
	implicitStorageLayerNotify func(methodName string)
	// implicitStorageLayer is used to implement the implicit
	// StorageLayerInterface.
	implicitStorageLayer StorageLayerInterface

	// implicitTenantControlNotify is called whenever a method of the
	// implicit TenantControlInterface is called.
	implicitTenantControlNotify func(methodName string)
	// implicitTenantControl is used to implement the implicit
	// TenantControlInterface.
	implicitTenantControl TenantControlInterface
}

var _ TestServerInterface = &wrap{}

func (w *wrap) ApplicationLayer() ApplicationLayerInterface          { return w.explicitAppLayer() }
func (w *wrap) SystemLayer() ApplicationLayerInterface               { return w.explicitSysLayer }
func (w *wrap) TenantController() TenantControlInterface             { return w.ts }
func (w *wrap) StorageLayer() StorageLayerInterface                  { return w.ts }
func (w *wrap) fwTestServerController(m string) TestServerController { return w.ts }

// fwApplicationLayerInterface is used by the generated forwarding code
// to access the concrete implementation of ApplicationLayerInterface.
func (w *wrap) fwApplicationLayerInterface(m string) ApplicationLayerInterface {
	w.implicitAppLayerNotify(m)
	return w.implicitAppLayer()
}

// fwStorageLayerInterface is used by the generated forwarding code
// to access the concrete implementation of StorageLayerInterface.
func (w *wrap) fwStorageLayerInterface(m string) StorageLayerInterface {
	w.implicitStorageLayerNotify(m)
	return w.implicitStorageLayer
}

// fwTenantControlInterface is used by the generated forwarding code
// to access the concrete implementation of TenantControlInterface.
func (w *wrap) fwTenantControlInterface(m string) TenantControlInterface {
	w.implicitTenantControlNotify(m)
	return w.implicitTenantControl
}

// TestingSetWrapperLogger is used in tests.
func TestingSetWrapperLogger(w TestServerInterface, logFn func(string, ...interface{})) {
	w.(*wrap).loggerFn = logFn
}

// makeBenignNotifyFn constructs a function that teaches the user
// about appropriate interface use, the first time a method is called
// on the given interface.
//
// The logging function is passed via a pointer because we want to
// keep the ability to override it after the forwarder has been
// instantiated (for example StartServerOnlyE injects the t.Log
// function as a logger.
func makeBenignNotifyFn(
	logFn *func(string, ...interface{}), ifname, accessor string, showTip bool,
) func(methodName string) {
	if !reportCalls {
		return func(_ string) {}
	}
	return func(methodName string) {
		(*logFn)("\n%s\n\tNOTICE: .%s() called via implicit interface %s;\nHINT: consider using .%s().%s() instead.\n",
			GetExternalCaller(),
			methodName, ifname, accessor, methodName)
		if showTip {
			(*logFn)("TIP: %s", tipText)
		}
	}
}

// GetExternalCaller returns the file:line of the first function in
// the call stack outside of this package. It is used as prefix for
// informational messages about potential interface misuse.
func GetExternalCaller() string {
	const omitPrefix = "github.com/cockroachdb/cockroach/"
	var pcs [15]uintptr
	runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:])
	var frame runtime.Frame
	found := false
	for {
		f, more := frames.Next()
		if !strings.Contains(f.Function, "pkg/testutils/serverutils.") && !strings.HasPrefix(f.Function, "sync.") {
			frame = f
			found = true
			break
		}
		if !more {
			break
		}
	}
	if !found {
		return "<unknown>"
	}
	idx := strings.LastIndexByte(frame.Function, '/')
	if idx >= 0 {
		rest := frame.Function[idx+1:]
		idx = strings.IndexByte(rest, '.')
		if idx >= 0 {
			frame.Function = rest[idx+1:]
		}
	}
	return fmt.Sprintf("%s:%d: (%s)",
		strings.TrimPrefix(frame.File, omitPrefix), frame.Line, frame.Function)
}
