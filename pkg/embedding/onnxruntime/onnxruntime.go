// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package onnxruntime is a wrapper around the ONNX Runtime C library for
// running neural network inference. The ONNX Runtime library is dynamically
// loaded at init time. Operations will error if the library was not found.
//
// This follows the same pattern as pkg/geo/geos for GEOS integration.
package onnxruntime

import (
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// #cgo CXXFLAGS: -std=c++14
// #cgo !windows LDFLAGS: -ldl -lm
//
// #include "onnxruntime.h"
import "C"

// EnsureInitErrorDisplay controls the error message displayed by EnsureInit.
type EnsureInitErrorDisplay int

const (
	// EnsureInitErrorDisplayPrivate displays the full error message, including
	// path info. It is intended for log messages.
	EnsureInitErrorDisplayPrivate EnsureInitErrorDisplay = iota
	// EnsureInitErrorDisplayPublic displays a redacted error message, excluding
	// path info. It is intended for errors to display for the client.
	EnsureInitErrorDisplayPublic
)

// onnxOnce contains the global instance of CR_ONNX, to be initialized at most
// once. If it has failed to open, the error will be populated in "err". This
// should only be touched by "fetchONNXOrError".
var onnxOnce struct {
	lib  *C.CR_ONNX
	loc  string
	err  error
	once sync.Once
}

// EnsureInit attempts to initialize the ONNX Runtime if it has not been
// initialized already and returns the location if found, and an error if
// the runtime is not valid.
func EnsureInit(
	errDisplay EnsureInitErrorDisplay, flagLibraryDirectoryValue string,
) (string, error) {
	crdbBinaryLoc := ""
	if len(os.Args) > 0 {
		crdbBinaryLoc = os.Args[0]
	}
	_, err := ensureInit(errDisplay, flagLibraryDirectoryValue, crdbBinaryLoc)
	return onnxOnce.loc, err
}

// ensureInitInternal ensures initialization has been done, always displaying
// errors privately and not assuming a flag has been set if initialized for the
// first time.
func ensureInitInternal() (*C.CR_ONNX, error) {
	return ensureInit(EnsureInitErrorDisplayPrivate, "", "")
}

func ensureInit(
	errDisplay EnsureInitErrorDisplay, flagLibraryDirectoryValue string, crdbBinaryLoc string,
) (*C.CR_ONNX, error) {
	onnxOnce.once.Do(func() {
		onnxOnce.lib, onnxOnce.loc, onnxOnce.err = initONNXRuntime(
			findLibraryDirectories(flagLibraryDirectoryValue, crdbBinaryLoc),
		)
	})
	if onnxOnce.err != nil && errDisplay == EnsureInitErrorDisplayPublic {
		return nil, pgerror.Newf(
			pgcode.System, "onnxruntime: this operation is not available",
		)
	}
	return onnxOnce.lib, onnxOnce.err
}

const libONNXRuntimeFileName = "libonnxruntime"

// getLibraryExt returns the filename with the platform-specific extension.
func getLibraryExt(base string) string {
	switch runtime.GOOS {
	case "darwin":
		return base + ".dylib"
	case "windows":
		return base + ".dll"
	default:
		return base + ".so"
	}
}

// findLibraryDirectories returns the default locations where the ONNX Runtime
// library may be installed.
func findLibraryDirectories(flagLibraryDirectoryValue string, crdbBinaryLoc string) []string {
	locs := []string{}

	// First: check the explicit flag value.
	if flagLibraryDirectoryValue != "" {
		locs = append(locs, flagLibraryDirectoryValue)
	}

	// Second: check the environment variable.
	if envDir := os.Getenv("ONNX_RUNTIME_LIB_DIR"); envDir != "" {
		locs = append(locs, envDir)
	}

	// Third: check Bazel runfile paths.
	if bazel.BuiltWithBazel() {
		pathsToCheck := []string{
			path.Join("c-deps", "libonnxruntime_foreign", "lib"),
			path.Join("external", "archived_cdep_libonnxruntime_linux", "lib"),
			path.Join("external", "archived_cdep_libonnxruntime_linuxarm", "lib"),
			path.Join("external", "archived_cdep_libonnxruntime_macos", "lib"),
			path.Join("external", "archived_cdep_libonnxruntime_macosarm", "lib"),
		}
		for _, p := range pathsToCheck {
			if loc, err := bazel.Runfile(p); err == nil {
				locs = append(locs, loc)
			}
		}
	}

	// Fourth: walk parent directories from binary and CWD looking for
	// lib/libonnxruntime.{so,dylib,dll}.
	locs = append(locs, findLibraryDirectoriesInParentDirs(crdbBinaryLoc)...)

	return locs
}

// findLibraryDirectoriesInParentDirs searches parent directories of the given
// path (and the current working directory) for the ONNX Runtime library.
func findLibraryDirectoriesInParentDirs(binaryLoc string) []string {
	locs := []string{}
	libFileName := getLibraryExt(libONNXRuntimeFileName)

	checkAndAdd := func(dir string) {
		if dir == "" {
			return
		}
		for {
			candidate := filepath.Join(dir, "lib")
			if _, err := os.Stat(filepath.Join(candidate, libFileName)); err == nil {
				locs = append(locs, candidate)
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	// Check binary location.
	if binaryLoc != "" {
		checkAndAdd(filepath.Dir(binaryLoc))
	}

	// Check current working directory.
	if cwd, err := os.Getwd(); err == nil {
		checkAndAdd(cwd)
	}

	return locs
}

// initONNXRuntime attempts to load the ONNX Runtime from each directory.
func initONNXRuntime(dirs []string) (*C.CR_ONNX, string, error) {
	libFileName := getLibraryExt(libONNXRuntimeFileName)
	var err error
	for _, dir := range dirs {
		libPath := filepath.Join(dir, libFileName)
		var ret *C.CR_ONNX
		newErr := statusToError(
			C.CR_ONNX_Init(goToCSlice([]byte(libPath)), &ret),
		)
		if newErr == nil {
			return ret, dir, nil
		}
		err = errors.CombineErrors(
			err,
			errors.Wrapf(
				newErr,
				"onnxruntime: cannot load from dir %q",
				dir,
			),
		)
	}
	if err != nil {
		return nil, "", wrapInitError(
			errors.Wrap(err, "onnxruntime: error during initialization"),
		)
	}
	return nil, "", wrapInitError(
		errors.Newf("onnxruntime: no locations to init ONNX Runtime"),
	)
}

func wrapInitError(err error) error {
	return pgerror.WithCandidateCode(
		errors.WithHintf(
			err,
			"Ensure you have the ONNX Runtime library installed. "+
				"See %s for details.",
			docs.URL("install-cockroachdb-"+installPage()),
		),
		pgcode.ConfigFile,
	)
}

func installPage() string {
	switch runtime.GOOS {
	case "darwin":
		return "mac"
	case "windows":
		return "windows"
	default:
		return "linux"
	}
}

// resetForTesting resets the global singleton state. This is intended for
// tests only and is not safe for concurrent use.
func resetForTesting() {
	if onnxOnce.lib != nil {
		C.CR_ONNX_Close(onnxOnce.lib)
	}
	onnxOnce.lib = nil
	onnxOnce.loc = ""
	onnxOnce.err = nil
	onnxOnce.once = sync.Once{}
}

// ---------------------------------------------------------------------------
// Data marshaling helpers
// ---------------------------------------------------------------------------

func goToCSlice(b []byte) C.CR_ONNX_Slice {
	if len(b) == 0 {
		return C.CR_ONNX_Slice{data: nil, len: 0}
	}
	return C.CR_ONNX_Slice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

func statusToError(s C.CR_ONNX_Status) error {
	if s.data == nil {
		return nil
	}
	msg := C.GoStringN(s.data, C.int(s.len))
	C.CR_ONNX_FreeString(s)
	return errors.Newf("%s", msg)
}

// ---------------------------------------------------------------------------
// Model type
// ---------------------------------------------------------------------------

// Model wraps a loaded ONNX model session.
type Model struct {
	model *C.CR_ONNX_Model
}

// LoadModel loads an ONNX model from the given file path. numThreads controls
// intra-op parallelism (0 = ORT default).
func LoadModel(modelPath string, numThreads int) (*Model, error) {
	lib, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cModel *C.CR_ONNX_Model
	if err := statusToError(
		C.CR_ONNX_LoadModel(
			lib,
			goToCSlice([]byte(modelPath)),
			C.int(numThreads),
			&cModel,
		),
	); err != nil {
		return nil, err
	}
	return &Model{model: cModel}, nil
}

// LoadModelFromBuffer loads an ONNX model from an in-memory buffer.
func LoadModelFromBuffer(data []byte, numThreads int) (*Model, error) {
	lib, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}
	var cModel *C.CR_ONNX_Model
	if err := statusToError(
		C.CR_ONNX_LoadModelFromBuffer(
			lib,
			unsafe.Pointer(&data[0]),
			C.size_t(len(data)),
			C.int(numThreads),
			&cModel,
		),
	); err != nil {
		return nil, err
	}
	return &Model{model: cModel}, nil
}

// Close releases all resources held by the model.
func (m *Model) Close() {
	if m.model != nil {
		C.CR_ONNX_CloseModel(m.model)
		m.model = nil
	}
}

// Dims returns the embedding dimension of the model (last dimension of the
// first output tensor).
func (m *Model) Dims() int {
	return int(C.CR_ONNX_ModelDims(m.model))
}

// NumInputs returns the number of inputs the model expects.
func (m *Model) NumInputs() int {
	return int(C.CR_ONNX_ModelNumInputs(m.model))
}

// NumOutputs returns the number of outputs the model produces.
func (m *Model) NumOutputs() int {
	return int(C.CR_ONNX_ModelNumOutputs(m.model))
}

// RunInference runs the model on pre-tokenized input and returns the raw
// output tensor as a flat float32 slice.
//
// Parameters:
//   - inputIDs:      int64 slice of shape [batchSize * seqLen]
//   - attentionMask: int64 slice of shape [batchSize * seqLen]
//   - batchSize:     number of sequences in the batch
//   - seqLen:        length of each sequence (after padding)
//
// The output shape depends on the model. For sentence-transformer models
// producing token embeddings, the output is [batchSize * seqLen * dims].
// Use PostProcess() to convert to sentence embeddings.
func (m *Model) RunInference(
	inputIDs, attentionMask []int64, batchSize, seqLen int,
) ([]float32, error) {
	lib, err := ensureInitInternal()
	if err != nil {
		return nil, err
	}

	var outputData *C.float
	var outputLen C.size_t

	if err := statusToError(
		C.CR_ONNX_RunInference(
			lib,
			m.model,
			(*C.int64_t)(unsafe.Pointer(&inputIDs[0])),
			(*C.int64_t)(unsafe.Pointer(&attentionMask[0])),
			C.int64_t(batchSize),
			C.int64_t(seqLen),
			&outputData,
			&outputLen,
		),
	); err != nil {
		return nil, err
	}

	// Convert the C float array to a Go slice by copying the data.
	n := int(outputLen)
	result := make([]float32, n)
	cSlice := unsafe.Slice((*float32)(unsafe.Pointer(outputData)), n)
	copy(result, cSlice)

	C.CR_ONNX_FreeOutput(outputData)
	return result, nil
}
