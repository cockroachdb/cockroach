// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package embedding

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/embedding/onnxruntime"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// globalEngine holds the singleton Engine instance, initialized once at
// server startup via Init. GetEngine returns this instance or a
// user-facing error if initialization failed or was not attempted.
var globalEngine struct {
	engine *Engine
	err    error
	once   sync.Once
}

// Init initializes the ONNX Runtime and loads the embedding model and
// vocabulary. It is called once during server startup. Initialization
// errors are recorded but do not prevent the server from starting; the
// embed() SQL builtin will return a user-facing error instead.
func Init(onnxLibDir, modelPath, vocabPath string) error {
	var initErr error
	globalEngine.once.Do(func() {
		if modelPath == "" || vocabPath == "" {
			globalEngine.err = errors.New(
				"embedding model not available; auto-download may have failed " +
					"or ONNX Runtime library is not installed",
			)
			initErr = globalEngine.err
			return
		}

		_, err := onnxruntime.EnsureInit(
			onnxruntime.EnsureInitErrorDisplayPrivate, onnxLibDir,
		)
		if err != nil {
			globalEngine.err = errors.Wrap(err, "initializing ONNX Runtime")
			initErr = globalEngine.err
			return
		}

		eng, err := NewEngine(modelPath, vocabPath)
		if err != nil {
			globalEngine.err = errors.Wrap(err, "loading embedding model")
			initErr = globalEngine.err
			return
		}

		globalEngine.engine = eng
	})
	return initErr
}

// GetEngine returns the global Engine singleton. If the engine was not
// initialized (or initialization failed), it returns a pgerror suitable
// for display to SQL clients.
func GetEngine() (*Engine, error) {
	if globalEngine.engine != nil {
		return globalEngine.engine, nil
	}
	if globalEngine.err != nil {
		return nil, pgerror.WithCandidateCode(
			errors.WithHint(
				errors.Wrap(globalEngine.err, "embedding engine is not available"),
				"Ensure the ONNX Runtime library is installed (--embedding-libs). "+
					"The model is downloaded automatically on first use, or set "+
					"--embedding-model and --embedding-vocab explicitly.",
			),
			pgcode.ConfigFile,
		)
	}
	return nil, pgerror.WithCandidateCode(
		errors.WithHint(
			errors.New("embedding engine is not initialized"),
			"Ensure the ONNX Runtime library is installed (--embedding-libs). "+
				"The model is downloaded automatically on first use, or set "+
				"--embedding-model and --embedding-vocab explicitly.",
		),
		pgcode.ConfigFile,
	)
}

// resetForTesting resets the global engine state for test isolation.
func resetForTesting() {
	if globalEngine.engine != nil {
		globalEngine.engine.Close()
	}
	globalEngine.engine = nil
	globalEngine.err = nil
	globalEngine.once = sync.Once{}
}
