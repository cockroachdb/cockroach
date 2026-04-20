// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// onnxruntime.h defines the C ABI boundary between Go (via CGo) and C++ for
// dynamically loading and calling into the ONNX Runtime library. This follows
// the same pattern as pkg/geo/geos/geos.h.
//
// ONNX Runtime is loaded at runtime via dlopen/dlsym. We define only the
// minimal ORT types we need here rather than vendoring the full
// onnxruntime_c_api.h header (~4000 lines).

#include <stdlib.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// ---------------------------------------------------------------------------
// Data types (matching GEOS conventions)
// ---------------------------------------------------------------------------

// CR_ONNX_Slice contains data that does not need to be freed. It can be either
// a Go or C pointer (which indicates who allocated the memory).
typedef struct {
  char* data;
  size_t len;
} CR_ONNX_Slice;

// CR_ONNX_String contains a C pointer that needs to be freed by the caller.
typedef struct {
  char* data;
  size_t len;
} CR_ONNX_String;

// CR_ONNX_Status represents the result of an operation. If data is NULL, the
// operation succeeded. Otherwise, data contains an error message that must be
// freed by the caller.
typedef CR_ONNX_String CR_ONNX_Status;

// ---------------------------------------------------------------------------
// Opaque handles
// ---------------------------------------------------------------------------

// CR_ONNX holds the dynamically loaded ONNX Runtime library and environment.
// Defined in onnxruntime.cc; opaque to Go.
typedef struct CR_ONNX CR_ONNX;

// CR_ONNX_Model holds a loaded ONNX model session ready for inference.
// Defined in onnxruntime.cc; opaque to Go.
typedef struct CR_ONNX_Model CR_ONNX_Model;

// ---------------------------------------------------------------------------
// Lifecycle: Init / Close
// ---------------------------------------------------------------------------

// CR_ONNX_Init initializes the ONNX Runtime by dynamically loading the shared
// library from the given path. The libPath slice must be convertible to a NUL
// character terminated C string. On success, *lib is populated and the returned
// status has data==NULL. On failure, the status contains an error message.
CR_ONNX_Status CR_ONNX_Init(CR_ONNX_Slice libPath, CR_ONNX** lib);

// CR_ONNX_Close releases all resources held by the ONNX Runtime instance,
// including the dlopen handle and the ORT environment.
void CR_ONNX_Close(CR_ONNX* lib);

// ---------------------------------------------------------------------------
// Model lifecycle
// ---------------------------------------------------------------------------

// CR_ONNX_LoadModel loads an ONNX model from the given file path. The
// modelPath slice must be convertible to a NUL terminated C string.
// numThreads controls intra-op parallelism (0 = ORT default).
CR_ONNX_Status CR_ONNX_LoadModel(
    CR_ONNX* lib, CR_ONNX_Slice modelPath, int numThreads, CR_ONNX_Model** model);

// CR_ONNX_LoadModelFromBuffer loads an ONNX model from an in-memory buffer.
// numThreads controls intra-op parallelism (0 = ORT default).
CR_ONNX_Status CR_ONNX_LoadModelFromBuffer(
    CR_ONNX* lib, const void* modelData, size_t modelDataLen,
    int numThreads, CR_ONNX_Model** model);

// CR_ONNX_CloseModel releases all resources held by a loaded model.
void CR_ONNX_CloseModel(CR_ONNX_Model* model);

// CR_ONNX_ModelDims returns the embedding dimension of the model's first
// output tensor's last dimension.
int CR_ONNX_ModelDims(CR_ONNX_Model* model);

// CR_ONNX_ModelNumInputs returns the number of inputs the model expects.
int CR_ONNX_ModelNumInputs(CR_ONNX_Model* model);

// CR_ONNX_ModelNumOutputs returns the number of outputs the model produces.
int CR_ONNX_ModelNumOutputs(CR_ONNX_Model* model);

// ---------------------------------------------------------------------------
// Inference
// ---------------------------------------------------------------------------

// CR_ONNX_RunInference runs the model on the given tokenized input.
//
// Inputs:
//   - inputIDs:      int64 array of shape [batchSize, seqLen]
//   - attentionMask: int64 array of shape [batchSize, seqLen]
//   - batchSize:     number of sequences in the batch
//   - seqLen:        length of each sequence (after padding)
//
// Outputs:
//   - outputData:    pointer to a float array allocated by this function.
//                    The caller must free it with CR_ONNX_FreeOutput().
//   - outputLen:     number of floats in outputData.
//
// The output shape depends on the model. For sentence-transformer models,
// the output is typically [batchSize, seqLen, dims] (token embeddings) or
// [batchSize, dims] (sentence embeddings).
CR_ONNX_Status CR_ONNX_RunInference(
    CR_ONNX* lib, CR_ONNX_Model* model,
    const int64_t* inputIDs, const int64_t* attentionMask,
    int64_t batchSize, int64_t seqLen,
    float** outputData, size_t* outputLen);

// ---------------------------------------------------------------------------
// Memory management
// ---------------------------------------------------------------------------

// CR_ONNX_FreeOutput frees output data returned by CR_ONNX_RunInference.
void CR_ONNX_FreeOutput(float* outputData);

// CR_ONNX_FreeString frees a CR_ONNX_String (or CR_ONNX_Status).
void CR_ONNX_FreeString(CR_ONNX_String str);

#ifdef __cplusplus
}  // extern "C"
#endif
