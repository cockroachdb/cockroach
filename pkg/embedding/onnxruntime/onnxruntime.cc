// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// onnxruntime.cc implements dynamic loading of the ONNX Runtime shared
// library via dlopen/dlsym. This follows the same pattern as
// pkg/geo/geos/geos.cc but is simpler because ONNX Runtime's C API exposes
// a single entry point (OrtGetApiBase) that returns a struct of function
// pointers — no need to dlsym individual functions.

#include <cstring>
#if _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif  // _WIN32
#include <memory>
#include <stdlib.h>
#include <string>
#include <vector>

#include "onnxruntime.h"

#if _WIN32
#define dlopen(x, y) LoadLibrary(x)
#define dlsym GetProcAddress
#define dlclose FreeLibrary
#define dlerror() ((char*)"failed to execute dlsym")
typedef HMODULE dlhandle;
#else
typedef void* dlhandle;
#endif  // _WIN32

// ---------------------------------------------------------------------------
// Minimal ONNX Runtime C API type definitions.
//
// We define only what we need rather than vendoring the full
// onnxruntime_c_api.h (~4000 lines). These must match the ORT ABI.
// ---------------------------------------------------------------------------

namespace {

// ORT API version we target. This corresponds to ONNX Runtime 1.21.x.
// The API is backwards-compatible: newer runtimes support older versions.
constexpr uint32_t kOrtApiVersion = 21;

// Opaque ORT types -- we only ever hold pointers to these.
struct OrtEnv;
struct OrtSession;
struct OrtSessionOptions;
struct OrtRunOptions;
struct OrtValue;
struct OrtMemoryInfo;
struct OrtStatus;
struct OrtAllocator;
struct OrtTensorTypeAndShapeInfo;
struct OrtTypeInfo;

// Logging levels.
enum OrtLoggingLevel {
  ORT_LOGGING_LEVEL_VERBOSE = 0,
  ORT_LOGGING_LEVEL_INFO = 1,
  ORT_LOGGING_LEVEL_WARNING = 2,
  ORT_LOGGING_LEVEL_ERROR = 3,
  ORT_LOGGING_LEVEL_FATAL = 4,
};

// Tensor element data types we use.
enum ONNXTensorElementDataType {
  ONNX_TENSOR_ELEMENT_DATA_TYPE_UNDEFINED = 0,
  ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT = 1,
  ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64 = 7,
};

// Allocator types.
enum OrtAllocatorType {
  OrtInvalidAllocator = -1,
  OrtDeviceAllocator = 0,
  OrtArenaAllocator = 1,
};

// Memory types.
enum OrtMemType {
  OrtMemTypeCPUInput = -2,
  OrtMemTypeCPUOutput = -1,
  OrtMemTypeDefault = 0,
};

// The OrtApi struct: a table of function pointers. We only declare the
// fields we actually use. The struct is laid out at specific offsets
// defined by the ORT ABI. Rather than declaring hundreds of unused
// fields, we access the function pointers through a helper that
// computes the offset from the base of the struct.
//
// IMPORTANT: The field offsets below are for ORT API version 21.
// They were determined from the official onnxruntime_c_api.h header.
// If we target a different version, these offsets must be updated.
//
// Instead of using raw offsets, we define the OrtApi as an opaque type
// and use a small subset of functions via a wrapper approach.

// The OrtApiBase struct is the entry point. It has exactly 2 function
// pointers, which is stable across ORT versions.
struct OrtApiBase {
  const void* (*GetApi)(uint32_t version);
  const char* (*GetVersionString)(void);
};

// OrtApi function pointer typedefs for the functions we need.
// These match the signatures in onnxruntime_c_api.h.
typedef OrtStatus* (*CreateEnvFn)(OrtLoggingLevel, const char*, OrtEnv**);
typedef OrtStatus* (*CreateSessionOptionsFn)(OrtSessionOptions**);
typedef OrtStatus* (*SetIntraOpNumThreadsFn)(OrtSessionOptions*, int);
typedef OrtStatus* (*SetInterOpNumThreadsFn)(OrtSessionOptions*, int);
typedef OrtStatus* (*CreateSessionFn)(const OrtEnv*, const char*,
                                      const OrtSessionOptions*, OrtSession**);
typedef OrtStatus* (*CreateSessionFromArrayFn)(const OrtEnv*, const void*, size_t,
                                               const OrtSessionOptions*, OrtSession**);
typedef OrtStatus* (*RunFn)(OrtSession*, const OrtRunOptions*,
                            const char* const*, const OrtValue* const*, size_t,
                            const char* const*, size_t, OrtValue**);
typedef OrtStatus* (*CreateCpuMemoryInfoFn)(OrtAllocatorType, OrtMemType, OrtMemoryInfo**);
typedef OrtStatus* (*CreateTensorWithDataAsOrtValueFn)(
    const OrtMemoryInfo*, void*, size_t, const int64_t*, size_t,
    ONNXTensorElementDataType, OrtValue**);
typedef OrtStatus* (*GetTensorMutableDataFn)(OrtValue*, void**);
typedef OrtStatus* (*SessionGetInputCountFn)(const OrtSession*, size_t*);
typedef OrtStatus* (*SessionGetOutputCountFn)(const OrtSession*, size_t*);
typedef OrtStatus* (*SessionGetInputNameFn)(const OrtSession*, size_t, OrtAllocator*, char**);
typedef OrtStatus* (*SessionGetOutputNameFn)(const OrtSession*, size_t, OrtAllocator*, char**);
typedef OrtStatus* (*SessionGetOutputTypeInfoFn)(const OrtSession*, size_t, OrtTypeInfo**);
typedef OrtStatus* (*CastTypeInfoToTensorInfoFn)(const OrtTypeInfo*, const OrtTensorTypeAndShapeInfo**);
typedef OrtStatus* (*GetDimensionsCountFn)(const OrtTensorTypeAndShapeInfo*, size_t*);
typedef OrtStatus* (*GetDimensionsFn)(const OrtTensorTypeAndShapeInfo*, int64_t*, size_t);
typedef OrtStatus* (*GetTensorShapeElementCountFn)(const OrtTensorTypeAndShapeInfo*, size_t*);
typedef void (*ReleaseEnvFn)(OrtEnv*);
typedef void (*ReleaseSessionFn)(OrtSession*);
typedef void (*ReleaseSessionOptionsFn)(OrtSessionOptions*);
typedef void (*ReleaseMemoryInfoFn)(OrtMemoryInfo*);
typedef void (*ReleaseValueFn)(OrtValue*);
typedef void (*ReleaseTypeInfoFn)(OrtTypeInfo*);
typedef const char* (*GetErrorMessageFn)(const OrtStatus*);
typedef void (*ReleaseStatusFn)(OrtStatus*);
typedef OrtStatus* (*GetAllocatorWithDefaultOptionsFn)(OrtAllocator**);
typedef OrtStatus* (*AllocatorFreeFn)(OrtAllocator*, void*);

// OrtApiFunctions holds the resolved function pointers we need.
struct OrtApiFunctions {
  CreateEnvFn CreateEnv;
  CreateSessionOptionsFn CreateSessionOptions;
  SetIntraOpNumThreadsFn SetIntraOpNumThreads;
  SetInterOpNumThreadsFn SetInterOpNumThreads;
  CreateSessionFn CreateSession;
  CreateSessionFromArrayFn CreateSessionFromArray;
  RunFn Run;
  CreateCpuMemoryInfoFn CreateCpuMemoryInfo;
  CreateTensorWithDataAsOrtValueFn CreateTensorWithDataAsOrtValue;
  GetTensorMutableDataFn GetTensorMutableData;
  SessionGetInputCountFn SessionGetInputCount;
  SessionGetOutputCountFn SessionGetOutputCount;
  SessionGetInputNameFn SessionGetInputName;
  SessionGetOutputNameFn SessionGetOutputName;
  SessionGetOutputTypeInfoFn SessionGetOutputTypeInfo;
  CastTypeInfoToTensorInfoFn CastTypeInfoToTensorInfo;
  GetDimensionsCountFn GetDimensionsCount;
  GetDimensionsFn GetDimensions;
  GetTensorShapeElementCountFn GetTensorShapeElementCount;
  ReleaseEnvFn ReleaseEnv;
  ReleaseSessionFn ReleaseSession;
  ReleaseSessionOptionsFn ReleaseSessionOptions;
  ReleaseMemoryInfoFn ReleaseMemoryInfo;
  ReleaseValueFn ReleaseValue;
  ReleaseTypeInfoFn ReleaseTypeInfo;
  GetErrorMessageFn GetErrorMessage;
  ReleaseStatusFn ReleaseStatus;
  GetAllocatorWithDefaultOptionsFn GetAllocatorWithDefaultOptions;
  AllocatorFreeFn AllocatorFree;
};

// Helper to build a CR_ONNX_Status from an error string. If the string is
// empty, returns a success status (data=NULL).
CR_ONNX_Status makeStatus(const std::string& err) {
  CR_ONNX_Status status;
  if (err.empty()) {
    status.data = NULL;
    status.len = 0;
  } else {
    status.data = static_cast<char*>(malloc(err.size()));
    memcpy(status.data, err.data(), err.size());
    status.len = err.size();
  }
  return status;
}

CR_ONNX_Status successStatus() {
  CR_ONNX_Status status;
  status.data = NULL;
  status.len = 0;
  return status;
}

}  // namespace

// ---------------------------------------------------------------------------
// CR_ONNX: the library handle holding dlopen handle + ORT API.
// ---------------------------------------------------------------------------

struct CR_ONNX {
  dlhandle handle;
  OrtApiFunctions api;
  OrtEnv* env;

  CR_ONNX(dlhandle h) : handle(h), api{}, env(nullptr) {}

  ~CR_ONNX() {
    if (env != nullptr && api.ReleaseEnv != nullptr) {
      api.ReleaseEnv(env);
    }
    if (handle != NULL) {
      dlclose(handle);
    }
  }

  // checkStatus converts an OrtStatus* to a C++ error string and releases
  // the status. Returns empty string on success.
  std::string checkStatus(OrtStatus* status) {
    if (status == nullptr) {
      return "";
    }
    std::string msg;
    if (api.GetErrorMessage != nullptr) {
      msg = api.GetErrorMessage(status);
    } else {
      msg = "onnxruntime: unknown error";
    }
    if (api.ReleaseStatus != nullptr) {
      api.ReleaseStatus(status);
    }
    return msg;
  }
};

// ---------------------------------------------------------------------------
// CR_ONNX_Model: a loaded model session ready for inference.
// ---------------------------------------------------------------------------

struct CR_ONNX_Model {
  CR_ONNX* lib;
  OrtSession* session;
  OrtSessionOptions* options;
  OrtMemoryInfo* memoryInfo;

  size_t numInputs;
  size_t numOutputs;
  char** inputNames;
  char** outputNames;

  // dims is the last dimension of the first output tensor, which for
  // sentence-transformer models is the embedding dimension.
  int dims;

  CR_ONNX_Model(CR_ONNX* l)
      : lib(l), session(nullptr), options(nullptr), memoryInfo(nullptr),
        numInputs(0), numOutputs(0), inputNames(nullptr), outputNames(nullptr),
        dims(0) {}

  ~CR_ONNX_Model() {
    OrtApiFunctions& api = lib->api;
    OrtAllocator* allocator = nullptr;
    api.GetAllocatorWithDefaultOptions(&allocator);

    if (inputNames != nullptr) {
      for (size_t i = 0; i < numInputs; i++) {
        if (inputNames[i] != nullptr && allocator != nullptr) {
          api.AllocatorFree(allocator, inputNames[i]);
        }
      }
      free(inputNames);
    }
    if (outputNames != nullptr) {
      for (size_t i = 0; i < numOutputs; i++) {
        if (outputNames[i] != nullptr && allocator != nullptr) {
          api.AllocatorFree(allocator, outputNames[i]);
        }
      }
      free(outputNames);
    }
    if (memoryInfo != nullptr) {
      api.ReleaseMemoryInfo(memoryInfo);
    }
    if (options != nullptr) {
      api.ReleaseSessionOptions(options);
    }
    if (session != nullptr) {
      api.ReleaseSession(session);
    }
  }
};

// ---------------------------------------------------------------------------
// resolveApi extracts function pointers from the opaque OrtApi struct.
//
// The OrtApi struct is a contiguous array of function pointers. The official
// header defines them in a specific order. We resolve them by casting the
// OrtApi pointer to an array of void* and indexing into it.
//
// The offsets below correspond to ORT API version 21 (ONNX Runtime 1.21.x).
// They were determined by counting the field positions in the official
// onnxruntime_c_api.h OrtApi struct definition.
// ---------------------------------------------------------------------------

namespace {

bool resolveApi(const void* ortApi, OrtApiFunctions* out, std::string* errOut) {
  if (ortApi == nullptr) {
    *errOut = "onnxruntime: GetApi returned null";
    return false;
  }
  // The OrtApi is a flat array of function pointers. We cast and index.
  const void* const* fns = reinterpret_cast<const void* const*>(ortApi);

  // Offsets from the OrtApi struct in onnxruntime_c_api.h (API v21).
  // Each offset is the 0-based index of the function pointer in the struct.
  // These were verified by parsing the official header with the Release*
  // macros expanded to their ORT_CLASS_RELEASE entries.
  out->CreateEnv = reinterpret_cast<CreateEnvFn>(const_cast<void*>(fns[3]));
  out->CreateSessionOptions = reinterpret_cast<CreateSessionOptionsFn>(const_cast<void*>(fns[10]));
  out->SetIntraOpNumThreads = reinterpret_cast<SetIntraOpNumThreadsFn>(const_cast<void*>(fns[24]));
  out->SetInterOpNumThreads = reinterpret_cast<SetInterOpNumThreadsFn>(const_cast<void*>(fns[25]));
  out->CreateSession = reinterpret_cast<CreateSessionFn>(const_cast<void*>(fns[7]));
  out->CreateSessionFromArray = reinterpret_cast<CreateSessionFromArrayFn>(const_cast<void*>(fns[8]));
  out->Run = reinterpret_cast<RunFn>(const_cast<void*>(fns[9]));
  out->CreateTensorWithDataAsOrtValue = reinterpret_cast<CreateTensorWithDataAsOrtValueFn>(const_cast<void*>(fns[49]));
  out->GetTensorMutableData = reinterpret_cast<GetTensorMutableDataFn>(const_cast<void*>(fns[51]));
  out->CreateCpuMemoryInfo = reinterpret_cast<CreateCpuMemoryInfoFn>(const_cast<void*>(fns[69]));
  out->SessionGetInputCount = reinterpret_cast<SessionGetInputCountFn>(const_cast<void*>(fns[30]));
  out->SessionGetOutputCount = reinterpret_cast<SessionGetOutputCountFn>(const_cast<void*>(fns[31]));
  out->SessionGetInputName = reinterpret_cast<SessionGetInputNameFn>(const_cast<void*>(fns[36]));
  out->SessionGetOutputName = reinterpret_cast<SessionGetOutputNameFn>(const_cast<void*>(fns[37]));
  out->ReleaseEnv = reinterpret_cast<ReleaseEnvFn>(const_cast<void*>(fns[92]));
  out->ReleaseStatus = reinterpret_cast<ReleaseStatusFn>(const_cast<void*>(fns[93]));
  out->ReleaseMemoryInfo = reinterpret_cast<ReleaseMemoryInfoFn>(const_cast<void*>(fns[94]));
  out->ReleaseSession = reinterpret_cast<ReleaseSessionFn>(const_cast<void*>(fns[95]));
  out->ReleaseValue = reinterpret_cast<ReleaseValueFn>(const_cast<void*>(fns[96]));
  out->ReleaseSessionOptions = reinterpret_cast<ReleaseSessionOptionsFn>(const_cast<void*>(fns[100]));
  out->GetErrorMessage = reinterpret_cast<GetErrorMessageFn>(const_cast<void*>(fns[2]));
  out->GetAllocatorWithDefaultOptions = reinterpret_cast<GetAllocatorWithDefaultOptionsFn>(const_cast<void*>(fns[78]));
  out->AllocatorFree = reinterpret_cast<AllocatorFreeFn>(const_cast<void*>(fns[76]));
  out->SessionGetOutputTypeInfo = reinterpret_cast<SessionGetOutputTypeInfoFn>(const_cast<void*>(fns[34]));
  out->CastTypeInfoToTensorInfo = reinterpret_cast<CastTypeInfoToTensorInfoFn>(const_cast<void*>(fns[55]));
  out->GetDimensionsCount = reinterpret_cast<GetDimensionsCountFn>(const_cast<void*>(fns[61]));
  out->GetDimensions = reinterpret_cast<GetDimensionsFn>(const_cast<void*>(fns[62]));
  out->GetTensorShapeElementCount = reinterpret_cast<GetTensorShapeElementCountFn>(const_cast<void*>(fns[64]));
  out->ReleaseTypeInfo = reinterpret_cast<ReleaseTypeInfoFn>(const_cast<void*>(fns[98]));

  // Sanity check: verify that critical function pointers are non-null.
  if (out->CreateEnv == nullptr || out->CreateSession == nullptr ||
      out->Run == nullptr || out->GetErrorMessage == nullptr) {
    *errOut = "onnxruntime: failed to resolve critical API functions";
    return false;
  }

  return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// Public API implementation
// ---------------------------------------------------------------------------

CR_ONNX_Status CR_ONNX_Init(CR_ONNX_Slice libPath, CR_ONNX** lib) {
  *lib = nullptr;

  // Convert the slice to a NUL-terminated C string.
  std::string libPathStr(libPath.data, libPath.len);

  // Open the shared library.
  dlhandle handle = dlopen(libPathStr.c_str(), RTLD_LAZY);
  if (handle == NULL) {
    std::string err = "onnxruntime: could not load library \"";
    err += libPathStr;
    err += "\": ";
    err += dlerror();
    return makeStatus(err);
  }

  // Resolve the single entry point.
  typedef const OrtApiBase* (*GetApiBaseFn)(void);
  auto getApiBase = reinterpret_cast<GetApiBaseFn>(dlsym(handle, "OrtGetApiBase"));
  if (getApiBase == nullptr) {
    std::string err = "onnxruntime: could not find OrtGetApiBase symbol: ";
    err += dlerror();
    dlclose(handle);
    return makeStatus(err);
  }

  const OrtApiBase* apiBase = getApiBase();
  if (apiBase == nullptr) {
    dlclose(handle);
    return makeStatus("onnxruntime: OrtGetApiBase returned null");
  }

  const void* ortApi = apiBase->GetApi(kOrtApiVersion);
  if (ortApi == nullptr) {
    // Try to provide a useful error message.
    std::string err = "onnxruntime: GetApi(";
    err += std::to_string(kOrtApiVersion);
    err += ") returned null (library version: ";
    const char* ver = apiBase->GetVersionString();
    err += (ver != nullptr) ? ver : "unknown";
    err += "). The installed ONNX Runtime may be too old.";
    dlclose(handle);
    return makeStatus(err);
  }

  // Create the library wrapper.
  std::unique_ptr<CR_ONNX> ret(new CR_ONNX(handle));

  // Resolve the function pointers we need.
  std::string resolveErr;
  if (!resolveApi(ortApi, &ret->api, &resolveErr)) {
    return makeStatus(resolveErr);
  }

  // Create the ORT environment.
  OrtStatus* status = ret->api.CreateEnv(
      ORT_LOGGING_LEVEL_WARNING, "cockroachdb", &ret->env);
  std::string envErr = ret->checkStatus(status);
  if (!envErr.empty()) {
    return makeStatus("onnxruntime: failed to create environment: " + envErr);
  }

  *lib = ret.release();
  return successStatus();
}

void CR_ONNX_Close(CR_ONNX* lib) {
  delete lib;
}

CR_ONNX_Status CR_ONNX_LoadModel(
    CR_ONNX* lib, CR_ONNX_Slice modelPath, int numThreads, CR_ONNX_Model** model) {
  *model = nullptr;
  OrtApiFunctions& api = lib->api;

  std::string modelPathStr(modelPath.data, modelPath.len);
  std::unique_ptr<CR_ONNX_Model> m(new CR_ONNX_Model(lib));
  std::string err;

  // Create session options.
  OrtStatus* status = api.CreateSessionOptions(&m->options);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: session options: " + err);
  }

  if (numThreads > 0) {
    status = api.SetIntraOpNumThreads(m->options, numThreads);
    err = lib->checkStatus(status);
    if (!err.empty()) {
      return makeStatus("onnxruntime: set threads: " + err);
    }
  }

  // Create session (load model).
  status = api.CreateSession(lib->env, modelPathStr.c_str(), m->options, &m->session);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: load model: " + err);
  }

  // Create CPU memory info for tensor allocation.
  status = api.CreateCpuMemoryInfo(OrtDeviceAllocator, OrtMemTypeDefault, &m->memoryInfo);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: memory info: " + err);
  }

  // Get default allocator for name strings.
  OrtAllocator* allocator = nullptr;
  status = api.GetAllocatorWithDefaultOptions(&allocator);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: allocator: " + err);
  }

  // Introspect input names.
  status = api.SessionGetInputCount(m->session, &m->numInputs);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: input count: " + err);
  }

  m->inputNames = static_cast<char**>(calloc(m->numInputs, sizeof(char*)));
  for (size_t i = 0; i < m->numInputs; i++) {
    status = api.SessionGetInputName(m->session, i, allocator, &m->inputNames[i]);
    err = lib->checkStatus(status);
    if (!err.empty()) {
      return makeStatus("onnxruntime: input name: " + err);
    }
  }

  // Introspect output names and dimensions.
  status = api.SessionGetOutputCount(m->session, &m->numOutputs);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: output count: " + err);
  }

  m->outputNames = static_cast<char**>(calloc(m->numOutputs, sizeof(char*)));
  for (size_t i = 0; i < m->numOutputs; i++) {
    status = api.SessionGetOutputName(m->session, i, allocator, &m->outputNames[i]);
    err = lib->checkStatus(status);
    if (!err.empty()) {
      return makeStatus("onnxruntime: output name: " + err);
    }
  }

  // Determine embedding dimensions from the first output's shape.
  if (m->numOutputs > 0) {
    OrtTypeInfo* typeInfo = nullptr;
    status = api.SessionGetOutputTypeInfo(m->session, 0, &typeInfo);
    err = lib->checkStatus(status);
    if (!err.empty()) {
      return makeStatus("onnxruntime: output type info: " + err);
    }

    const OrtTensorTypeAndShapeInfo* tensorInfo = nullptr;
    status = api.CastTypeInfoToTensorInfo(typeInfo, &tensorInfo);
    err = lib->checkStatus(status);
    if (err.empty() && tensorInfo != nullptr) {
      size_t numDims = 0;
      api.GetDimensionsCount(tensorInfo, &numDims);
      if (numDims > 0) {
        std::vector<int64_t> shape(numDims);
        api.GetDimensions(tensorInfo, shape.data(), numDims);
        // The last dimension is typically the embedding dimension.
        m->dims = static_cast<int>(shape[numDims - 1]);
      }
    }
    api.ReleaseTypeInfo(typeInfo);
  }

  *model = m.release();
  return successStatus();
}

CR_ONNX_Status CR_ONNX_LoadModelFromBuffer(
    CR_ONNX* lib, const void* modelData, size_t modelDataLen,
    int numThreads, CR_ONNX_Model** model) {
  *model = nullptr;
  OrtApiFunctions& api = lib->api;

  std::unique_ptr<CR_ONNX_Model> m(new CR_ONNX_Model(lib));
  std::string err;

  // Create session options.
  OrtStatus* status = api.CreateSessionOptions(&m->options);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: session options: " + err);
  }

  if (numThreads > 0) {
    status = api.SetIntraOpNumThreads(m->options, numThreads);
    err = lib->checkStatus(status);
    if (!err.empty()) {
      return makeStatus("onnxruntime: set threads: " + err);
    }
  }

  // Create session from buffer.
  status = api.CreateSessionFromArray(
      lib->env, modelData, modelDataLen, m->options, &m->session);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: load model from buffer: " + err);
  }

  // Create CPU memory info.
  status = api.CreateCpuMemoryInfo(OrtDeviceAllocator, OrtMemTypeDefault, &m->memoryInfo);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: memory info: " + err);
  }

  // Get allocator and introspect (same as LoadModel).
  OrtAllocator* allocator = nullptr;
  status = api.GetAllocatorWithDefaultOptions(&allocator);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: allocator: " + err);
  }

  status = api.SessionGetInputCount(m->session, &m->numInputs);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: input count: " + err);
  }

  m->inputNames = static_cast<char**>(calloc(m->numInputs, sizeof(char*)));
  for (size_t i = 0; i < m->numInputs; i++) {
    status = api.SessionGetInputName(m->session, i, allocator, &m->inputNames[i]);
    err = lib->checkStatus(status);
    if (!err.empty()) {
      return makeStatus("onnxruntime: input name: " + err);
    }
  }

  status = api.SessionGetOutputCount(m->session, &m->numOutputs);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: output count: " + err);
  }

  m->outputNames = static_cast<char**>(calloc(m->numOutputs, sizeof(char*)));
  for (size_t i = 0; i < m->numOutputs; i++) {
    status = api.SessionGetOutputName(m->session, i, allocator, &m->outputNames[i]);
    err = lib->checkStatus(status);
    if (!err.empty()) {
      return makeStatus("onnxruntime: output name: " + err);
    }
  }

  // Determine embedding dimensions.
  if (m->numOutputs > 0) {
    OrtTypeInfo* typeInfo = nullptr;
    status = api.SessionGetOutputTypeInfo(m->session, 0, &typeInfo);
    err = lib->checkStatus(status);
    if (err.empty() && typeInfo != nullptr) {
      const OrtTensorTypeAndShapeInfo* tensorInfo = nullptr;
      status = api.CastTypeInfoToTensorInfo(typeInfo, &tensorInfo);
      err = lib->checkStatus(status);
      if (err.empty() && tensorInfo != nullptr) {
        size_t numDims = 0;
        api.GetDimensionsCount(tensorInfo, &numDims);
        if (numDims > 0) {
          std::vector<int64_t> shape(numDims);
          api.GetDimensions(tensorInfo, shape.data(), numDims);
          m->dims = static_cast<int>(shape[numDims - 1]);
        }
      }
      api.ReleaseTypeInfo(typeInfo);
    }
  }

  *model = m.release();
  return successStatus();
}

void CR_ONNX_CloseModel(CR_ONNX_Model* model) {
  delete model;
}

int CR_ONNX_ModelDims(CR_ONNX_Model* model) {
  return model->dims;
}

int CR_ONNX_ModelNumInputs(CR_ONNX_Model* model) {
  return static_cast<int>(model->numInputs);
}

int CR_ONNX_ModelNumOutputs(CR_ONNX_Model* model) {
  return static_cast<int>(model->numOutputs);
}

CR_ONNX_Status CR_ONNX_RunInference(
    CR_ONNX* lib, CR_ONNX_Model* model,
    const int64_t* inputIDs, const int64_t* attentionMask,
    int64_t batchSize, int64_t seqLen,
    float** outputData, size_t* outputLen) {
  *outputData = nullptr;
  *outputLen = 0;

  OrtApiFunctions& api = lib->api;
  std::string err;

  // Validate that the model expects at least 2 inputs.
  if (model->numInputs < 2) {
    return makeStatus("onnxruntime: model expects fewer than 2 inputs");
  }

  int64_t inputShape[2] = {batchSize, seqLen};
  size_t inputDataLen = static_cast<size_t>(batchSize * seqLen) * sizeof(int64_t);

  // Create input tensors. CreateTensorWithDataAsOrtValue wraps existing
  // buffers without copying, so the input data must remain valid until
  // after Run() completes.
  OrtValue* inputIDsTensor = nullptr;
  OrtStatus* status = api.CreateTensorWithDataAsOrtValue(
      model->memoryInfo,
      const_cast<int64_t*>(inputIDs), inputDataLen,
      inputShape, 2,
      ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64,
      &inputIDsTensor);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    return makeStatus("onnxruntime: create input_ids tensor: " + err);
  }

  OrtValue* attentionMaskTensor = nullptr;
  status = api.CreateTensorWithDataAsOrtValue(
      model->memoryInfo,
      const_cast<int64_t*>(attentionMask), inputDataLen,
      inputShape, 2,
      ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64,
      &attentionMaskTensor);
  err = lib->checkStatus(status);
  if (!err.empty()) {
    api.ReleaseValue(inputIDsTensor);
    return makeStatus("onnxruntime: create attention_mask tensor: " + err);
  }

  // If the model expects a third input (token_type_ids), create a
  // zero-filled tensor. For single-sentence embedding tasks, token type
  // IDs are always 0.
  OrtValue* tokenTypeIdsTensor = nullptr;
  size_t numInputsToPass = 2;
  size_t tokenTypeDataLen = static_cast<size_t>(batchSize * seqLen);
  std::vector<int64_t> tokenTypeIDs;
  if (model->numInputs >= 3) {
    tokenTypeIDs.resize(tokenTypeDataLen, 0);
    status = api.CreateTensorWithDataAsOrtValue(
        model->memoryInfo,
        tokenTypeIDs.data(),
        tokenTypeDataLen * sizeof(int64_t),
        inputShape, 2,
        ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64,
        &tokenTypeIdsTensor);
    err = lib->checkStatus(status);
    if (!err.empty()) {
      api.ReleaseValue(inputIDsTensor);
      api.ReleaseValue(attentionMaskTensor);
      return makeStatus("onnxruntime: create token_type_ids tensor: " + err);
    }
    numInputsToPass = 3;
  }

  // Set up input/output arrays.
  const OrtValue* inputs[3] = {inputIDsTensor, attentionMaskTensor, tokenTypeIdsTensor};
  const char* inputNames[3] = {
      model->inputNames[0],
      model->inputNames[1],
      model->numInputs >= 3 ? model->inputNames[2] : nullptr,
  };

  // We request only the first output.
  OrtValue* outputTensor = nullptr;
  const char* outputNames[1] = {model->outputNames[0]};

  // Run inference.
  status = api.Run(
      model->session, nullptr,
      inputNames, inputs, numInputsToPass,
      outputNames, 1, &outputTensor);
  err = lib->checkStatus(status);

  // Release input tensors regardless of outcome.
  api.ReleaseValue(inputIDsTensor);
  api.ReleaseValue(attentionMaskTensor);
  if (tokenTypeIdsTensor != nullptr) {
    api.ReleaseValue(tokenTypeIdsTensor);
  }

  if (!err.empty()) {
    return makeStatus("onnxruntime: inference failed: " + err);
  }

  // Extract output data.
  float* rawOutput = nullptr;
  status = api.GetTensorMutableData(outputTensor, reinterpret_cast<void**>(&rawOutput));
  err = lib->checkStatus(status);
  if (!err.empty()) {
    api.ReleaseValue(outputTensor);
    return makeStatus("onnxruntime: get output data: " + err);
  }

  // Determine the total number of output elements by introspecting the
  // output tensor's type info.
  size_t totalElements = 0;
  OrtTensorTypeAndShapeInfo* outTensorInfo = nullptr;
  // GetTensorTypeAndShape is at a different offset; use element count
  // from the output shape instead. We compute it from the known shape.
  //
  // For sentence-transformer models, the output is either:
  //   [batch, seq, dims] (token_embeddings) — totalElements = batch*seq*dims
  //   [batch, dims]      (sentence_embedding) — totalElements = batch*dims
  //
  // We use GetTensorShapeElementCount if available, otherwise compute
  // from model dims.
  //
  // Since GetTensorShapeElementCount requires OrtTensorTypeAndShapeInfo from
  // the value (not the session), and we don't have that API resolved,
  // we compute from the model's known output shape.
  if (model->dims > 0) {
    // Try to figure out from the static shape. For a [batch, seq, dims]
    // model, total = batch * seq * dims. For [batch, dims], total = batch * dims.
    // We use the model's static shape to determine the rank.
    //
    // Introspect the session's output type info for rank.
    OrtTypeInfo* typeInfo = nullptr;
    status = api.SessionGetOutputTypeInfo(model->session, 0, &typeInfo);
    if (status == nullptr && typeInfo != nullptr) {
      const OrtTensorTypeAndShapeInfo* tensorInfo = nullptr;
      status = api.CastTypeInfoToTensorInfo(typeInfo, &tensorInfo);
      if (status == nullptr && tensorInfo != nullptr) {
        size_t numDims = 0;
        api.GetDimensionsCount(tensorInfo, &numDims);
        if (numDims == 3) {
          totalElements = static_cast<size_t>(batchSize * seqLen * model->dims);
        } else if (numDims == 2) {
          totalElements = static_cast<size_t>(batchSize * model->dims);
        }
      }
      api.ReleaseTypeInfo(typeInfo);
    }
  }

  if (totalElements == 0) {
    api.ReleaseValue(outputTensor);
    return makeStatus("onnxruntime: could not determine output size");
  }

  // Copy the output data to a caller-owned buffer.
  size_t outputBytes = totalElements * sizeof(float);
  float* result = static_cast<float*>(malloc(outputBytes));
  memcpy(result, rawOutput, outputBytes);

  api.ReleaseValue(outputTensor);

  *outputData = result;
  *outputLen = totalElements;
  return successStatus();
}

void CR_ONNX_FreeOutput(float* outputData) {
  free(outputData);
}

void CR_ONNX_FreeString(CR_ONNX_String str) {
  free(str.data);
}
