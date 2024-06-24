#!/bin/bash -eu
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
################################################################################

# libc++ gives linker error(td::__cxx11::basic_string is definitely a libstdc++ symbol, only in FuzzSessionIDEncoding),
# switching to  libstdc++ instead (libc++ is instrumented, libstdc++ is not)
# TO DO try this workaround : https://github.com/google/oss-fuzz/issues/1114#issuecomment-360649334
current_stdlib=$(echo $CXXFLAGS | grep -oP '(?<=-stdlib=)[^"]*')
CXXFLAGS="${CXXFLAGS/-stdlib=$current_stdlib/-stdlib=libstdc++}"
# Needed for initialize the submodules, the submodule directories are empty.
# Some header files (proj_api.h here) do not actually exist in cockroach, it exists in the submodule.
git submodule update --init --recursive

# Install dependencies for compile_native_go_fuzzer
go install github.com/AdamKorcz/go-118-fuzz-build@latest
go get github.com/AdamKorcz/go-118-fuzz-build/testing

bazel clean --expunge

# Generate artifacts for building the binary
bazel run pkg/gen:code
bazel run pkg/cmd/generate-cgo:generate-cgo --run_under "cd $SRC/cockroach && "

# File is not being built via bazel and doesnt seem to be in use anymore (gives compilation errors in SessionIDEncoding)
rm pkg/sql/pgwire/fuzz.go

# Rename as go-118-fuzz-build cant read _test.go files (used by sessionID and frontier)
mv ./pkg/util/span/frontier_test.go pkg/util/span/frontier_test_fuzz.go

# Build the fuzz targets
compile_native_go_fuzzer ./pkg/keys FuzzPrettyPrint fuzzPrettyPrint

compile_native_go_fuzzer ./pkg/sql/sqlliveness/slstorage FuzzSessionIDEncoding fuzzSessionIDEncoding

compile_native_go_fuzzer ./pkg/util/span FuzzBtreeFrontier fuzzBtreeFrontier

# hacky fix since we've already copied these files (used in coverage only)
sed -i '/cp "\${fuzzer_filename}" "\${OUT}\/rawfuzzers\/\${fuzzer}"/d' /usr/local/bin/compile_native_go_fuzzer > /dev/null 2>&1
compile_native_go_fuzzer ./pkg/util/span FuzzLLRBFrontier fuzzLLRBFrontier

compile_native_go_fuzzer ./pkg/ccl/pgcryptoccl/pgcryptocipherccl FuzzEncryptDecryptAES fuzzEncryptDecryptAES

compile_native_go_fuzzer ./pkg/ccl/pgcryptoccl/pgcryptocipherccl FuzzNoPaddingEncryptDecryptAES fuzzNoPaddingEncryptDecryptAES

compile_native_go_fuzzer ./pkg/storage FuzzEngineKeysInvariants fuzzEngineKeysInvariants

compile_go_fuzzer /src/cockroach/pkg/util/uuid Fuzz fuzzuuid
