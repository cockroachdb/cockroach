#!/bin/bash -eu

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


cd $SRC/go-118-fuzz-build
go build .
mv go-118-fuzz-build /root/go/bin/

cd $SRC/cockroach
# Don't need the stdlib and libc++ gives linker error
CXXFLAGS="${CXXFLAGS/-stdlib=libc++/}"

# Needed for initialize the submodules, the submodule directories are empty.
# Some header files (proj_api.h here) do not actually exist in cockroach, it exists in the submodule.
git submodule update --init --recursive

# Install dependencies for compile_native_go_fuzzer
printf "package testutils\nimport _ \"github.com/AdamKorcz/go-118-fuzz-build/testing\"\n" > $SRC/cockroach/pkg/testutils/registerfuzzdependency.go
go mod edit -replace github.com/AdamKorcz/go-118-fuzz-build=$SRC/go-118-fuzz-build
go get github.com/AdamKorcz/go-118-fuzz-build

# Generate artifacts for building the binary
bazel run pkg/gen:code
bazel run pkg/cmd/generate-cgo:generate-cgo --run_under "cd $SRC/cockroach && "

# File is not being built via bazel and outdated (gives compilation errors in SessionIDEncoding)
rm pkg/sql/pgwire/fuzz.go

# Rename as go-118-fuzz-build cant read _test.go files (used by sessionID and frontier)
mv ./pkg/util/span/frontier_test.go pkg/util/span/frontier_test_fuzz.go

# Build the native go fuzz targets

# Runs into a race condition and panics (bug in go-118-fuzz-build)
#compile_native_go_fuzzer ./pkg/keys FuzzPrettyPrint fuzzPrettyPrint

compile_native_go_fuzzer ./pkg/sql/sqlliveness/slstorage FuzzSessionIDEncoding fuzzSessionIDEncoding

compile_native_go_fuzzer ./pkg/util/span FuzzBtreeFrontier fuzzBtreeFrontier

#compile_native_go_fuzzer ./pkg/util/span FuzzLLRBFrontier fuzzLLRBFrontier

compile_native_go_fuzzer ./pkg/ccl/pgcryptoccl/pgcryptocipherccl FuzzEncryptDecryptAES fuzzEncryptDecryptAES

compile_native_go_fuzzer ./pkg/ccl/pgcryptoccl/pgcryptocipherccl FuzzNoPaddingEncryptDecryptAES fuzzNoPaddingEncryptDecryptAES

# This needs some extra work to run on OSS-Fuzz: TODO @AdamKorcz
#compile_native_go_fuzzer ./pkg/storage FuzzEngineKeysInvariants fuzzEngineKeysInvariants

# Build old fuzz targets which used `gofuzz`
compile_go_fuzzer /src/cockroach/pkg/util/uuid Fuzz fuzzuuid

# Generate seed corpus
zip -r $OUT/fuzzuuid_seed_corpus.zip ./pkg/util/uuid/testdata/corpus || true
