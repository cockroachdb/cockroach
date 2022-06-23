# Copyright 2016 Michal Witkowski. All Rights Reserved.
# See LICENSE for licensing terms.

export PATH := ${GOPATH}/bin:${PATH}

install:
	@echo "--- Installing govalidators to GOPATH"
	go install github.com/mwitkow/go-proto-validators/protoc-gen-govalidators

regenerate_test_gogo:
	@echo "Regenerating test .proto files with gogo imports"
	(protoc  \
	--proto_path=${GOPATH}/src \
 	--proto_path=test \
	--gogo_out=test/gogo \
	--govalidators_out=gogoimport=true:test/gogo test/*.proto)

regenerate_test_golang:
	@echo "--- Regenerating test .proto files with golang imports"
	(protoc  \
	--proto_path=${GOPATH}/src \
 	--proto_path=test \
	--go_out=test/golang \
	--govalidators_out=test/golang test/*.proto)

regenerate_example: install
	@echo "--- Regenerating example directory"
	(protoc  \
	--proto_path=${GOPATH}/src \
	--proto_path=. \
	--go_out=. \
	--govalidators_out=. examples/*.proto)

test: install regenerate_test_gogo regenerate_test_golang
	@echo "Running tests"
	go test -v ./...

regenerate:
	@echo "--- Regenerating validator.proto"
	(protoc \
	--proto_path=${GOPATH}/src \
	--proto_path=${GOPATH}/src/github.com/gogo/protobuf/protobuf \
	--proto_path=. \
	--gogo_out=Mgoogle/protobuf/descriptor.proto=github.com/gogo/protobuf/protoc-gen-gogo/descriptor:. \
	validator.proto)
