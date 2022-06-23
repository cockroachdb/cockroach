.PHONY: dist
dist:
	@mkdir -p ./bin
	@rm -f ./bin/*
	GOOS=darwin  GOARCH=amd64 go build -o ./bin/goose-darwin64       ./cmd/goose
	GOOS=linux   GOARCH=amd64 go build -o ./bin/goose-linux64        ./cmd/goose
	GOOS=linux   GOARCH=386   go build -o ./bin/goose-linux386       ./cmd/goose
	GOOS=windows GOARCH=amd64 go build -o ./bin/goose-windows64.exe  ./cmd/goose
	GOOS=windows GOARCH=386   go build -o ./bin/goose-windows386.exe ./cmd/goose

test-packages:
	go test -v $$(go list ./... | grep -v -e /tests -e /bin -e /cmd -e /examples)

test-e2e: test-e2e-postgres test-e2e-mysql

test-e2e-postgres:
	go test -v ./tests/e2e -dialect=postgres

test-e2e-mysql:
	go test -v ./tests/e2e -dialect=mysql

docker-cleanup:
	docker stop -t=0 $$(docker ps --filter="label=goose_test" -aq)
