# DEPRECATED

Cockroach Labs has stopped using this repository (outside of builds for older releases). This code is no longer maintained.

[![Docker automated build](https://img.shields.io/badge/docker-automated--build-blue.svg?style=flat-square)](https://hub.docker.com/r/xjewer/go-test-teamcity/)

# Golang test TeamCity converter

Convert go test output to TeamCity format

Support Run, Skip, Pass, Fail

### Installation

```bash
go get github.com/2tvenom/go-test-teamcity
```

### How use
```bash
go test -v ./... | go-test-teamcity
```

### Docker
```bash
go test -v ./... | docker run -i xjewer/go-test-teamcity
```

### Links
- https://confluence.jetbrains.com/display/TCD9/Build+Script+Interaction+with+TeamCity
