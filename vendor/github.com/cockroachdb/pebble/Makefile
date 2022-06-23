GO := go
PKG := ./...
GOFLAGS :=
STRESSFLAGS :=
TAGS := invariants
TESTS := .
LATEST_RELEASE := $(shell git fetch origin && git branch -r --list '*/crl-release-*' | grep -o 'crl-release-.*$$' | sort | tail -1)

.PHONY: all
all:
	@echo usage:
	@echo "  make test"
	@echo "  make testrace"
	@echo "  make stress"
	@echo "  make stressrace"
	@echo "  make stressmeta"
	@echo "  make crossversion-meta"
	@echo "  make mod-update"
	@echo "  make clean"

override testflags :=
.PHONY: test
test:
	${GO} test -mod=vendor -tags '$(TAGS)' ${testflags} -run ${TESTS} ${PKG}

.PHONY: testrace
testrace: testflags += -race -timeout 20m
testrace: test

.PHONY: stress stressrace
stressrace: testflags += -race
stress stressrace: testflags += -exec 'stress ${STRESSFLAGS}' -timeout 0 -test.v
stress stressrace: test

.PHONY: stressmeta
stressmeta: override PKG = ./internal/metamorphic
stressmeta: override STRESSFLAGS += -p 1
stressmeta: override TESTS = TestMeta$$
stressmeta: stress

.PHONY: crossversion-meta
crossversion-meta:
	git checkout ${LATEST_RELEASE}; \
		${GO} test -c ./internal/metamorphic -o './internal/metamorphic/crossversion/${LATEST_RELEASE}.test'; \
		git checkout -; \
		${GO} test -c ./internal/metamorphic -o './internal/metamorphic/crossversion/head.test'; \
		${GO} test -tags '$(TAGS)' ${testflags} -v -run 'TestMetaCrossVersion' ./internal/metamorphic/crossversion --version '${LATEST_RELEASE},${LATEST_RELEASE},${LATEST_RELEASE}.test' --version 'HEAD,HEAD,./head.test'

.PHONY: generate
generate:
	${GO} generate -mod=vendor ${PKG}

mod-update:
	${GO} get -u
	${GO} mod tidy
	${GO} mod vendor

.PHONY: clean
clean:
	rm -f $(patsubst %,%.test,$(notdir $(shell go list ${PKG})))

git_dirty := $(shell git status -s)

.PHONY: git-clean-check
git-clean-check:
ifneq ($(git_dirty),)
	@echo "Git repository is dirty!"
	@false
else
	@echo "Git repository is clean."
endif

.PHONY: mod-tidy-check
mod-tidy-check:
ifneq ($(git_dirty),)
	$(error mod-tidy-check must be invoked on a clean repository)
endif
	@${GO} mod tidy
	$(MAKE) git-clean-check

.PHONY: format
format:
	for _file in $$(gofmt -s -l . | grep -vE '^vendor/'); do \
		gofmt -s -w $$_file ; \
	done

.PHONY: format-check
format-check:
ifneq ($(git_dirty),)
	$(error format-check must be invoked on a clean repository)
endif
	$(MAKE) format
	$(MAKE) git-clean-check
