#!/usr/bin/env bash

set -euo pipefail

# This script performs assorted checks to make sure there is nothing obviously
# wrong with the Bazel build.

EXISTING_GO_GENERATE_COMMENTS="
pkg/roachprod/vm/aws/config.go://go:generate go-bindata -mode 0600 -modtime 1400000000 -pkg aws -o embedded.go config.json old.json
pkg/roachprod/vm/aws/config.go://go:generate gofmt -s -w embedded.go
pkg/roachprod/vm/aws/config.go://go:generate goimports -w embedded.go
pkg/roachprod/vm/aws/config.go://go:generate terraformgen -o terraform/main.tf
pkg/cmd/roachtest/prometheus/prometheus.go://go:generate mockgen -package=prometheus -destination=mocks_generated_test.go . Cluster
pkg/cmd/roachtest/tests/drt.go://go:generate mockgen -package tests -destination drt_generated_test.go . PromClient
pkg/kv/kvclient/kvcoord/transport.go://go:generate mockgen -package=kvcoord -destination=mocks_generated_test.go . Transport
pkg/kv/kvclient/rangecache/range_cache.go://go:generate mockgen -package=rangecachemock -destination=rangecachemock/mocks_generated.go . RangeDescriptorDB
pkg/kv/kvclient/rangefeed/rangefeed.go://go:generate mockgen -destination=mocks_generated_test.go --package=rangefeed . DB
pkg/kv/kvserver/concurrency/lock_table.go://go:generate ../../../util/interval/generic/gen.sh *lockState concurrency
pkg/kv/kvserver/spanlatch/manager.go://go:generate ../../../util/interval/generic/gen.sh *latch spanlatch
pkg/roachpb/api.go://go:generate mockgen -package=roachpbmock -destination=roachpbmock/mocks_generated.go . InternalClient,Internal_RangeFeedClient
pkg/roachpb/batch.go://go:generate go run gen/main.go --filename batch_generated.go *.pb.go
pkg/security/certmgr/cert.go://go:generate mockgen -package=certmgr -destination=mocks_generated_test.go . Cert
pkg/security/securitytest/securitytest.go://go:generate go-bindata -mode 0600 -modtime 1400000000 -pkg securitytest -o embedded.go -ignore README.md -ignore regenerate.sh test_certs
pkg/security/securitytest/securitytest.go://go:generate gofmt -s -w embedded.go
pkg/security/securitytest/securitytest.go://go:generate goimports -w embedded.go
pkg/server/api_v2.go://go:generate swagger generate spec -w . -o ../../docs/generated/swagger/spec.json --scan-models
pkg/sql/conn_fsm.go://go:generate ../util/fsm/gen/reports.sh TxnStateTransitions stateNoTxn
pkg/sql/opt/optgen/lang/expr.go://go:generate langgen -out expr.og.go exprs lang.opt
pkg/sql/opt/optgen/lang/expr.go://go:generate langgen -out operator.og.go ops lang.opt
pkg/sql/schemachanger/scexec/exec_backfill_test.go://go:generate mockgen -package scexec_test -destination=mocks_generated_test.go --self_package scexec . Catalog,Dependencies,Backfiller,BackfillTracker,IndexSpanSplitter,PeriodicProgressFlusher
pkg/sql/schemachanger/scop/backfill.go://go:generate go run ./generate_visitor.go scop Backfill backfill.go backfill_visitor_generated.go
pkg/sql/schemachanger/scop/mutation.go://go:generate go run ./generate_visitor.go scop Mutation mutation.go mutation_visitor_generated.go
pkg/sql/schemachanger/scop/validation.go://go:generate go run ./generate_visitor.go scop Validation validation.go validation_visitor_generated.go
pkg/sql/schemachanger/scpb/state.go://go:generate go run element_generator.go --in elements.proto --out elements_generated.go
pkg/sql/schemachanger/scpb/state.go://go:generate go run element_uml_generator.go --out uml/table.puml
pkg/util/interval/generic/doc.go:  //go:generate ../../util/interval/generic/gen.sh *latch spanlatch
pkg/util/interval/generic/example_t.go://go:generate ./gen.sh *example generic
pkg/util/log/channels.go://go:generate go run gen/main.go logpb/log.proto channel.go channel/channel_generated.go
pkg/util/log/channels.go://go:generate go run gen/main.go logpb/log.proto log_channels.go log_channels_generated.go
pkg/util/log/channels.go://go:generate go run gen/main.go logpb/log.proto logging.md ../../../docs/generated/logging.md
pkg/util/log/channels.go://go:generate go run gen/main.go logpb/log.proto severity.go severity/severity_generated.go
pkg/util/log/sinks.go://go:generate mockgen -package=log -destination=mocks_generated_test.go --mock_names=TestingLogSink=MockLogSink . TestingLogSink
pkg/util/timeutil/zoneinfo.go://go:generate go run gen/main.go
"

EXISTING_BROKEN_TESTS_IN_BAZEL="
pkg/acceptance/BUILD.bazel
pkg/cmd/cockroach-oss/BUILD.bazel
pkg/cmd/github-post/BUILD.bazel
pkg/cmd/prereqs/BUILD.bazel
pkg/cmd/publish-artifacts/BUILD.bazel
pkg/cmd/roachtest/BUILD.bazel
pkg/cmd/teamcity-trigger/BUILD.bazel
"

EXISTING_CRDB_TEST_BUILD_CONSTRAINTS="
pkg/util/buildutil/crdb_test_off.go://go:build !crdb_test || crdb_test_off
pkg/util/buildutil/crdb_test_on.go://go:build crdb_test && !crdb_test_off
"

git grep 'go:generate stringer' pkg | while read LINE; do
    dir=$(dirname $(echo $LINE | cut -d: -f1))
    type=$(echo $LINE | grep -o -- '-type[= ][^ ]*' | sed 's/-type[= ]//g' | awk '{print tolower($0)}')
    build_out=$(bazel query --output=build "//$dir:${type}_string.go")
    if [[ -z "$build_out" ]]; then
        echo 'Detected an autogenerated file that is not built inside the Bazel sandbox: '
        echo "  $dir/${type}_string.go, generated by: $LINE"
        echo 'Generate this file using the Bazel sandbox (see the utilities in build/STRINGER.bzl);'
        exit 1
    fi
done

# We exclude stringer and add-leaktest.sh -- the former is already all
# Bazelfied, and the latter can be safely ignored since we have a lint to check
# the same thing: https://github.com/cockroachdb/cockroach/issues/64440
git grep '//go:generate' -- './*.go' | grep -v stringer | grep -v 'add-leaktest\.sh' | while read LINE; do
    if [[ "$EXISTING_GO_GENERATE_COMMENTS" == *"$LINE"* ]]; then
	# Grandfathered.
	continue
    fi
    echo 'Detected an unknown //go:generate comment:'
    echo "$LINE"
    echo 'Please ensure that the equivalent logic to generate these files is'
    echo 'present in the Bazel build as well, then add the line to the'
    echo 'EXISTING_GO_GENERATE_COMMENTS in build/bazelutil/check.sh.'
    echo 'Also see https://cockroachlabs.atlassian.net/wiki/spaces/CRDB/pages/1380090083/How+to+ensure+your+code+builds+with+Bazel'
    exit 1
done

git grep 'broken_in_bazel' pkg | grep BUILD.bazel: | grep -v pkg/BUILD.bazel | grep -v pkg/cli/BUILD.bazel | grep -v generate-test-suites | cut -d: -f1 | while read LINE; do
    if [[ "$EXISTING_BROKEN_TESTS_IN_BAZEL" == *"$LINE"* ]]; then
	# Grandfathered.
	continue
    fi
    echo "A new broken test in Bazel was added in $LINE"
    echo 'Ensure the test runs with Bazel, then remove the broken_in_bazel tag.'
    exit 1
done

git grep '//go:build' pkg | grep crdb_test | while read LINE; do
    if [[ "$EXISTING_CRDB_TEST_BUILD_CONSTRAINTS" == *"$LINE"* ]]; then
        # Grandfathered.
        continue
    fi
    echo "A new crdb_test/crdb_test_off build constraint was added in $LINE"
    echo 'Make sure you port the conditional compilation logic to the Bazel build,'
    echo 'which does not use the build tags in the same way.'
    echo "Once you've done so, you can add the line to "
    echo 'EXISTING_CRDB_TEST_BUILD_CONSTRAINTS in build/bazelutil/check.sh.'
    exit 1
done
