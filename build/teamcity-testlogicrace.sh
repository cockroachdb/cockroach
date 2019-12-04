#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

source "$(dirname "${0}")/teamcity-support.sh"
maybe_ccache

mkdir -p artifacts

run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make testrace \
  GOTESTFLAGS=-json \
  PKG=./pkg/sql/logictest \
  TESTFLAGS='-v' \
  ENABLE_ROCKSDB_ASSERTIONS=1

# Run each of the optimizer tests again with randomized alternate query plans.

# Perturb the cost of each expression by up to 90%.
run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make testrace \
  GOTESTFLAGS=-json \
  PKG=./pkg/sql/logictest \
  TESTS='^TestLogic/local$$' \
  TESTFLAGS='-optimizer-cost-perturbation=0.9 -v' \
  ENABLE_ROCKSDB_ASSERTIONS=1

LOGICTESTS=`ls -A pkg/sql/logictest/testdata/logic_test/`

# Exclude the following tests when running with -disable-opt-rule-probability.
# These files either do not use the opt configurations or contain queries that
# rely on normalization rules for optimization, to avoid out-of-memory errors,
# or for subquery decorrelation.
EXCLUDE="(cluster_version|distsql_.*|explain_analyze.*|feature_counts|join|\
optimizer|orms|sequences_distsql|show_trace|subquery_correlated)"

# Disable each rule with 50% probability.
for file in $LOGICTESTS; do
    if [[ ! "$file" =~ (^|[[:space:]])${EXCLUDE}($|[[:space:]]) ]]; then
        run_json_test build/builder.sh \
          stdbuf -oL -eL \
          make testrace \
    GOTESTFLAGS=-json \
          PKG=./pkg/sql/logictest \
          TESTS='^TestLogic/local/'${file}'$$' \
          TESTFLAGS='-disable-opt-rule-probability=0.5 -v' \
          ENABLE_ROCKSDB_ASSERTIONS=1 \
    fi
done
