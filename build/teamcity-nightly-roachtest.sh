#!/usr/bin/env bash
# Entry point for the nightly roachtests. These are run from CI and require
# appropriate secrets for the ${CLOUD} parameter (along with other things,
# apologies, you're going to have to dig around for them below or even better
# yet, look at the job).

set -euo pipefail

"build/teamcity-nightly-roachtest-${CLOUD}.sh"
