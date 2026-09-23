# Common test definitions for CockroachDB test scenarios

This directory contains the "building blocks" to define scenario
tests.

The following conventions are used:

- servers are named `n1`, `n2`, ...
- clients (workers) are named `w1`, `w2`, ...
- acts 1 to 3 of the storyline are used for initialization:
  - act 1 for server initialization.
  - act 2 for client initialization.
  - act 3 for ramp-up.
- acts 4 and further are used to exercise test clusters.
- the mood "blue" is used to identify ramp-up periods.
- for tests that define only one nemesis (operational event) at a time:
  - mood "orange" identifies the period where the nemesis activates on the affected node
  - mood "red" identifies the period during which the effects of the nemesis are active
  - mood "yellow" identifies the period where the nemesis deactivates on the affected node

General template for a test:

```
# tweak this to change the server/client combination
include workers/xxx.cfg
include nemeses/yyy.cfg

# common cast + scenes
include servers.cfg
include workers.cfg
include nemesis_n1.cfg

# short/long/cycle or define your own
include scripts/zzz.cfg

# desired audience
include workers/check_xxx.cfg

# ...additional definitions here...
```
