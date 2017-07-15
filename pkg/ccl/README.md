# Cockroach Community License (ccl) Functionality
This tree is intended to house our non-Apache2 licensed Go packages. By
convention, all packages under this tree have the suffix `ccl`.

Grouping CCL packages into one tree and clearly labeling all CCL packages
with a recognizable suffix will hopefully make it easier to identify and prevent
introducing any accidental dependencies on ccl from Apache2 packages.

C++ CCL code lives in [c-deps/libroach/ccl](/c-deps/libroach/ccl).
