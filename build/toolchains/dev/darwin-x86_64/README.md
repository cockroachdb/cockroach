This directory defines a simple macOS toolchain for development based on
Clang 10.0. It can be enabled by adding

    build --crosstool_top=@toolchain_dev_darwin_x86-64//:suite

to your `.bazelrc.user`. Note that a full XCode installation (not just
command-line tools) is required.

For the definition of the repo containing the toolchain, check out
`toolchain.bzl`. This uses `BUILD.darwin-x86_64` and
`cc_toolchain_config.bzl.tmpl` as templates to populate the `BUILD` and
`cc_toolchain_config.bzl` files in the new repo.