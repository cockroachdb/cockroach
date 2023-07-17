load(":archived.bzl", "archived_cdep_repository")

# c-deps repository definitions.

# Define the repositories for each of our c-dependencies. BUILD_ALL_CONTENT is
# a shorthand to glob over all checked-in files.
BUILD_ALL_CONTENT = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])"""

# Each of these c-dependencies map to one or more library definitions in the
# top-level BUILD.bazel. Library definitions will list the following
# definitions as sources.

# This is essentially the same above, we elide a generated file to avoid
# permission issues when building jemalloc within the bazel sandbox.
BUILD_JEMALLOC_CONTENT = """filegroup(name = "all", srcs = glob(["**"], exclude=["configure"]), visibility = ["//visibility:public"])"""

# We do need to add native as new_local_repository is defined in Bazel core.
def c_deps():
    native.new_local_repository(
        name = "geos",
        path = "c-deps/geos",
        build_file_content = BUILD_ALL_CONTENT,
    )
    native.new_local_repository(
        name = "jemalloc",
        path = "c-deps/jemalloc",
        build_file_content = BUILD_JEMALLOC_CONTENT,
    )
    native.new_local_repository(
        name = "krb5",
        path = "c-deps/krb5",
        build_file_content = BUILD_ALL_CONTENT,
    )
    native.new_local_repository(
        name = "proj",
        path = "c-deps/proj",
        build_file_content = BUILD_ALL_CONTENT,
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linux",
        sha256 = "41742e88359d55a8d45207e5628e28cd7ea7689b804bbb6ea5950e26bc7da070",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linux",
        sha256 = "9bac2dd86a77e47b2891649cfe8b14f8885ccd7d08ab1283eca7889bba65a576",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linux",
        sha256 = "fcc88b98b93109c70edf2180f969e2164c9c25bdffcebc647116b47b80114863",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linux",
        sha256 = "a36e373c04817f322dc1482a192a0911a41855ccc30f0c356e76133db4333aa4",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "linuxarm",
        sha256 = "93d19ab0c8506c0d00284e0e393ff8703a0bbf7ed3103f242e29ffdef20bbdd8",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "linuxarm",
        sha256 = "124c29566233005309becd58c5deaa80c1d961a9fb18bba0807865f8f1ee07af",
    )
    archived_cdep_repository(
        lib = "libkrb5",
        config = "linuxarm",
        sha256 = "1fd3e8a24ee8345986f705c7bae9977a3c574c43a62f17025959d041081a1678",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "linuxarm",
        sha256 = "b97df26615a592434f782bcd4f5faacbbb4f939da78004e447764b66a445d942",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macos",
        sha256 = "3fa2c5a668a097cc6ff75ed6b8ff7c2a67364f9f73fbd7f930dcbc880dcf398d",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macos",
        sha256 = "aad0b1ec36d37d5f91d22936eff0d0676a35bb5cb140949a9b85d734df64ed78",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macos",
        sha256 = "3235279411c84fa03e5b19ac8b28756dabf54535ea67a98c0e44f73197e20bea",
    )
    archived_cdep_repository(
        lib = "libgeos",
        config = "macosarm",
        sha256 = "0eccaa5745f0228bb4be02ef20970078861a3652ef6ff91b436f44dd7fbc7f65",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "macosarm",
        sha256 = "4494582bd2909440124ed46d5fb84142742706c5edc968ee3ff6be0f62b4e2ad",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "macosarm",
        sha256 = "eb8830c18b5d906d9951934ae9b8e2bf99e2d4d0d74897dff6dd4f35c50aef54",
    )
    archived_cdep_repository(
        lib = "libjemalloc",
        config = "windows",
        sha256 = "cab948999d3f16e820fe1c58b85164718903b6a55a3634edf6564645b057ee71",
    )
    archived_cdep_repository(
        lib = "libproj",
        config = "windows",
        sha256 = "ca1335c8182a2562ea43e0803d4ca437d45fb95462896752219f19cb302814eb",
    )
