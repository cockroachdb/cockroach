load("@build_bazel_rules_nodejs//:index.bzl", "node_repositories", "yarn_install")

_doc = """
Ensures the current user's global yarn cache (typically in ~/.cache/yarn)
contains all vendored yarn dependencies, so that later yarn_install calls can
be --offline without having access to the yarn-vendor submodule.

build_bazel_rules_nodejs's yarn_install can only get access to the
pkg/ui/yarn-vendor submodule by explicitly listing each .tgz file in that
submodule as a dependency, which causes every file to be copied individually
(in serial) and makes the yarn_install task unusably slow (> 50 minutes on a
24-core Xeon machine with 32GB of RAM). To ensure yarn_install only uses
packages from the yarn-vendor submodule this rule helps to leverage the fact
that yarn_install has access to the user's "global" yarn cache, which is
typically used to accelerate `yarn install` by not repeatedly fetching packages
from remote locations.

Specifically, this rule performs one `yarn install` invocation for each package
from within the Bazel workspace (not the sandbox!) where access to
pkg/ui/yarn-vendor is guaranteed, and stores the resulting modules in a location
that intentionally won't be read. It relies on the fact that performing those
installations has a side-effect of adding each installed dependency to the
global yarn cache.
"""

def _seed_yarn_cache_impl(rctx):
    workspace_root = str(
        rctx.path(
            Label("//:WORKSPACE")
        ).dirname
    )

    rctx.report_progress("Checking for yarn-vendor submodule...")
    res = rctx.execute(
        [
            rctx.path(Label("//build/bazelutil:check_yarn_vendor_submodule.sh")),
            workspace_root + "/pkg/ui"
        ],
    )
    if res.return_code != 0:
        fail("Unable to seed yarn cache: " + res.stderr)

    paths = {
        "protos": "pkg/ui/packages/db-console/src/js",
        "cluster_ui": "pkg/ui/packages/cluster-ui",
        "db_console": "pkg/ui/packages/db-console",
    }
    yarn_dir = str(rctx.path(Label("@yarn//:bin/yarn")).dirname)
    for key, path in paths.items():
        rctx.report_progress("Seeding yarn cache for {}...".format(path))
        # Execute a script that uses the bazel-installed yarn (via $PATH) to install
        # dependencies while running at the root of the *non-sandboxed* node
        # package (argv[1]), but putting those dependencies in a repo-specific
        # location (argv[2]) to avoid corrupting the local node_modules
        # directories.
        res = rctx.execute(
            [
                rctx.path(Label("//build/bazelutil:seed_yarn_cache.sh")),
                workspace_root + "/" + path,
                rctx.path("node_modules." + key),
            ],
            environment = {
              "PATH": "{}:{}".format(yarn_dir, rctx.os.environ["PATH"])
            },
        )

        if res.return_code != 0:
            fail("Unable to seed yarn cache: " + res.stderr)

    rctx.file(
        ".seed",
        content = "",
        executable = False,
    )
    rctx.file(
        "BUILD.bazel",
        content = """exports_files([".seed"])""",
        executable = False,
    )

seed_yarn_cache = repository_rule(
    implementation = _seed_yarn_cache_impl,
    doc = _doc,
    local = True,
)
