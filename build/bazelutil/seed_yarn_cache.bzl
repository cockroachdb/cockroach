load("@build_bazel_rules_nodejs//:index.bzl", "node_repositories", "yarn_install")

# Returns the root of the user workspace. No built-in way to get
# this but we can derive it from the path of the package.json file
# in the user workspace sources.
# From https://github.com/bazelbuild/rules_nodejs/blob/55a84d14604b121230a2013b2469d93e38f1f601/internal/npm_install/npm_install.bzl#L367
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
        "protos": "pkg/ui/workspaces/db-console/src/js",
        "cluster_ui": "pkg/ui/workspaces/cluster-ui",
        "db_console": "pkg/ui/workspaces/db-console",
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
    local = True,
)
