load("@bazel_skylib//lib:shell.bzl", "shell")

# lint_binary works as follows:
# 1. For each test, we generate a script, which uses linttest.sh.in as a
#    template. It simply bootstraps the environment by locating the go SDK,
#    setting an appropriate `PATH` and `GOROOT`, and cd-ing to the right
#    directory in the workspace. This roughly replicates what `go test` would
#    do.
# 2. Using that script, we create a `sh_binary` using that script as an entry
#    point with the appropriate dependencies.

def _gen_script_impl(ctx):
    subs = {
        "@@PACKAGE@@": shell.quote(ctx.attr.test.label.package),
        "@@NAME@@": shell.quote(ctx.attr.test.label.name),
    }
    out_file = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.expand_template(
        template = ctx.file._template,
        output = out_file,
        substitutions = subs,
    )
    return [
        DefaultInfo(files = depset([out_file])),
    ]

_gen_script = rule(
    implementation = _gen_script_impl,
    attrs = {
        "test": attr.label(mandatory = True),
        "_template": attr.label(
            default = "@cockroach//build/bazelutil:lint.sh.in",
            allow_single_file = True)
    },
)

def lint_binary(name, test):
    script_name = name + ".sh"
    _gen_script(
        name = script_name,
        test = test,
        testonly = 1,
    )
    native.sh_binary(
        name = name,
        srcs = [script_name],
        data = [
            test,
            "//c-deps:libedit_files",
            "//c-deps:libgeos_files",
            "//c-deps:libproj_files",
            "//c-deps:libroach_files",
            "//pkg/cmd/returncheck",
            "//pkg/cmd/roachvet",
            "//pkg/sql/opt/optgen/cmd/optfmt",
            "@co_honnef_go_tools//cmd/staticcheck",
            "@com_github_client9_misspell//cmd/misspell:misspell",
            "@com_github_cockroachdb_crlfmt//:crlfmt",
            "@com_github_kisielk_errcheck//:errcheck",
            "@go_sdk//:bin/go",
            "@org_golang_x_lint//golint:golint",
        ],
        deps = ["@bazel_tools//tools/bash/runfiles"],
        testonly = 1,
    )
