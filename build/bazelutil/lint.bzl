load("@bazel_skylib//lib:shell.bzl", "shell")

# lint_test_binary works as follows:
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
            default = "@cockroach//build/bazelutil:linttest.sh.in",
            allow_single_file = True,
        ),
    },
)

# I understand the name lint_test_binary is confusing. Sorry. Unfortunately,
# it's accurate: it's not appropriate to call them "tests", since the targets
# are not tests and can't be run with `bazel test`. So the name reflects that
 # the targets are binaries, that when run, run a test of a linter. Sorry again.
def lint_test_binary(name, test):
    script_name = name+".sh"
    _gen_script(test=test, name=script_name, testonly=1)
    native.sh_binary(
        name=name,
        srcs=[script_name],
        data = [
            test,
            "@go_sdk//:bin/go",
        ],
        deps = ["@bazel_tools//tools/bash/runfiles"],
        testonly = 1,
    )
