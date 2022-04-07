load("@io_bazel_rules_go//go:def.bzl", "GoLibrary")

# This file contains a single macro disallowed_imports_test which internally
# generates a sh_test which ensures that the label provided as the first arg
# does not import any of the labels provided as a list in the second arg.

# _DepsInfo is used in the _deps_aspect to pick up all the transitive
# dependencies of a go package.
_DepsInfo = provider(
    fields = {'deps' : 'depset of targets'}
)

def _deps_aspect_impl(target, ctx):
    return [_DepsInfo(
      deps = depset(
        [target],
        transitive = [dep[_DepsInfo].deps for dep in ctx.rule.attr.deps],
      )
    )]

_deps_aspect = aspect(
    implementation = _deps_aspect_impl,
    attr_aspects = ['deps'],
    provides = [_DepsInfo],
)

def _deps_rule_impl(ctx):
    deps = {k: None for k in ctx.attr.src[_DepsInfo].deps.to_list()}
    data = ""
    failed = [p for p in ctx.attr.disallowed if p in deps]
    if failed:
        failures = [
            """echo >&2 "ERROR: {0} imports {1}
\tcheck: bazel query 'somepath({0}, {1})'"\
""".format(
               ctx.attr.src.label, d.label,
            ) for d in failed
        ]
        data = "\n".join(failures + ["exit 1"])
    f = ctx.actions.declare_file(ctx.attr.name + "_deps_test.sh")
    ctx.actions.write(f, data)
    return [
      DefaultInfo(executable = f),
    ]

_deps_rule = rule(
    implementation = _deps_rule_impl,
    executable = True,
    attrs = {
        'src' : attr.label(aspects = [_deps_aspect], providers = [GoLibrary]),
        'disallowed': attr.label_list(providers = [GoLibrary]),
    },
)

def disallowed_imports_test(src, disallowed):
  script = src.strip(":") + "_disallowed_imports_script"
  _deps_rule(name = script, src = src, disallowed = disallowed)
  native.sh_test(
      name = src.strip(":") + "_disallowed_imports_test",
      srcs = [":"+script],
      tags = ["local"],
  )


