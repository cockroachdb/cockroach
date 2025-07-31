load("@io_bazel_rules_go//go:def.bzl", "GoLibrary")

# This file contains a single macro disallowed_imports_test which internally
# generates a sh_test which ensures that the label provided as the first arg
# does not import any of the labels provided as a list in the second arg.

# _DepsInfo is used in the _deps_aspect to pick up all the transitive
# dependencies of a go package.
_DepsInfo = provider(
    fields = {
        "cdeps": "dictionary with imported cdep names",
        "dep_pkgs": "dictionary with package names",
        "deps": "depset of targets",
    },
)

def _update_with_targets(attrs, attr_name, cdeps, dep_pkgs):
    if not hasattr(attrs, attr_name):
        return []
    dep_list = getattr(attrs, attr_name)
    for dep in dep_list:
        dep_pkgs.update(dep[_DepsInfo].dep_pkgs)
        cdeps.update(dep[_DepsInfo].cdeps)
    return dep_list

def _deps_aspect_impl(target, ctx):
    cdeps = {}
    if hasattr(ctx.rule.attr, "cdeps"):
        cdeps = {cdep.label: True for cdep in ctx.rule.attr.cdeps}
    dep_pkgs = {}
    if hasattr(ctx.rule.attr, "importpath"):
        dep_pkgs = {ctx.rule.attr.importpath: True}
    all_deps = []
    all_deps += _update_with_targets(ctx.rule.attr, "deps", cdeps, dep_pkgs)
    all_deps += _update_with_targets(ctx.rule.attr, "embed", cdeps, dep_pkgs)
    return [_DepsInfo(
        cdeps = cdeps,
        dep_pkgs = dep_pkgs,
        deps = depset(
            [target],
            transitive = [dep[_DepsInfo].deps for dep in all_deps],
        ),
    )]

_deps_aspect = aspect(
    implementation = _deps_aspect_impl,
    attr_aspects = ["deps", "embed"],
    provides = [_DepsInfo],
)

def _find_deps_with_disallowed_prefixes(current_pkg, dep_pkgs, prefixes):
    return [dp for dp_list in [
        [(d, p) for p in prefixes if d.startswith(p) and d != current_pkg]
        for d in dep_pkgs
    ] for dp in dp_list]

def _deps_rule_impl(ctx):
    failed_prefixes = _find_deps_with_disallowed_prefixes(
        current_pkg = ctx.attr.src[GoLibrary].importpath,
        dep_pkgs = ctx.attr.src[_DepsInfo].dep_pkgs,
        prefixes = ctx.attr.disallowed_prefixes,
    )
    deps = {k: None for k in ctx.attr.src[_DepsInfo].deps.to_list()}
    if ctx.attr.allowlist:
        failed = [p for p in deps if p not in ctx.attr.allowlist and
                                     p.label != ctx.attr.src.label]
    else:
        failed = [p for p in ctx.attr.disallowed_list if p in deps]
    failures = []
    if failed_prefixes:
        failures.extend([
            """\
echo >&2 "ERROR: {0} depends on {1} with disallowed prefix {2}"\
""".format(ctx.attr.src.label, p[0], p[1])
            for p in failed_prefixes
        ])
    if failed:
        failures.extend([
            """\
echo >&2 "ERROR: {0} imports {1}
\tcheck: bazel query 'somepath({0}, {1})'"\
""".format(ctx.attr.src.label, d.label)
            for d in failed
        ])
    if ctx.attr.disallow_cdeps:
        for cdep in ctx.attr.src[_DepsInfo].cdeps:
            failures.extend([
                """\
echo >&2 "ERROR: {0} depends on a c-dep {1}, which is disallowed"\
""".format(ctx.attr.src.label, cdep),
            ])
    if failures:
        data = "\n".join(failures + ["exit 1"])
    else:
        data = ""
    f = ctx.actions.declare_file(ctx.attr.name + "_deps_test.sh")
    ctx.actions.write(f, data)
    return [
        DefaultInfo(executable = f),
    ]

_deps_rule = rule(
    implementation = _deps_rule_impl,
    executable = True,
    attrs = {
        "src": attr.label(aspects = [_deps_aspect], providers = [GoLibrary]),
        "allowlist": attr.label_list(providers = [GoLibrary]),
        "disallow_cdeps": attr.bool(mandatory = False, default = False),
        "disallowed_list": attr.label_list(providers = [GoLibrary]),
        "disallowed_prefixes": attr.string_list(mandatory = False, allow_empty = True),
    },
)

def _validate_disallowed_prefixes(prefixes):
    validated = []
    repo_prefix = "github.com/cockroachdb/cockroach/"
    short_prefix = "pkg/"
    for prefix in prefixes:
        if prefix.startswith(repo_prefix):
            validated.append(prefix)
        elif prefix.startswith(short_prefix):
            validated.append(repo_prefix + prefix)
        else:
            fail("invalid prefix {}: should start with {} or {}".format(
                prefix,
                repo_prefix,
                short_prefix,
            ))
    return validated

def disallowed_imports_test(
        src,
        disallowed_list = [],
        disallowed_prefixes = [],
        disallow_cdeps = False,
        allowlist = []):
    if (disallowed_list and allowlist) or (disallowed_prefixes and allowlist):
        fail("allowlist or (disallowed_list or disallowed_prefixes) can be " +
             "provided, but not both")
    disallowed_prefixes = _validate_disallowed_prefixes(disallowed_prefixes)
    script = src.strip(":") + "_disallowed_imports_script"
    _deps_rule(
        name = script,
        testonly = 1,
        src = src,
        allowlist = allowlist,
        disallowed_list = disallowed_list,
        disallowed_prefixes = disallowed_prefixes,
        disallow_cdeps = disallow_cdeps,
    )
    native.sh_test(
        name = src.strip(":") + "_disallowed_imports_test",
        size = "small",
        srcs = [":" + script],
    )
