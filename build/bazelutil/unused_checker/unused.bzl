load("@io_bazel_rules_go//go:def.bzl", "GoArchive")
load("//build/bazelutil:targets.bzl", "GO_TESTS", "GO_TRANSITION_TESTS", "ALL_BINARIES", "ALL_LIBRARIES")

# This file contains a single macro disallowed_imports_test which internally
# generates a sh_test which ensures that the label provided as the first arg
# does not import any of the labels provided as a list in the second arg.

# _DepsInfo is used in the _deps_aspect to pick up all the .x archives
# constructed for a target.
_DepsInfo = provider(
  fields = {
      'archives': 'depset of .x archives'
  }
)

def _deps_aspect_impl(target, ctx):
  data = target[GoArchive].data
  archives = [data.export_file] + [d.export_file for d in data.extra_archive_datas]
  return [_DepsInfo(
      archives = depset(
        archives,
        transitive = [dep[_DepsInfo].archives for dep in ctx.rule.attr.deps],
      ),
  )]

_deps_aspect = aspect(
  implementation = _deps_aspect_impl,
  attr_aspects = ['deps'],
  provides = [_DepsInfo],
)

def _unused_rule_impl(ctx):
  accum = depset(transitive = [src[_DepsInfo].archives for src in ctx.attr.srcs])
  data = """# This is boilerplate taken directly from
#  https://github.com/bazelbuild/bazel/blob/master/tools/bash/runfiles/runfiles.bash
# See that page for an explanation of what this is and why it's necessary.
# --- begin runfiles.bash initialization v2 ---
# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---

if [ -z "${BUILD_WORKSPACE_DIRECTORY-}" ]; then
  echo "error: BUILD_WORKSPACE_DIRECTORY not set" >&2
  exit 1
fi

cd "$BUILD_WORKSPACE_DIRECTORY"

unused_checker_bin=$(rlocation com_github_cockroachdb_cockroach/build/bazelutil/unused_checker/unused_checker_/unused_checker)
if [ -z "${unused_checker_bin-}" ]; then
  echo "error: could not find the location of unused_checker"
  exit 1
fi

$unused_checker_bin """ + " ".join([f.path.replace("bazel-out", "_bazel/out") for f in accum.to_list() if '/external/' not in f.path])
  f = ctx.actions.declare_file("unused_test.sh")
  ctx.actions.write(f, data)
  return [
    DefaultInfo(executable = f),
  ]

_unused_rule = rule(
  implementation = _unused_rule_impl,
  executable = True,
  attrs = {
    'srcs' : attr.label_list(aspects = [_deps_aspect], providers = [GoArchive]),
  },
)

def unused_test():
  _unused_rule(
    name = "unused_script",
    srcs = GO_TESTS + ALL_BINARIES + ALL_LIBRARIES + GO_TRANSITION_TESTS,
    testonly = 1,
  )
  native.sh_test(
    name = "unused_test",
    data = ["//build/bazelutil/unused_checker"],
    deps = ["@bazel_tools//tools/bash/runfiles"],
    size = "small",
    srcs = [":unused_script"],
    tags = ["local"],
  )
