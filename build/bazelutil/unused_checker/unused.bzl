load("@io_bazel_rules_go//go:def.bzl", "GoArchive")

# _XInfo is used in the _deps_aspect to pick up all the .x archives
# constructed for a target.
_XInfo = provider(
  fields = {
      'archives': 'depset of .x archives'
  }
)

def _x_aspect_impl(target, ctx):
  data = target[GoArchive].data
  archives = [data.export_file] + [d.export_file for d in data.extra_archive_datas]
  return [_XInfo(
      archives = depset(
        archives,
        transitive = [dep[_XInfo].archives for dep in ctx.rule.attr.deps],
      ),
  )]

_x_aspect = aspect(
  implementation = _x_aspect_impl,
  attr_aspects = ['deps'],
  provides = [_XInfo],
)

def _get_x_data_rule_impl(ctx):
  accum = depset(transitive = [src[_XInfo].archives for src in ctx.attr.srcs])
  f = ctx.actions.declare_file("x_archives.txt")
  ctx.actions.write(f, "\n".join([f.path for f in accum.to_list() if "/external/" not in f.path]))
  return [
    DefaultInfo(files = depset(direct = [f])),
    _XInfo(archives = accum),
  ]

_get_x_data = rule(
  implementation = _get_x_data_rule_impl,
  attrs = {
    'srcs' : attr.label_list(aspects = [_x_aspect], providers = [GoArchive]),
  },
)

def get_x_data(name):
  if name != "get_x_data":
    fail("name must be 'get_x_data'")
  rules = native.existing_rules()
  _get_x_data(
    name = name,
    srcs = [rule['name'] for rule in rules.values() if rule['kind'] in (
      'go_binary', 'go_library', 'go_test', 'go_transition_binary', 'go_transition_test')],
    testonly = 1,
    visibility = ["//pkg:__pkg__"],
  )

def _unused_rule_impl(ctx):
  accum = depset(transitive = [src[_XInfo].archives for src in ctx.attr.srcs])
  data = """# This is boilerplate taken directly from
#  https://github.com/bazelbuild/bazel/blob/master/tools/bash/runfiles/runfiles.bash
# See that page for an explanation of what this is and why it's necessary.
# --- begin runfiles.bash initialization v2 ---
# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
   source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" |cut -f2- -d' ')" 2>/dev/null || \
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

# Need to run this so that Go can find the runfiles.
runfiles_export_envvars
$unused_checker_bin """ + " ".join([f.path.replace("bazel-out", "_bazel/out") for f in accum.to_list() if '/external/' not in f.path])
  f = ctx.actions.declare_file("unused_checker.sh")
  ctx.actions.write(f, data)
  return [
     DefaultInfo(executable = f),
   ]

_unused_rule = rule(
  implementation = _unused_rule_impl,
  executable = True,
  attrs = {
    'srcs' : attr.label_list(providers = [_XInfo]),
  },
)

def unused_checker(srcs):
  _unused_rule(
    name = "unused_script",
    srcs = srcs,
    testonly = 1,
  )
  native.sh_binary(
    name = "unused_checker",
    data = ["//build/bazelutil/unused_checker"],
    deps = ["@bazel_tools//tools/bash/runfiles"],
    srcs = [":unused_script"],
    tags = ["local"],
    testonly = 1,
  )
