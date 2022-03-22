# This file works in concert with the binary in genbzl. That tool will utilize
# bazel query to generate lists of generated file targets. These lists are
# loaded below and then utilized in exported macros from here for use in
# BUILD.bazel. That tool is invoked during the bazel generation which occurs
# in build/bazelutil/bazel-generate.sh which itself is invoked by
# ./dev generate bazel.
#
# Note that there's a circularity to the definitions which is relied upon to
# create the MISC_SRCS target. The genbzl tool will reference targets which
# capture all of the EXPLICIT_SRCS and the EXCLUDED_SRCS and subtract them from
# all of the generated files in the repo in order to compute the MISC_SRCS
# list.
#
# Most of these lists of generated files utilize genrule or something derived
# from genrule, and thus the mapping within the sandbox to the files is
# straightforward. The exception is go_proto_library which hides its
# generated artifacts behind a few layers of indirection. See
# _go_proto_srcs which deals with properly sussing out the prefix for those
# generated go files.

load("@io_bazel_rules_go//go:def.bzl", "GoSource")
load(":bindata.bzl", "BINDATA_SRCS")
load(":docs.bzl", "DOCS_SRCS")
load(":execgen.bzl", "EXECGEN_SRCS")
load(":gomock.bzl", "GOMOCK_SRCS")
load(":misc.bzl", "MISC_SRCS")
load(":optgen.bzl", "OPTGEN_SRCS")
load(":protobuf.bzl", "PROTOBUF_SRCS")
load(":stringer.bzl", "STRINGER_SRCS")

# GeneratedFileInfo provides two pieces of information to the _hoist_files
# rule. It provides the set of files to be hoisted via the generated_files
# field and it provides a list of commands to run to clean up potentially
# stale generated files. The reason to couple these is so that various rules
# and invocations of _hoist_files can compose and the end result will properly
# clean and hoist those files.
#
# Note that the layout of generated_files is a dict where the key is a prefix
# to trim from the paths in the list of strings that are the values when
# hoisting back into the workspace. This exists primarily to deal with the
# _go_proto_srcs rule and go_proto_library which emit their sources into a
# path in the sandbox which is not parallel to its path in the repo.
_GeneratedFileInfo = provider(
  "Info needed to hoist generated files",
  fields = {
    "generated_files": "dictionary from prefix to list of files",
    "cleanup_tasks": "list of bash commands to run"
  }
)

# This is a useful helper for creating cleanup commands which operate within
# the workspace root.
def _subshell_in_workspace_snippet(cmds=[]):
  return """\
# Use a subshell with () to avoid changing the directory in the main shell.
(
    cd "${{BUILD_WORKSPACE_DIRECTORY}}"
    {}
)
""".format("\n    ".join(cmds))

# Avoid searching the node_modules directory because it's full of
# irrelevant files.
_find_relevant = "find ./pkg -name node_modules -prune -o "

# This rule implementation takes PROTOBUF_SRCS, which expose the GoSource
# provider and map then into a _GeneratedFileInfo which tells _hoist_files
# how to locate the generated code within the sandbox. Compare this to
def _go_proto_srcs_impl(ctx):
  generated_files = {}
  for s in ctx.attr._srcs:
    srcs = s[GoSource]
    lbl = srcs.library.label
    imp = srcs.library.importpath
    imp = imp[:imp.find(lbl.package)]
    prefix = "{}/{}_/{}".format(lbl.package, lbl.name, imp)
    generated_files[prefix] = [f for f in srcs.srcs]

  return [
    _GeneratedFileInfo(
      generated_files = generated_files,
      # Create a task to remove any existing protobuf files.
      cleanup_tasks = [
        _subshell_in_workspace_snippet([
          _find_relevant + " -type f -name {} -exec rm {{}} +".format(suffix)
        ]) for suffix in ["*.pb.go", "*.pb.gw.go"]
      ],
    )
  ]

_go_proto_srcs = rule(
  implementation = _go_proto_srcs_impl,
  attrs = {
   "_srcs": attr.label_list(providers = [GoSource], default=PROTOBUF_SRCS),
  },
)

# This rule is the default rule to build construct the input to _hoist_files
# for srcs which have a path in the sandbox that is parallel to where those
# files should end up in the repo.
def _no_prefix_impl(ctx):
  files = [f for di in ctx.attr.srcs for f in di[DefaultInfo].files.to_list()]
  return [_GeneratedFileInfo(
    generated_files = {"": files},
    cleanup_tasks = ctx.attr.cleanup_tasks,
  )]

_no_prefix = rule(
  implementation = _no_prefix_impl,
  attrs = {
    "srcs": attr.label_list(allow_files=True),
    "cleanup_tasks": attr.string_list(),
  },
)

# This rule is responsible for generating an executable which can clean up old
# generated files and hoist new ones according to info in a _GeneratedFileInfo
# provider. Note that it also propagates the same _GeneratedFileInfo so that
# multiple _hoist_files targets can be combined into a larger _hoist_files
# target.
#
# The basic structure is that it creates a bash script which performs the
# cleanup tasks and then copies the files and sets their permissions.
#
# Note that this rule is not exported and is invoked through macros which
# obfuscate some of its structure. The go_proto and _hoist_no_prefix macros
# invoke this rule. The gen macro also invokes this rule with targets that
# were either generated with hard-coded invocations exported here or
# combinations thereof.
#
# TODO(ajwerner): If this script proves slow, we could rewrite it to depend
# on a go program which can perform the file IO in parallel.
def _hoist_files_impl(ctx):
  cp_file_cmd_fmt = """\
cp {src} {dst}
chmod 0644 {dst}\
"""

  script_fmt = """\
#!/bin/bash
set -euo pipefail

{cleanup_tasks}
{cmds}
"""
  cleanup_tasks = []
  cmds = []
  generated_files = {}
  for set in ctx.attr.data:
    gfi = set[_GeneratedFileInfo]
    cleanup_tasks += gfi.cleanup_tasks if hasattr(gfi, "cleanup_tasks") else []
    for prefix, files in gfi.generated_files.items():
      if prefix not in generated_files:
        generated_files[prefix] = []
      for file in files:
        dst = '"${{BUILD_WORKSPACE_DIRECTORY}}/{}"'.format(
          file.short_path[len(prefix):]
        )
        cmd = cp_file_cmd_fmt.format(src=file.short_path, dst=dst)
        cmds.append(cmd)
        generated_files[prefix].append(file)

  executable = ctx.actions.declare_file(ctx.label.name)
  script = script_fmt.format(
      cleanup_tasks = "\n".join(cleanup_tasks),
      cmds = "\n".join(cmds),
  )
  ctx.actions.write(executable, script, is_executable=True)
  runfiles = ctx.runfiles(files = [file for files in generated_files.values() for file in files])
  return [
    DefaultInfo(executable = executable, runfiles=runfiles),
    _GeneratedFileInfo(
      generated_files = generated_files,
      cleanup_tasks = cleanup_tasks,
    )
  ]

_hoist_files = rule(
    implementation = _hoist_files_impl,
    attrs = {
        "data": attr.label_list(providers = [_GeneratedFileInfo]),
    },
    executable = True,
)

def go_proto():
  _go_proto_srcs(name = "go_proto_srcs")
  _hoist_files(name = "go_proto", data = ["go_proto_srcs"], tags = ["no-remote-exec"])


# This macro is leveraged below by all of the macros corresponding to targets
# which don't need any special prefix handling (all but go_proto).
def _hoist_no_prefix(name, srcs, cleanup_tasks = []):
  srcs_name = name + "_srcs"
  _no_prefix(
    name = srcs_name,
    srcs = srcs,
    cleanup_tasks = cleanup_tasks,
  )
  _hoist_files(name = name, data = [srcs_name], tags = ["no-remote-exec"])

def gomock():
  _hoist_no_prefix(
    name = "gomock",
    srcs = GOMOCK_SRCS,
    cleanup_tasks = [
        _subshell_in_workspace_snippet([
          _find_relevant + "-type f -name '*.go' " +
            # Use this || true dance to avoid egrep failing
            # the whole script.
            "| { egrep '/mocks_generated(_test)?\\.go' || true ; }"+
            "| xargs rm ",
        ]),
      ],
  )

def execgen():
  _hoist_no_prefix(
    name = "execgen",
    srcs = EXECGEN_SRCS,
    cleanup_tasks = [
      _subshell_in_workspace_snippet([
        _find_relevant + "-type f -name '*.eg.go' -exec rm {} +",
      ])
    ]
  )

def stringer():
  _hoist_no_prefix(
    name = "stringer",
    srcs = STRINGER_SRCS,
  )

def optgen():
  _hoist_no_prefix(
    name = "optgen",
    srcs = OPTGEN_SRCS,
    cleanup_tasks = [
      _subshell_in_workspace_snippet([
        _find_relevant + "-type f -name '*.og.go'" +
          " ! -regex '.*lang/[^/].*\\.og\\.go$'" +
          " -exec rm {} +",
      ]),
    ],
  )

def misc():
  _hoist_no_prefix(
    name = "misc",
    srcs = MISC_SRCS,
  )

def docs():
  _hoist_no_prefix(
    name = "docs",
    srcs = DOCS_SRCS,
  )

def bindata():
  _hoist_no_prefix(
    name = "bindata",
    srcs = BINDATA_SRCS,
  )

def gen(name, srcs):
  _hoist_files(name = name, data = srcs, tags = ["no-remote-exec"])
