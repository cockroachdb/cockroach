# Generating the code for `sort_partitioner.eg.go` requires special handling
# because the template lives in a subpackage.
def gen_sort_partitioner_rule(name, target, visibility = ["//visibility:private"]):
    native.genrule(
        name = name,
        srcs = ["//pkg/sql/colexec/colexecbase:distinct_tmpl.go"],
        outs = [target],
	tags = ["no-remote-exec"],
        cmd = """\
GO_REL_PATH=`dirname $(location @go_sdk//:bin/go)`
GO_ABS_PATH=`cd $$GO_REL_PATH && pwd`
# Set GOPATH to something to workaround https://github.com/golang/go/issues/43938
export PATH=$$GO_ABS_PATH:$$PATH
export HOME=$(GENDIR)
export GOPATH=/nonexist-gopath
export COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=true
export GOROOT=
$(location :execgen) -template $(SRCS) -fmt=false pkg/sql/colexec/$@ > $@
$(location :goimports) -w $@
""",
        tools = [
            "@go_sdk//:bin/go",
            ":execgen",
            ":goimports",
        ],
        visibility = [":__pkg__", "//pkg/gen:__pkg__"],
    )

# Generating the code for `default_cmp_proj_const_op.eg.go` requires special
# handling because the template lives in a different package.
def gen_default_cmp_proj_const_rule(name, target, visibility = ["//visibility:private"]):
    native.genrule(
        name = name,
        srcs = ["//pkg/sql/colexec/colexecproj:default_cmp_proj_ops_tmpl.go"],
        outs = [target],
        cmd = """\
GO_REL_PATH=`dirname $(location @go_sdk//:bin/go)`
GO_ABS_PATH=`cd $$GO_REL_PATH && pwd`
# Set GOPATH to something to workaround https://github.com/golang/go/issues/43938
export PATH=$$GO_ABS_PATH:$$PATH
export HOME=$(GENDIR)
export GOPATH=/nonexist-gopath
export COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=true
$(location :execgen) -template $(SRCS) -fmt=false pkg/sql/colexec/colexecprojconst/$@ > $@
$(location :goimports) -w $@
""",
        tools = [
            "@go_sdk//:bin/go",
            ":execgen",
            ":goimports",
        ],
        visibility = [":__pkg__", "//pkg/gen:__pkg__"],
    )
