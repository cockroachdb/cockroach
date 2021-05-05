def gen_interval_btree(name, type, package):
    munged_type = type.lower().replace("*", "")
    src_out = munged_type + "_interval_btree.go"
    test_out = munged_type + "_interval_btree_test.go"
    native.genrule(
        name = name,
        srcs = ["@cockroach//pkg/util/interval/generic:gen_srcs"],
        outs = [src_out, test_out],
        tools = [
            "@com_github_cockroachdb_crlfmt//:crlfmt",
            "@com_github_mmatczuk_go_generics//cmd/go_generics",
        ],
        cmd = """
        export PATH=$$PATH:$$(dirname $(location @com_github_cockroachdb_crlfmt//:crlfmt)):$$(dirname $(location @com_github_mmatczuk_go_generics//cmd/go_generics))
        SCRIPT_LOC=$$(echo $(locations @cockroach//pkg/util/interval/generic:gen_srcs) | grep -o '[^ ]*\\.sh')
        $$SCRIPT_LOC {type} {package}
        mv {src_out} $(location {src_out})
        mv {test_out} $(location {test_out})
""".format(type=type, package=package, src_out=src_out, test_out=test_out)
    )
