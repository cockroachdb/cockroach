def generate(name, template, output, misc = []):
    # `$@` lets us substitute in the output path[1].
    #
    # [1]. https://docs.bazel.build/versions/3.7.0/be/general.html#genrule_args
    native.genrule(
        name = name,
        srcs = [template],
        outs = [output],
        # The symlink below is frowned upon for genrules[1]. That said, when
        # testing pkg/sql/colexec through bazel it expects to find the template
        # files in a path other than what SRCS would suggest. We haven't really
        # investigated why. For now lets just symlink the relevant files into
        # the "right" path within the bazel sandbox[2].
        #
        # [1]: https://docs.bazel.build/versions/3.7.0/be/general.html#general-advice
        # [2]: https://github.com/cockroachdb/cockroach/pull/57027
        cmd = """
          ln -s external/cockroach/pkg pkg
          $(location :execgen) -template $(SRCS) \
              -fmt=false pkg/sql/colexec/$@ > $@
          $(location :goimports) -w $@
          """,
        tools = [":execgen", ":goimports"], # Aliases defined in BUILD.bazel
    )

# TODO(irfansharif): We should be able to use `generate` to generate
# like_ops.eg.go. It's special-cased here because execgen reads two separate
# template files[1] when generating like_ops.eg.go.
def generate_like_ops(name, template, output, misc = []):
    templates = [template]
    templates.extend(misc)

    native.genrule(
        name = name,
        srcs = templates,
        outs = [output],
        # See TODO above. We should ideally be using $(SRCS) to point to the
        # template file, but like_ops.eg.go needs access to two template files.
        # We point to the first, which is the template the generator is
        # registered with, but we also need to include the other in our srcs so
        # it's included in the sandbox when generating the file (-template only
        # expects one argument).
        cmd = """
          ln -s external/cockroach/pkg pkg
          $(location :execgen) -template $(location %s) \
              -fmt=false pkg/sql/colexec/$@ > $@
          $(location :goimports) -w $@
          """ % (templates[0]),
        tools = [":execgen", ":goimports"],
    )
