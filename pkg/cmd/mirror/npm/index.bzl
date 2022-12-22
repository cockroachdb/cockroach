load("@build_bazel_rules_nodejs//:index.bzl", "js_library", "npm_package_bin", "nodejs_binary")

def yarn_lock_to_json(name, yarn_lock, **kwargs):
    """Runs ./parse.js on the provided yarn_lock file,
       storing the produced output in "__${name}".
    """
    npm_package_bin(
        name = name,
        tool = ":yarn-lock-parse",
        stdout = "__" + name,
        data = [
            yarn_lock,
        ],
        args = [
            "$(execpath {})".format(yarn_lock),
        ],
    )

def json_to_yarn_lock(name, json, **kwargs):
    """Runs ./stringify.js on the provided json file, storing
       the produced output in "__${name}"
    """
    npm_package_bin(
        name = name,
        tool = ":yarn-lock-stringify",
        stdout = "__" + name,
        data = [
            json,
        ],
        args = [
            "$(execpath {})".format(json)
        ]
    )

