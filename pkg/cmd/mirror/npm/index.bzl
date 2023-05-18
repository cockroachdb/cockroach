load("@aspect_rules_js//js:defs.bzl", "js_run_binary")

def yarn_lock_to_json(name, yarn_lock):
    """Runs ./parse.js on the provided yarn_lock file,
       storing the produced output in "__${name}".
    """
    js_run_binary(
        name = name,
        tool = ":yarn-lock-parse",
        chdir = native.package_name(),
        stdout = "__" + name,
        srcs = [yarn_lock],
        args = [
            yarn_lock[yarn_lock.rfind(":")+1:],
        ],
    )

def json_to_yarn_lock(name, json):
    """Runs ./stringify.js on the provided json file, storing
       the produced output in "__${name}"
    """
    js_run_binary(
        name = name,
        tool = ":yarn-lock-stringify",
        chdir = native.package_name(),
        stdout = "__" + name,
        srcs = [json],
        args = [
            json[json.rfind(":")+1:],
        ]
    )

