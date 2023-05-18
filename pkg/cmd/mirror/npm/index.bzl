load("@aspect_rules_js//js:defs.bzl", "js_run_binary")

def _filename_from_label(label):
    """Given a label representing a filename like //foo/bar:baz,
       return the filename (`baz` in this case). This is a simple
       helper function that simply returns the text after the last colon.
    """
    return label[label.rfind(":")+1:]

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
            _filename_from_label(yarn_lock),
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
            _filename_from_label(json),
        ]
    )

