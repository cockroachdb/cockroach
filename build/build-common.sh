# Build utility functions.

function check_static() {
    local libs=$(ldd $1)
    if [ -n "${libs}" ]; then
        echo "$1 is not properly statically linked"
        ldd $1
        exit 1
    fi
}
