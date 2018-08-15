#!/usr/bin/env bash
set -euo pipefail

# A POSIX variable
# Reset in case getopts has been used previously in the shell.
OPTIND=1

binary=""
version=""
os=""
arch="amd64" # TODO(tschottdorf): how to auto-detect?
dir="$(pwd)"
component=""
suffix=""
url=""

function errcho() { cat <<< "$@" 1>&2; }

case "${OSTYPE}" in
    darwin*)
        os="darwin"
        ;;
    linux*)
        os="linux"
        ;;
    cygwin*)
        os="windows"
        ;;
    *)
        errcho "Unknown \$OSTYPE of ${OSTYPE}"
        exit 1
        ;;
esac

function show_help() {
    cat<<EOF
$0 [-o <os>] [-a <arch>] <binary> <version>

<version> is either a CockroachDB release version or LATEST (for the latest bleeding edge).
When <binary> is not "cockroach", only LATEST or a git commit SHA are valid.

Examples:
    $0 -o linux -a amd64 cockroach v1.1.3
    $0 -o windows cockroach v1.1.3
    $0 -o linux loadgen/kv LATEST
EOF
}

OPTARG=""
while getopts "h?o:a:d:c:s:u:" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    o)  os=$OPTARG
        ;;
    a)  arch=$OPTARG
        ;;
    d)  dir=$OPTARG
        ;;
    c)  component=$OPTARG
        ;;
    s)  suffix=$OPTARG
        ;;
    u)  url=$OPTARG
        ;;
    *)  errcho "Unknown option. This is a bug."
        exit 1
        ;;
    esac
done

shift $((OPTIND-1))
[ "${1-}" = "--" ] && shift

if [ $# -gt 1 ]; then
    binary=${1}
    shift
fi

if [ $# -gt 0 ]; then
    version=${1}
    shift
fi

if [ -z "${binary}" ];
then
    errcho "No binary specified."
    show_help
    exit 1
fi

if [ -z "${version}" ]; then
    errcho "No version specified."
    show_help
    exit 1
fi


if [ -z "${dir}" ]; then dir=$(mktemp -d); fi
if [ -z "${os}" ]; then echo "Unable to detect OS. Please use '-o'."; fi
if [ -z "${arch}" ]; then echo "Unable to detect architecture. Please use '-a'."; fi

if [ -z "${component}" ] && [ ! -z "${binary}" ]; then
    if [[ "${binary}" == "cockroach" ]]; then
        component="cockroach"
    else
        component=$(dirname "${binary}")
        binary=$(basename "${binary}")
    fi
fi

autosuffix=""
case "${os}" in
    darwin)
        autosuffix=".tgz"
        ;;
    linux)
        autosuffix=".tgz"
        ;;
    windows)
        autosuffix=".zip"
        ;;
    *)
        echo "Unsupported OS: ${os}."
        exit 1
        ;;
esac


if [ -z "${url}" ]; then
    case "${version}" in
        v*)
            if [ "${binary}" != "cockroach" ]; then
                echo "Invalid binary ${binary} for version ${version}"
                exit 1
            fi
            urlos="${os}"
            case "${os}" in
                darwin)
                    urlos="${urlos}-10.9"
                    ;;
                windows)
                    urlos="${urlos}-6.2"
                    ;;
            esac
            if [ -z "${suffix}" ]; then
                suffix="${autosuffix}"
            fi

            url="https://binaries.cockroachdb.com/${binary}-${version}.${urlos}-${arch}${suffix}"
            ;;
        *)
            urlos="${os}"
            base=""
            if [ "${binary}" == "cockroach" ]; then
                if [ "${os}" == "linux" ]; then
                    urlos="${urlos}-gnu"
                fi
                base="${binary}.${urlos}-${arch}.${version}"
            else
                if [ "${os}" != "linux" ]; then
                    errcho "${binary} version ${version} is not available for ${os}"
                    exit 1
                fi
                base="${binary}.${version}"
            fi
            url="https://edge-binaries.cockroachdb.com/${component}/${base}"
            ;;
    esac
    url="${url}?binfetcher=true"
fi
echo "${url}"