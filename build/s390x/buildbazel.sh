#!/bin/bash
# © Copyright IBM Corporation 2024
#
# Use of this source code is governed by the Apache License, Version 2.0,
# which can be found in licenses/APACHE-2.0-github.com-linux-on-ibm-z-scripts.txt.
#
# Portions of this file are additionally subject to the following
# license and copyright.
#
# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
#
# This code originated in https://github.com/linux-on-ibm-z/scripts/blob/master/CockroachDB/24.1.6/build_crdb.sh, SHA d2d885b3b9edae9d0e69383c18475485149875b1

# Instructions:
# Execute build script: bash build-bazel-s390x.sh    (provide -h for help)
set -e  -o pipefail

PACKAGE_NAME="bazel"
PACKAGE_VERSION="7.6.0"
NETTY_TCNATIVE_VERSION="2.0.61"
NETTY_VERSION="4.1.93"
SOURCE_ROOT="$(pwd)"
USER="$(whoami)"

FORCE="false"
TESTS="false"
LOG_FILE="${SOURCE_ROOT}/logs/${PACKAGE_NAME}-${PACKAGE_VERSION}-$(date +"%F-%T").log"

trap cleanup 0 1 2 ERR

#Check if directory exists
if [ ! -d "$SOURCE_ROOT/logs/" ]; then
    mkdir -p "$SOURCE_ROOT/logs/"
fi

if [ -f "/etc/os-release" ]; then
    source "/etc/os-release"
fi

function error() { echo "Error: ${*}"; exit 1; }

function prepare() {
    if command -v "sudo" >/dev/null; then
        printf -- 'Sudo : Yes\n' >>"$LOG_FILE"
    else
        printf -- 'Sudo : No \n' >>"$LOG_FILE"
        printf -- 'Install sudo from repository using apt, yum or zypper based on your distro. \n'
        exit 1
    fi

    if [[ "$FORCE" == "true" ]]; then
        printf -- 'Force attribute provided hence continuing with install without confirmation message\n' |& tee -a "$LOG_FILE"
    else
        # Ask user for prerequisite installation
        printf -- "\nAs part of the installation, dependencies would be installed/upgraded. \n"
        while true; do
            read -r -p "Do you want to continue (y/n) ? :  " yn
            case $yn in
            [Yy]*)
                printf -- 'User responded with Yes. \n' >> "$LOG_FILE"
                break
                ;;
            [Nn]*) exit ;;
            *) echo "Please provide confirmation to proceed." ;;
            esac
        done
    fi
}

function cleanup() {
    # Remove artifacts
    rm -rf $SOURCE_ROOT/netty
    rm -rf $SOURCE_ROOT/netty-tcnative

    printf -- "Cleaned up the artifacts\n" | tee -a "$LOG_FILE"
}

function buildNetty() {
    # Install netty-tcnative 2.0.61
    wget -q https://raw.githubusercontent.com/linux-on-ibm-z/scripts/master/netty-tcnative/2.0.61/build_netty.sh
    if [[ $DISTRO =~ ^ubuntu-24\.(04|10)$ ]]; then
            sed -i "s/ubuntu-22.04/$DISTRO/g" build_netty.sh
    fi
    bash build_netty.sh -y
    export LD_LIBRARY_PATH=$SOURCE_ROOT/netty-tcnative/openssl-dynamic/target/native-build/.libs/:$LD_LIBRARY_PATH

    printf -- 'Set JAVA_HOME\n'
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-s390x
    export PATH=$JAVA_HOME/bin:$PATH
    printf -- 'JAVA version: \n'
    java -version

    # Install netty 4.1.93 Final
    printf -- '\nBuild netty 4.1.93 from source... \n'
    cd $SOURCE_ROOT
    git clone -b netty-$NETTY_VERSION.Final https://github.com/netty/netty.git
    cd netty
    mvn clean install -DskipTests
}

function configureAndInstall() {
    printf -- 'Configuration and Installation started \n'

    buildNetty

    printf -- 'Set JAVA_HOME\n'
    export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-s390x
    export PATH=$JAVA_HOME/bin:$PATH
    printf -- 'JAVA version is: \n'
    java -version

    # Download Bazel distribution archive
    printf -- '\nDownload Bazel ${PACKAGE_VERSION} distribution archive... \n'
    cd $SOURCE_ROOT
    wget https://github.com/bazelbuild/bazel/releases/download/$PACKAGE_VERSION/bazel-$PACKAGE_VERSION-dist.zip
    mkdir -p dist/bazel && cd dist/bazel
    unzip -q ../../bazel-$PACKAGE_VERSION-dist.zip
    chmod -R +w .

    printf -- '\nBuild the bootstrap Bazel binary... \n'
    env EXTRA_BAZEL_ARGS="--tool_java_runtime_version=local_jdk" bash ./compile.sh

    printf -- '\nCheckout and patch the Bazel source... \n'
    cd $SOURCE_ROOT
    git clone -b crl-release-7.6.0-s390x https://github.com/cockroachdb/bazel

    #Copy netty and netty-tcnative jar to respective bazel directory
    cp $SOURCE_ROOT/netty-tcnative/openssl-classes/target/netty-tcnative-classes-$NETTY_TCNATIVE_VERSION.Final.jar \
       $SOURCE_ROOT/netty-tcnative/boringssl-static/target/netty-tcnative-boringssl-static-$NETTY_TCNATIVE_VERSION.Final-linux-s390_64.jar \
       $SOURCE_ROOT/netty/buffer/target/netty-buffer-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/codec/target/netty-codec-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/codec-http/target/netty-codec-http-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/codec-http2/target/netty-codec-http2-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/common/target/netty-common-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/handler/target/netty-handler-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/handler-proxy/target/netty-handler-proxy-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/resolver/target/netty-resolver-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/resolver-dns/target/netty-resolver-dns-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/transport/target/netty-transport-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/transport-classes-epoll/target/netty-transport-classes-epoll-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/transport-classes-kqueue/target/netty-transport-classes-kqueue-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/transport-native-unix-common/target/netty-transport-native-unix-common-$NETTY_VERSION.Final-linux-s390_64.jar \
       $SOURCE_ROOT/netty/transport-native-kqueue/target/netty-transport-native-kqueue-$NETTY_VERSION.Final.jar \
       $SOURCE_ROOT/netty/transport-native-epoll/target/netty-transport-native-epoll-$NETTY_VERSION.Final-linux-s390_64.jar \
       $SOURCE_ROOT/bazel/third_party

    printf -- '\nBuild Bazel from source... \n'
    cd $SOURCE_ROOT/bazel
    ${SOURCE_ROOT}/dist/bazel/output/bazel build --enable_bzlmod --lockfile_mode=update -c opt --stamp --embed_label "$PACKAGE_VERSION" //src:bazel //src:bazel_jdk_minimal //src:test_repos //src/main/java/...
    mkdir -p output
    cp bazel-bin/src/bazel output/bazel

    # Run Tests
    runTest

    #Cleanup
    cleanup

    printf -- "\n Installation of %s %s was successful \n\n" $PACKAGE_NAME $PACKAGE_VERSION
}

function runTest() {
    set +e
    if [[ "$TESTS" == "true" ]]; then
        printf -- "TEST Flag is set, Continue with running test \n"

        cd $SOURCE_ROOT/bazel
        ./output/bazel --host_jvm_args=-Xmx2g test --enable_bzlmod --lockfile_mode=update -c opt --build_tests_only --flaky_test_attempts=1 --test_timeout=3600 --show_progress_rate_limit=5 --terminal_columns=143 --show_timestamps --verbose_failures \
-- //scripts/... //src/java_tools/... //src/test/... //src/tools/execlog/... //src/tools/singlejar/... //src/tools/workspacelog/... //third_party/ijar/... -//tools/android/... //tools/aquery_differ/... //tools/python/... \
-//src/java_tools/import_deps_checker/... -//src/test/shell/bazel/android/... -//src/test/java/com/google/devtools/build/android/... -//src/test/shell/bazel:bazel_determinism_test -//src/test/shell/bazel:bazel_android_tools_test
        printf -- "Tests completed. \n\n"
        printf -- "If you see an unexpected test case failure, you could rerun it the following command:\n\n"
        printf -- "  bazel test --enable_bzlmod --lockfile_mode=update -c opt --build_tests_only --flaky_test_attempts=3 --test_timeout=3600 --show_progress_rate_limit=5 --show_timestamps --verbose_failures -- //src/<module_name>:<testcase_name>\n\n"
        printf -- "For example:\n"
        printf -- "  bazel test --enable_bzlmod --lockfile_mode=update -c opt --build_tests_only --flaky_test_attempts=3 --test_timeout=3600 --show_progress_rate_limit=5 --show_timestamps --verbose_failures -- //src/test/cpp/util:md5_test\n"
    fi
    set -e
}

function logDetails() {
    printf -- '**************************** SYSTEM DETAILS *************************************************************\n' >>"$LOG_FILE"
    if [ -f "/etc/os-release" ]; then
        cat "/etc/os-release" >>"$LOG_FILE"
    fi

    cat /proc/version >>"$LOG_FILE"
    printf -- '*********************************************************************************************************\n' >>"$LOG_FILE"

    printf -- "Detected %s \n" "$PRETTY_NAME"
    printf -- "Request details : PACKAGE NAME= %s , VERSION= %s \n" "$PACKAGE_NAME" "$PACKAGE_VERSION" |& tee -a "$LOG_FILE"
}

# Print the usage message
function printHelp() {
    echo
    echo "Usage: "
    echo "  bash build_bazel.sh  [-d debug] [-y install-without-confirmation] [-t install-with-tests]"
    echo
}

while getopts "h?dyt" opt; do
    case "$opt" in
    h | \?)
        printHelp
        exit 0
        ;;
    d)
        set -x
        ;;
    y)
        FORCE="true"
        ;;
    t)
        TESTS="true"
        ;;
    esac
done

function gettingStarted() {
    printf -- '\n***********************************************************************************************\n'
    printf -- "Getting Started: \n"
    printf -- "Make sure bazel binary is in your path\n"
    printf -- "export PATH=$SOURCE_ROOT/bazel/output:'$PATH'\n"
    printf -- "Check the version of Bazel, it should be something like the following:\n"
    printf -- "  $ bazel --version\n"
    printf -- "    bazel ${PACKAGE_VERSION}\n"
    printf -- "The bazel location should be something like the following:\n"
    printf -- "  $ which bazel\n"
    printf -- "    $SOURCE_ROOT/bazel/output/bazel\n"
}

###############################################################################################################

logDetails
prepare #Check Prequisites

DISTRO="$ID-$VERSION_ID"
case "$DISTRO" in

"ubuntu-20.04")
    printf -- "Installing %s %s for %s \n" "$PACKAGE_NAME" "$PACKAGE_VERSION" "$DISTRO" |& tee -a "$LOG_FILE"
    printf -- "Installing dependencies... it may take some time.\n"
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        bind9-host build-essential coreutils curl dnsutils ed expect file git gnupg2 iproute2 iputils-ping mkisofs \
        lcov less libssl-dev lsb-release netcat-openbsd openjdk-11-jdk-headless openjdk-21-jdk-headless zip zlib1g-dev unzip wget \
        python-is-python3 python3 python3-dev python3-pip python3-requests python3-setuptools \
        python3-six python3-wheel python3-yaml |& tee -a "${LOG_FILE}"
    configureAndInstall |& tee -a "${LOG_FILE}"
    ;;

"ubuntu-22.04" | "ubuntu-24.04" | "ubuntu-24.10")
    printf -- "Installing %s %s for %s \n" "$PACKAGE_NAME" "$PACKAGE_VERSION" "$DISTRO" |& tee -a "$LOG_FILE"
    printf -- "Installing dependencies... it may take some time.\n"
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        bind9-host build-essential coreutils curl dnsutils ed expect file git gnupg2 iproute2 iputils-ping mkisofs \
        lcov less libssl-dev lsb-release netcat-openbsd openjdk-11-jdk-headless openjdk-21-jdk-headless zip zlib1g-dev unzip wget python3 |& tee -a "${LOG_FILE}"

    sudo ln -sf /usr/bin/python3 /usr/bin/python

    configureAndInstall |& tee -a "${LOG_FILE}"
    ;;

*)
    printf -- "%s not supported \n" "$DISTRO" |& tee -a "$LOG_FILE"
    exit 1
    ;;
esac

gettingStarted |& tee -a "${LOG_FILE}"
