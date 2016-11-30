#!/usr/bin/env bash
set -e

# set up environment
export PATH=$PATH:/usr/lib/jvm/java-1.8-openjdk/bin
export JAVA_ROOT=/go/src/github.com/cockroachdb/cockroach/acceptance/java
export CLASSPATH=$CLASSPATH:$JAVA_ROOT/success:$JAVA_ROOT/fail:$JAVA_ROOT

cd $JAVA_ROOT

# See: https://basildoncoder.com/blog/postgresql-jdbc-client-certificates.html
openssl pkcs8 -topk8 -inform PEM -outform DER -in /certs/node.key -out key.pk8 -nocrypt

# compile and run java files
case $1 in
SUCCESS)
    expected=SUCCESS
    test_cases_dir=success
   ;;
FAILURE)
    expected=FAIL
    test_cases_dir=fail
   ;;
esac

javac TestRunner.java
java TestRunner $expected $JAVA_ROOT/$test_cases_dir

./run_cleanup.sh
