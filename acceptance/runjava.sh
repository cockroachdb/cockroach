#! /usr/bin/env bash
set -ex

cd acceptance/

while test $# > 1; do
    template=$1
    substitution=$2
    output=$3
    sed -e "$substitution" <"$template" >"$output"
    shift 3
done

# See: https://basildoncoder.com/blog/postgresql-jdbc-client-certificates.html
openssl pkcs8 -topk8 -inform PEM -outform DER -in /certs/node.client.key -out key.pk8 -nocrypt

export PATH=$PATH:/usr/lib/jvm/java-1.7-openjdk/bin

javac main.java TestConnection.java TestStatements.java
java -cp /postgres.jar:. main
