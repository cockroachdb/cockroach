#! /usr/bin/env bash
set -ex

cd acceptance/java/

# See: https://basildoncoder.com/blog/postgresql-jdbc-client-certificates.html
openssl pkcs8 -topk8 -inform PEM -outform DER -in /certs/node.client.key -out key.pk8 -nocrypt

export PATH=$PATH:/usr/lib/jvm/java-1.7-openjdk/bin

cp -f "$1" TestConnection.java
cp -f "$2" TestStatements.java

javac main.java TestConnection.java TestStatements.java
java -cp /postgres.jar:. main
