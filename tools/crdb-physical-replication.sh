#!/bin/bash

# Intended to automate https://docs.google.com/document/d/1vTX6N72J0yL2dDVdyJg063gC761QdZcMQ3ADthFYi8g/edit

bold=$(tput bold)
normal=$(tput sgr0)

echo "${bold}CockroachDB Physical Replication setup${normal}"
echo ""
echo "This script will assist you in running a demo of Physical Replication on your local machine"
echo ""

echo "Checking prerequisites..."
versionstr=$(cockroach version 2>&1)

if [[ $versionstr == *"command not found"* ]]; then
    echo "❌ We did not find CockroachDB installed"
    echo "Please begin by installing CockroachDB: https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html"
    echo "Re-run this script after you have done so"
    exit 2
fi

regexmajor=(v[0-9]+\.[0-9]+)
regexminor=(v[0-9]+\.[0-9]+\.[0-9]+)

if [[ $versionstr =~ $regexmajor ]];
then
    majorversion=${BASH_REMATCH[0]}
fi

if [[ $versionstr =~ $regexminor ]];
then
    minorversion=${BASH_REMATCH[0]}
fi

if [ ! -z "$minorversion" ];
then
    echo "✅ We found CockroachDB version $minorversion"
fi

minversion="v23.1"
if [[ $majorversion < $minversion ]];
then
    echo "❌ You will need to install CockroachDB version $minversion or greater: https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html"
    echo ""
    echo "Exiting this script. Please re-run it after you have installed CockroachDB."
    exit 2
fi

echo ""
echo "We will be setting up two independent CockroachDB clusters, a Primary and a Standby. The Primary will stream changes to the Standby."

echo ""
echo "You will need two terminal windows open at the same directory."
echo ""
echo "One will represent the Primary cluster (we recommend putting it on the left side of your screen)"
echo ""
echo "The other will represent the Standby cluster (we recommend putting it on the right side of your screen)"
echo ""


while [[ $ps != "P" && $ps != "p" && $ps != "S" && $ps != "s" ]];
do
    read -p "Would you like to use this terminal for the Primary or for the Standby (enter P or S)? " ps
done
echo ""

function finish {
    # clear out window title
    echo -n -e "\033]0;\007"
}
trap finish EXIT

### Primary branch ###

if [[ $ps == "P" || $ps == "p" ]];
then
    echo "${bold}Configuring Primary cluster...${normal}"
    echo ""

    # set window title. maybe mac only?
    echo -n -e "\033]0;Primary cluster, system tenant\007"

    echo "Configuring certificates... TODO"
    # TODO check for existence of certs, remember the paths, prompt for creation as necessary, use good defaults
    echo ""

    echo "Starting Primary cluster... TODO"
    # TODO start cockroach --config-profile replication-source
    echo ""

    echo "Creating replication user... TODO"
    # TODO create replication user & grant
    # pwgen is not on all systems, is there a better choice?
    # https://unix.stackexchange.com/questions/230673/how-to-generate-a-random-string
    echo ""

    echo "Remember this connection string for use by the Standby cluster: TODO"
    # TODO echo the connection URL for use by the Standby
    echo ""

    echo "Starting workload... TODO"
    # TODO start workload
    echo ""

    echo "Your Standby cluster is running"
    echo ""
    echo "You can observe the workload in the DB Console"
    echo ""

    # TODO Tell the user it's running with instructions on how to interact (SHOW DATABASES, DB Console)

    read -p "Type c to connect to a SQL shell for the Standby cluster, or any other key to exit: " connect

    if [[ $connect == "c" ]];
    then
        echo "Connecting to Standby cluster... TODO"
        # TODO dump user into cockroach sql --certs-dir=certs --url 'postgresql://root@localhost:9001/defaultdb?options=-ccluster=system&sslmode=verify-full'   
    fi

    # TODO optionally dump user into cockroach sql --certs-dir=certs --url 'postgresql://root@localhost:9001/defaultdb?options=-ccluster=system&sslmode=verify-full'   

    exit
fi




### Standby branch ###

if [[ $ps == "S" || $ps == "s" ]];
then
    echo "${bold}Configuring Standby cluster...${normal}"
    echo ""

    # set window title. maybe mac only?
    echo -n -e "\033]0;Standby cluster, system tenant\007"

    echo "Configuring certificates... TODO"
    # TODO check for existence of certs, remember the paths, prompt for creation as necessary, use good defaults
    echo ""

    echo "Starting Standby cluster... TODO"
    # TODO start cockroach --config-profile replication-target
    echo ""

    echo "Starting phsyical replication... TODO"
    # TODO start the replication (CREATE TENANT)
    echo ""

    echo "Your Standby cluster is replicating from your Primary"
    echo ""

    echo "You can monitor with the SQL command SHOW TENANT application WITH REPLICATION STATUS"
    echo ""

    echo "Please look for the Physical Replication tab in the DB Console"
    echo ""

    # TODO echo connection string

    read -p "Type c to connect to a SQL shell for the Standby cluster, or any other key to exit: " connect

    if [[ $connect == "c" ]];
    then
        echo "Connecting to Standby cluster... TODO"
        # TODO dump user into cockroach sql --certs-dir=certs --url 'postgresql://root@localhost:9001/defaultdb?options=-ccluster=system&sslmode=verify-full'   
    fi

    exit
fi 

read
