#!/bin/bash

# Intended to automate https://docs.google.com/document/d/1vTX6N72J0yL2dDVdyJg063gC761QdZcMQ3ADthFYi8g/edit

bold=$(printf "\033[1m")
normal=$(printf "\033[m")

echo "${bold}CockroachDB Physical Replication setup${normal}"
echo ""

echo "Checking prerequisites..."

if ! version_str=$(cockroach version --build-tag 2>/dev/null); then
    echo "❌ We did not find CockroachDB installed"
    echo "Please begin by installing CockroachDB: https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html"
    echo "Re-run this script after you have done so"
    exit 2
fi

version_str=${version_str#v}

echo "✅ We found CockroachDB version $version_str"

majorversion=${version_str%%.*}
# Uncomment the following if we ever need to check the minor version. For now, we don't.
# version_str=${version_str#${major}.}
# minor=${version_str%%.*}

minversion=23
if [ $majorversion -lt $minversion ]; then
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

function setwindowtitle () {
    # maybe mac only?
    echo -n -e "\033]0;$1\007"
}

function finish {
    # clear out window title
    setwindowtitle ""
}
trap finish EXIT

### Primary branch ###

if [[ $ps == "P" || $ps == "p" ]];
then
    echo "${bold}Configuring Primary cluster...${normal}"
    echo ""

    setwindowtitle "Primary cluster, system tenant"
    
    echo "${bold}Configuring certificates and CA...${normal}"
    echo ""
    
    primarycertsdir=$PWD/cockroach-test-clusters/primary/certs
    if [ ! -d $primarycertsdir ];
    then
        echo "Creating certs directory at $primarycertsdir"    
        mkdir -p $primarycertsdir
    fi

    primaryCApath="$primarycertsdir/ca.key"

    # Look for existing CA
    foundcount=0
    for filename in "ca.crt" "ca.key";
    do
        if [ -f $primarycertsdir/$filename ];
        then
            ((foundcount++))
        fi
    done

    if [[ $foundcount -eq 2 ]]; # all there
    then
        echo "Found certificate authority files in $primarycertsdir, using those."
        echo ""
    else   
        echo "Creating certificate authority..."
        cockroach cert create-ca --ca-key $primaryCApath --certs-dir $primarycertsdir
    fi


    # Look for existing certs
    foundcount=0
    for filename in "client.root.crt" "client.root.key" "node.crt" "node.key";
    do
        if [ -f $primarycertsdir/$filename ];
        then
            ((foundcount++))
        fi
    done

    if [[ $foundcount -eq 4 ]]; # all there
    then
        read -p "Found existing certs in $primarycertsdir. Use those? (y=yes, n=you will need to delete them) " yn
        if [ $yn != "y" ];
        then
            echo "You will need to delete or move the directory $primarycertsdir manually"
            echo "After that, you can re-run this script"
            exit
        fi
    fi

    if [[ $foundcount -eq 0 ]];
    then
        echo "Creating node certificates..."
        cockroach cert create-node localhost 127.0.0.1 "$(hostname -f)" ::1 localhost6 --certs-dir=$primarycertsdir --ca-key=$primaryCApath
        echo "Creating client certificates..."
        cockroach cert create-client root --certs-dir $primarycertsdir --ca-key $primaryCApath
    fi
 
    echo ""

    listenaddr="localhost:9001"
    httpaddr="localhost:8081"

    echo "${bold}Starting Primary cluster (single node)...${normal}"
    echo ""
    cockroach start-single-node --certs-dir=$primarycertsdir --http-addr $httpaddr --listen-addr $listenaddr --config-profile replication-source --background
    echo ""
    echo "Your Standby cluster is running at $listenaddr, with the DB Console at $httpaddr"
    echo ""
    echo "This cluster will continue to run unless you terminate it manually"
    echo ""
 
    echo "Creating replication user..."
    primaryurl="postgresql://root@$listenaddr/?options=-ccluster=system&sslmode=verify-full"

    # pwgen is not on all systems, is there a better choice?
    # https://unix.stackexchange.com/questions/230673/how-to-generate-a-random-string
    primaryrepluserpass=$(openssl rand -base64 20)

    sql="CREATE USER replication WITH LOGIN PASSWORD '$primaryrepluserpass';"
    cockroach sql --execute "$sql" --url "$primaryurl" --certs-dir=$primarycertsdir

    echo "Setting password..."
    sql="ALTER USER replication WITH PASSWORD '$primaryrepluserpass';"
    cockroach sql --execute "$sql" --url "$primaryurl" --certs-dir=$primarycertsdir

    echo "Granting REPLICATION role..."
    cockroach sql --execute "GRANT SYSTEM REPLICATION TO replication;" --url "$primaryurl" --certs-dir=$primarycertsdir

    echo "Remember this connection string for use by the Standby cluster:"
    primaryCAcertbytes="TODO" # read and url encode the CA bytes
    urlforstandby="postgresql://replication:$primaryrepluserpass@$listenaddr/?options=-ccluster=system&sslmode=verify-full&sslinline=true&sslrootcert=$primaryCAcertbytes"
    echo $urlforstandby
    echo ""

    echo "Starting workload... TODO"
    # COCKROACH_CERTS_DIR=$primarycertsdir cockroach workload init bank "$primaryurl"
    # COCKROACH_CERTS_DIR=$primarycertsdir cockroach workload run bank "$primaryurl"
    # TODO how to specify certs directory for workload?
    echo ""

    # TODO Tell the user it's running with instructions on how to interact (SHOW DATABASES, DB Console)

    read -p "Type c to connect to a SQL shell for the Standby cluster, or any other key to exit: " connect

    if [[ $connect == "c" ]];
    then
        echo "Connecting to Standby cluster..."
        cockroach sql --certs-dir=$primarycertsdir --url "postgresql://root@$listenaddr/?options=-ccluster=system&sslmode=verify-full"
    fi

    exit
fi



### Standby branch ###

if [[ $ps == "S" || $ps == "s" ]];
then
    echo "${bold}Configuring Standby cluster...${normal}"
    echo ""

    setwindowtitle "Standby cluster, system tenant"

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
