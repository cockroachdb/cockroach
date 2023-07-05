#!/bin/bash

# Intended to automate https://docs.google.com/document/d/1vTX6N72J0yL2dDVdyJg063gC761QdZcMQ3ADthFYi8g/edit

# Constants
readonly primary="primary"
readonly standby="standby"
readonly system="system"
readonly application="system"
readonly repluser='replication'

# Style stuff
readonly bold=$(printf "\033[1m")
readonly normal=$(printf "\033[m")
function setwindowtitle () {
    # maybe mac only?
    echo -n -e "\033]0;$1\007"
}
function finish {
    # clear out window title
    setwindowtitle ''
}
trap finish EXIT

# Main

echo "${bold}CockroachDB Physical Replication setup${normal}"
echo

echo "Checking prerequisites..."

if ! version_str=$(cockroach version --build-tag 2>/dev/null); then
    echo "❌ We did not find CockroachDB installed"
    echo "Please begin by installing CockroachDB: https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html"
    echo "Re-run this script (bash $0) after you have done so"
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
    echo "❌ You will need to install CockroachDB version v$minversion or greater: https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html"
    echo
    echo "Exiting this script. Please re-run it (bash $0) after you have installed CockroachDB."
    exit 2
fi

echo
echo "We will be setting up two independent CockroachDB clusters, a Primary and a Standby. The Primary will stream changes to the Standby."

echo
echo "You will need two terminal windows open at the same directory."
echo
echo "One will represent the Primary cluster (we recommend putting it on the left side of your screen)"
echo
echo "The other will represent the Standby cluster (we recommend putting it on the right side of your screen)"
echo

while [[ $psw != 'P' && $psw != 'p' && $psw != 'S' && $psw != 's' && $psw != 'W' && $psw != 'w' ]];
do
    read -p "Would you like to use this terminal to set up the Primary, the Standby, or a workload [enter P, S or W] " psw
done
echo

if [[ $psw == 'P' || $psw == 'p' || $psw == 'W' || $psw == 'w' ]];
then
    readonly cluster=$primary
fi

if [[ $psw == "S" || $psw == "s" ]];
then
    readonly cluster=$standby
fi

# Init

if [ $cluster == $primary ];
then
    readonly label='Primary'
    readonly configprofile='replication-source'
    readonly listenaddr='localhost:9001'
    readonly httpaddr='localhost:8081'
fi

if [ $cluster == $standby ];
then
    readonly label='Standby'
    readonly configprofile='replication-target'
    readonly listenaddr='localhost:9002'
    readonly httpaddr='localhost:8082'
fi

readonly basedir="$PWD/cockroach-test"
readonly certsdir="$basedir/$cluster/certs"
readonly systemconn="postgresql://root@$listenaddr/?options=-ccluster=$system&sslmode=verify-full"
readonly appconn="postgresql://root@$listenaddr/?options=-ccluster=$system&sslmode=verify-full"
readonly trysql='select 1;'

function workload {
    if [[ $(cockroach sql --certs-dir "$certsdir" --url "$systemconn" --execute "$trysql" 2>/dev/null) ]];
    then
        echo "✅ $label cluster appears to already be running at $listenaddr"
    else
        echo "❌ $label cluster appears not to be running at $listenaddr"
        echo "Re-run this script (bash $0) and choose P"
        exit
    fi
    echo

    setwindowtitle "Workload on $label cluster"

    echo "${bold}Starting workload...${normal}"
    readonly workloadconn="postgresql://root@$listenaddr/bank?options=-ccluster=$application&sslmode=verify-full&sslrootcert=$certsdir/ca.crt&sslcert=$certsdir/client.root.crt&sslkey=$certsdir/client.root.key"
    cockroach workload init bank "$workloadconn"
    cockroach workload run bank "$workloadconn"
}

if [[ $psw == "W" || $psw == "w" ]];
then
    workload
    exit
fi


function intro {
    echo "${bold}Configuring $label cluster...${normal}"
    echo

    setwindowtitle "$label cluster"
}

function certs {
    echo "${bold}Configuring certificates and CA...${normal}"
    echo
    
    if [ ! -d $certsdir ];
    then
        echo "Creating certs directory at $certsdir"    
        mkdir -p $certsdir
    fi

    # Look for existing CA
    readonly cakeypath="$certsdir/ca.key"
    readonly cacrtpath="$certsdir/ca.crt"

    local foundcount=0
    for f in $cakeypath $cacrtpath;
    do
        if [ -f $f ];
        then
            ((foundcount++))
        fi
    done

    if [[ $foundcount -eq 2 ]]; # all there
    then
        echo "Found certificate authority files in $certsdir, using those."
        echo
    else   
        echo "Creating certificate authority..."
        cockroach cert create-ca --ca-key "$cakeypath" --certs-dir "$certsdir"
    fi


    # Look for existing certs
    local foundcount=0
    for filename in "client.root.crt" "client.root.key" "node.crt" "node.key" "client.$repluser.crt" "client.$repluser.key";
    do
        if [ -f "$certsdir/$filename" ];
        then
            ((foundcount++))
        fi
    done

    if [[ $foundcount -eq 6 ]]; # all there
    then
        echo "Found existing certs in $certsdir, using those."
    else
        echo "Creating node certificates..."
        cockroach cert create-node localhost 127.0.0.1 "$(hostname -f)" ::1 localhost6 --certs-dir "$certsdir" --ca-key="$cakeypath"
        echo "Creating client certificates..."
        cockroach cert create-client root --certs-dir "$certsdir" --ca-key "$cakeypath"
        cockroach cert create-client replication --certs-dir "$certsdir" --ca-key "$cakeypath"
    fi

    echo
}

function server {
    echo "${bold}Starting $label cluster (single node)...${normal}"
    echo

    readonly datadir=$basedir/$cluster/data
    if [ ! -d "$datadir" ];
    then
        echo "Creating data directory at $datadir"    
        mkdir -p "$datadir"
    fi

    if [[ $(cockroach sql --certs-dir "$certsdir" --url "$systemconn" --execute "$trysql" 2>/dev/null) ]];
    then
        echo "$label cluster appears to already be running at $listenaddr"
        echo
    else
        cockroach start-single-node --certs-dir "$certsdir" --store=path="$datadir" --http-addr "$httpaddr" --listen-addr "$listenaddr" --config-profile "$configprofile" --background
        echo
        echo "Your $label cluster is running at $listenaddr, with the DB Console at $httpaddr."
        echo
        echo "This cluster will continue to run unless you terminate it manually."
        echo
    fi
}

function offersql {
    local ccluster

    echo "Optionally, in the current ($label) terminal, you can:"
    echo " • type 'a' to connect to a SQL shell for the $label cluster's $application tenant"
    echo " • type 's' to connect to a SQL shell for the $label cluster's $system tenant"
    echo " • type any other key to exit"
    local next
    read next
    echo

    if [[ $next == 'a' ]];
    then
        ccluster=$application
    elif [[ $next == 's' ]];
    then
        ccluster=$system
    fi

    if [ ! -z $ccluster ];
    then
        echo "Connecting to $label cluster's $ccluster tenant, using the following command..."
        cmd="cockroach sql --certs-dir '$certsdir' --url 'postgresql://root@$listenaddr/?options=-ccluster=$ccluster&sslmode=verify-full'"
        echo
        echo "$cmd"
        echo
        setwindowtitle "$label cluster, $ccluster tenant"
        eval "$cmd"
    else
        echo "${bold}Exiting${normal}"
        echo
        echo "If you wish to connect to the $label cluster's $application tenant, use:"
        cmd="cockroach sql --certs-dir '$certsdir' --url '$appconn'"
        echo $cmd
        echo

        echo "If you wish to connect to the $label cluster's $system tenant, use:"
        cmd="cockroach sql --certs-dir '$certsdir' --url '$systemconn'"
        echo $cmd
        echo
    fi
}

function outro {
    echo 'If you want to wipe everything away and start fresh:'
    echo " • Stop all cockroach processes with pkill 'cockroach'"
    echo " • Wait 10 seconds"
    echo " • rm -rf '$basedir'"
    echo " • Invoke this script again with bash $0"
}

function license {
    echo "${bold}Enabling replication on $label${normal}..."

    local sql='SHOW CLUSTER SETTING cluster.organization;'
    local org
    org=$(cockroach sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir" --format 'tsv' | tail -1 | xargs)
    
    if [ -z $org ];
    then
        read -p "What is the organization name for this cluster? " org
        sql="SET CLUSTER SETTING cluster.organization = '$org';"
        cockroach sql --execute "$sql" --certs-dir "$certsdir" --url "$systemconn"
    else
        echo "Found organization name $org"
    fi

    local sql='SHOW CLUSTER SETTING enterprise.license;'
    local lic
    lic=$(cockroach sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir" --format 'tsv' | tail -1 | xargs)
    if [ -z $lic ];
    then
        read -p 'What is the license key for this cluster? ' key
        sql="SET CLUSTER SETTING enterprise.license = '$key';"
        cockroach sql --execute "$sql" --certs-dir "$certsdir" --url "$systemconn"
    else
        echo 'Found enterprise license'
    fi

    local sql='SHOW CLUSTER SETTING cross_cluster_replication.enabled;'
    local enabled
    enabled=$(cockroach sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir" --format 'tsv' | tail -1 | xargs)

    if [[ $enabled != 't' ]];
    then
        echo 'Enabling physical replication...'
        sql='SET CLUSTER SETTING cross_cluster_replication.enabled = true;'
        cockroach sql --execute "$sql" --certs-dir "$certsdir" --url "$systemconn"
    else
        echo "Physical replication is enabled"
    fi

    if [ $cluster == $primary ];
    then
        sql='SHOW CLUSTER SETTING kv.rangefeed.enabled;'
        enabled=$(cockroach sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir" --format 'tsv' | tail -1 | xargs)

        if [[ $enabled != 't' ]];
        then
            echo 'Enabling rangefeeds...'
            sql='SET CLUSTER SETTING kv.rangefeed.enabled = true;'
            cockroach sql --execute "$sql" --certs-dir "$certsdir" --url "$systemconn"
        else
            echo "Rangefeeds enabled"
        fi

        echo "Checking replication user..."

        local sql="select username from [show users] where username='$repluser';"
        local user
        user=$(cockroach sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir" --format 'tsv' | tail -1 | xargs)
        
        if [ $user == $repluser ]; then
            echo "User '$repluser' already exists. Hopefully you have saved the password."
            echo
            echo "${bold}Connection string for later use by the Standby cluster:${normal}"
            local urlforstandby="postgresql://$repluser:<the password>@$listenaddr/?options=-ccluster=system&sslmode=verify-full&sslinline=true&sslrootcert=$cacrtpath"
            echo $urlforstandby
            echo
        else
            # pwgen is not on all systems, is there a better choice?
            # https://unix.stackexchange.com/questions/230673/how-to-generate-a-random-string
            pw=$(openssl rand -base64 20)
            echo "Creating user '$repluser' password..."
            sql="CREATE USER $repluser WITH LOGIN PASSWORD '$pw';"
            cockroach sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir"

            echo "Granting REPLICATION role..."
            cockroach sql --execute "GRANT SYSTEM REPLICATION TO $repluser;" --url "$systemconn" --certs-dir "$certsdir"

            echo "${bold}Remember this connection string for later use by the Standby cluster:${normal}"
            local urlforstandby="postgresql://$repluser:$pw@$listenaddr/?options=-ccluster=system&sslmode=verify-full&sslrootcert=$cacrtpath"
            echo $urlforstandby
            echo
        fi
    fi
}

if [ $cluster == $primary ];
then
    intro
    certs
    server
    license

    echo "${bold}Your next steps:${normal}"
    echo " • Remember the 'Connection string for later use by the Standby cluster' above"
    echo " • Open a new terminal window at this location, for the Standby cluster"
    echo " • Invoke this script (bash $0) in that terminal"
    echo " • Select the Standby option, and follow those prompts"
    echo

    offersql
    outro
fi

if [ $cluster == $standby ]; 
then    
    intro
    certs
    server
    license
    echo

    ok=''
    while [[ ! $ok == 't' ]];
    do
        read -p "What is the connection string for the replication user on the Primary cluster? " primaryconn
        echo
        echo 'Connecting to Primary...'
        echo

        if [[ $(cockroach sql --certs-dir "$certsdir" --url "$primaryconn" --execute "$trysql" 2>/dev/null) ]];
        then
            ok='t'
            echo "Successfully pinged the Primary"
        else
            echo "Unable to reach the Primary. You can try again..."
        fi
        echo
    done
    
    echo "${bold}Starting replication using CREATE TENANT...${normal}"
    sql="CREATE TENANT application FROM REPLICATION OF application ON '$primaryconn'"
    cockroach sql --execute "$sql" --certs-dir "$certsdir" --url "$systemconn"

    echo "To monitor replication status, use SHOW TENANTS WITH REPLICATION STATUS; from the SQL shell"
    echo

    offersql
    outro
fi 
