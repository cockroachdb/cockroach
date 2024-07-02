#!/bin/bash
set -e

# Intended to automate https://docs.google.com/document/d/1vTX6N72J0yL2dDVdyJg063gC761QdZcMQ3ADthFYi8g/edit

# Constants
readonly primary='primary'
readonly standby='standby'
readonly system='system'
readonly application='application'
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

if [ "$1" == "-v" ]; 
then
    echo "Running in -v verbose mode, you will see all commands, look for the '$PS4' prefixed lines"
    echo
    # verbose
    function run() {
        set -x
        "$@"
        { e=$?; set +x; } 2>/dev/null
        return $e
    }
else
    function run () {
        "$@"
    }
fi

if [ ! -z "$COCKROACH" ];
then
    echo "Using cockroach binary: $COCKROACH"
    echo
fi

COCKROACH=${COCKROACH:-cockroach}

echo "Checking prerequisites..."

if ! version_str=$(run "$COCKROACH" version --build-tag 2>/dev/null); then
    echo "❌ We did not find CockroachDB"
    echo "Please begin by installing CockroachDB: https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html"
    echo "If would like to use a specific cockroach binary (perhaps not in the PATH), set the env var COCKROACH=path/to/binary"
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

while [[ ! "$psw" =~ ^(p|P|s|S|w|W)$ ]];
do
    read -r -p "Would you like to use this terminal to set up the Primary, the Standby, or a workload [enter P, S or W] " psw
done
echo

case $psw in
   P|p|W|w) readonly cluster=$primary ;;
   S|s) readonly cluster=$standby ;;
esac

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
readonly appconn="postgresql://root@$listenaddr/?options=-ccluster=$application&sslmode=verify-full"
readonly trysql='select 1 as ping;'

function workload {
    if run "$COCKROACH" sql --certs-dir "$certsdir" --url "$systemconn" --execute "$trysql" 2>/dev/null;
    then
        echo "✅ $label cluster appears to already be running at $listenaddr"
    else
        echo "❌ $label cluster appears not to be running at $listenaddr"
        echo "Re-run this script (bash $0) and choose P"
        exit
    fi
    echo

    setwindowtitle "Workload on $label cluster"
    local workload="bank"

    echo "${bold}Starting workload $workload...${normal}"
    readonly workloadconn="postgresql://root@$listenaddr/$workload?options=-ccluster=$application&sslmode=verify-full&sslrootcert=$certsdir/ca.crt&sslcert=$certsdir/client.root.crt&sslkey=$certsdir/client.root.key"
    run "$COCKROACH" workload init $workload "$workloadconn" --drop
    run "$COCKROACH" workload run $workload "$workloadconn"
}

if [[ $psw == "W" || $psw == "w" ]];
then
    workload
    exit $?
fi


function intro {
    echo "${bold}Configuring $label cluster...${normal}"
    echo

    setwindowtitle "$label cluster"
}

function certs {
    echo "${bold}Configuring certificates and CA...${normal}"
    echo
    
    if [ ! -d "$certsdir" ];
    then
        echo "Creating certs directory at $certsdir"    
        mkdir -p "$certsdir"
    fi

    # Look for existing CA
    readonly cakeypath="$certsdir/ca.key"
    readonly cacrtpath="$certsdir/ca.crt"

    local foundcount=0
    for f in "$cakeypath" "$cacrtpath";
    do
        if [ -r "$f" ];
        then
            foundcount=$((foundcount+1))
        fi
    done

    if [ $foundcount = 2 ]; # all there
    then
        echo "Found certificate authority files in $certsdir, using those."
        echo
    else   
        echo "Creating certificate authority..."
        run "$COCKROACH" cert create-ca --ca-key "$cakeypath" --certs-dir "$certsdir"
    fi


    # Look for existing certs
    local foundcount=0
    for filename in "client.root.crt" "client.root.key" "node.crt" "node.key" "client.$repluser.crt" "client.$repluser.key";
    do
        if [ -r "$certsdir/$filename" ];
        then
            foundcount=$((foundcount+1))
        fi
    done

    if [ $foundcount = 6 ]; # all there
    then
        echo "Found existing certs in $certsdir, using those."
    else
        echo "Creating node certificates..."
        run "$COCKROACH" cert create-node localhost 127.0.0.1 "$(hostname -f)" ::1 localhost6 --certs-dir "$certsdir" --ca-key="$cakeypath"
        echo "Creating client certificates..."
        run "$COCKROACH" cert create-client root --certs-dir "$certsdir" --ca-key "$cakeypath"
        run "$COCKROACH" cert create-client replication --certs-dir "$certsdir" --ca-key "$cakeypath"
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
        run mkdir -p "$datadir"
    fi

    if run "$COCKROACH" sql --certs-dir "$certsdir" --url "$systemconn" --execute "$trysql" --format tsv 2>/dev/null;
    then
        echo "$label cluster appears to already be running at $listenaddr"
        echo
    else
        echo "Starting server at $listenaddr with DB Console at $httpaddr"
        run "$COCKROACH" start-single-node --certs-dir "$certsdir" --store=path="$datadir" --http-addr "$httpaddr" --listen-addr "$listenaddr" --config-profile "$configprofile" --background
        echo
        echo "Your $label cluster is running at $listenaddr (with the DB Console at $httpaddr)"
        echo
        echo "This cluster will continue to run unless you terminate it manually."
        echo
    fi
}

# from https://stackoverflow.com/a/10660730
function rawurlencode {
  local string="${1}"
  local strlen=${#string}
  local encoded=""
  local pos c o

  for (( pos=0 ; pos<strlen ; pos++ )); do
     c=${string:$pos:1}
     case "$c" in
        [-_.~a-zA-Z0-9] ) o="${c}" ;;
        * )               printf -v o '%%%02x' "'$c"
     esac
     encoded+="${o}"
  done
  echo "${encoded}"
}

function license {
    echo "${bold}Enabling replication on $label${normal}..."

    local sql='SHOW CLUSTER SETTING cluster.organization;'
    local org
    org=$(run "$COCKROACH" sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir" --format 'tsv' | tail -1 2>/dev/null)
    
    if [ -z $org ];
    then
        read -r -p "What is the organization name for this cluster? " org
        sql="SET CLUSTER SETTING cluster.organization = '$org';"
        run "$COCKROACH" sql --execute "$sql" --certs-dir "$certsdir" --url "$systemconn"
    else
        echo "Found organization name $org"
    fi

    local sql='SHOW CLUSTER SETTING enterprise.license;'
    local lic
    lic=$(run "$COCKROACH" sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir" --format 'tsv' | tail -1)
    if [ -z $lic ];
    then
        read -r -p 'What is the license key for this cluster? ' key
        sql="SET CLUSTER SETTING enterprise.license = '$key';"
        run "$COCKROACH" sql --execute "$sql" --certs-dir "$certsdir" --url "$systemconn"
    else
        echo 'Found enterprise license'
    fi

    local sql='SHOW CLUSTER SETTING cross_cluster_replication.enabled;'
    local enabled
    enabled=$(run "$COCKROACH" sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir" --format 'tsv' | tail -1)

    if [[ $enabled != 't' ]];
    then
        echo 'Enabling physical replication...'
        sql='SET CLUSTER SETTING cross_cluster_replication.enabled = true;'
        run "$COCKROACH" sql --execute "$sql" --certs-dir "$certsdir" --url "$systemconn"
    else
        echo "Physical replication is enabled"
    fi

    if [ $cluster == $primary ];
    then
        sql='SHOW CLUSTER SETTING kv.rangefeed.enabled;'
        enabled=$(run "$COCKROACH" sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir" --format 'tsv' | tail -1)

        if [[ $enabled != 't' ]];
        then
            echo 'Enabling rangefeeds...'
            sql='SET CLUSTER SETTING kv.rangefeed.enabled = true;'
            run "$COCKROACH" sql --execute "$sql" --certs-dir "$certsdir" --url "$systemconn"
        else
            echo "Rangefeeds enabled"
        fi

        echo "Checking replication user..."

        local sql="select username from [show users] where username='$repluser';"
        local user
        user=$(run "$COCKROACH" sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir" --format 'tsv' | tail -1)
        
        if [ $user == $repluser ]; then
            echo "User '$repluser' already exists. Hopefully you have saved the password."
            echo
            echo "${bold}Connection string for later use by the Standby cluster:${normal}"
            
            local urlforstandby="postgresql://$repluser:<the password>@$listenaddr/?options=-ccluster=system&sslmode=verify-full&sslinline=true&sslrootcert=$cacrtpath&sslcert=$certsdir/client.root.crt&sslkey=$certsdir/client.root.key"
            echo $urlforstandby
            echo
        else
            # pwgen is not on all systems, is there a better choice?
            # https://unix.stackexchange.com/questions/230673/how-to-generate-a-random-string
            pw=$(run openssl rand -base64 20)
            echo "Creating user '$repluser' password..."
            sql="CREATE USER $repluser WITH LOGIN PASSWORD '$pw';"
            run "$COCKROACH" sql --execute "$sql" --url "$systemconn" --certs-dir "$certsdir"

            echo "Granting REPLICATION role..."
            run "$COCKROACH" sql --execute "GRANT SYSTEM REPLICATION TO $repluser;" --url "$systemconn" --certs-dir "$certsdir"

            pw=$(rawurlencode "$pw")
            echo "${bold}Remember this connection string for later use by the Standby cluster:${normal}"
            local urlforstandby="postgresql://$repluser:$pw@$listenaddr/?options=-ccluster=system&sslmode=verify-full&sslrootcert=$cacrtpath"
            echo $urlforstandby
            echo
        fi
    fi
}

function offersql {
    echo "In the current terminal, you can:"
    if [ $cluster == $primary ];
    then
        echo " • type 'a' to connect to a SQL shell for the $label cluster's $application tenant"
    fi
    echo " • type 's' to connect to a SQL shell for the $label cluster's $system tenant"
    echo " • type any other key to exit"
    local next
    read -r next
    echo

    local ccluster
    case $next in
        A|a) ccluster=$application ;;
        S|s) ccluster=$system ;;
    esac


    if [ ! -z "$ccluster" ];
    then
        echo "Connecting to $label cluster's $ccluster tenant..."
        setwindowtitle "$label cluster, $ccluster tenant"
        run "$COCKROACH" sql --certs-dir "$certsdir" --url "postgresql://root@$listenaddr/?options=-ccluster=$ccluster&sslmode=verify-full"
    else
        echo "${bold}Exiting${normal}"
        echo
        echo "If you wish to connect to the $label cluster's $system tenant (for application data), use:"
        cmd="$COCKROACH sql --certs-dir '$certsdir' --url '$systemconn'"
        echo "$cmd"
        echo
        echo "If you wish to connect to the $label cluster's $application tenant (for administration), use:"
        cmd="$COCKROACH sql --certs-dir '$certsdir' --url '$appconn'"
        echo "$cmd"
        echo
    fi
}

function outro {
    echo 'If you want to wipe everything away and start fresh:'
    echo " • Stop all cockroach processes with pkill cockroach"
    echo " • Wait 10 seconds"
    echo " • rm -rf '$basedir'"
    echo " • Invoke this script again with bash $0"
}

if [ $cluster == $primary ];
then
    intro
    certs
    server
    license

    echo "${bold}Your next steps:${normal}"
    echo "Continue on to create a Standby cluster for replication:"
    echo " • Remember the 'Connection string for later use' above"
    echo " • Open a new terminal window at this location, for the Standby cluster"
    echo " • Invoke this script (bash $0) in that terminal"
    echo " • Select the Standby option, and follow those prompts"
    echo
    echo "To start a workload on this $label cluster:"
    echo " • Open a new terminal window at this location"
    echo " • Invoke this script (bash $0) in that terminal"
    echo " • Select the Workload option, and follow those prompts"
    echo
    echo "To open a DB Console on this $label cluster:"
    echo " • Create a user with these instructions: https://www.cockroachlabs.com/docs/stable/create-user.html"
    echo " • Optionally grant that user the admin role, to see more in the DB Console"
    echo " • Open this URL in a web browser: http://$httpaddr"
    echo " • Log in with the above user"
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
    while [[ $ok != 't' ]];
    do
        read -r -p "What is the connection string for the replication user on the Primary cluster? " primaryconn
        echo
        echo 'Connecting to Primary...'
        echo

        if run "$COCKROACH" sql --url "$primaryconn" --execute "$trysql";
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
    run "$COCKROACH" sql --execute "$sql" --certs-dir "$certsdir" --url "$systemconn"

    echo "To monitor replication status, from the SQL shell:"
    echo " SHOW TENANTS WITH REPLICATION STATUS;"
    echo
    echo "To terminate replication (cutover) on this $cluster cluster:"
    echo " ALTER TENANT application COMPLETE REPLICATION TO LATEST;"
    echo
    echo "To open a DB Console on this $label cluster:"
    echo " • Create a user with these instructions: https://www.cockroachlabs.com/docs/stable/create-user.html"
    echo " • Optionally grant that user the admin role, to see more in the DB Console"
    echo " • Open this URL in a web browser: http://$httpaddr"
    echo " • Log in with the above user"
    echo

    offersql
    outro
fi 
