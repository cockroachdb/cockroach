# Source this file from one of the other jepsen scripts

PS4="+($(basename $0)) "

LOG_DIR="${COCKROACH_PATH}/artifacts"
mkdir -p "${LOG_DIR}"
cd "${LOG_DIR}"

KEY_NAME="${KEY_NAME-google_compute_engine}"

SSH_OPTIONS=(-o "ServerAliveInterval=60" -o "StrictHostKeyChecking no" -i "$HOME/.ssh/${KEY_NAME}")

# Ensure that the terraform config is cancelled if one of the run scripts fails
# or the entire thing is interrupted externally.
function destroy {
  set +e
  progress Destroying cluster...
  terraform destroy --var=key_name="${KEY_NAME}" --force || true

  if test -n "${currentTestName:-}"; then
      tc Failed "$currentTestName"
      tc Finished "$currentTestName"
  fi
  exit 1
}
trap destroy ERR SIGHUP SIGINT SIGTERM

function tc {
    printf "##%s[test%s name='Jepsen%s']\\n" teamcity "$1" "$2"
    case $1 in
	Started) currentTestName=$2 ;;
	Finished) currentTestName= ;;
    esac
}

function progress {
    printf "##%s[progressMessage '%s']\\n" teamcity "$*"
}

nemeses=(
    # big-skews disabled since they assume an eth0 interface.
    #"--nemesis big-skews"
    "--nemesis majority-ring"
    "--nemesis start-stop-2"
    "--nemesis start-kill-2"
    #"--nemesis majority-ring --nemesis2 big-skews"
    #"--nemesis big-skews --nemesis2 start-kill-2"
    "--nemesis majority-ring --nemesis2 start-kill-2"
    "--nemesis parts --nemesis2 start-kill-2"
)

tests=(
    "bank"
    "comments"
    "register"
    "monotonic"
    "sets"
    "sequential"
)
