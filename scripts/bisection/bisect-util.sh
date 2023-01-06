#bisect helpers
#expects TEST_NAME, BRANCH, FROM_DATE, TO_DATE


log() { local msg=$1
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ")   $msg" >> "$INFO_LOG"
}

create_conf_if_not_exists() {
   if [ ! -f "$CONF_NAME" ]; then
     echo "{}" > "$CONF_NAME"
   fi
}

get_conf_val() { local key=$1
  if [ ! -f "$CONF_NAME" ]; then
    echo ""
  else
    val=$(jq -r "$key" "$CONF_NAME")
    if [ "$val" == "null" ]; then
      echo ""
    else
      echo "$val"
    fi
  fi
}

#create or update file
set_conf_val() { local key=$1; local val=$2
  create_conf_if_not_exists
  updated=$(jq ". += {\"$key\": \"$val\"}" "$CONF_NAME")
  echo "$updated" > "$CONF_NAME"
}


set_hash_result() { local hash=$1; local val=$2
  create_conf_if_not_exists
  updated=$(jq ".hashResults += {\"$hash\": \"$val\"}" "$CONF_NAME")
  echo "$updated" > "$CONF_NAME"
}

get_hash_result() { local hash=$1
  if [ ! -f "$CONF_NAME" ]; then
    echo ""
  else
    val=$(jq -r ".hashResults.\"$hash\"" "$CONF_NAME")
    if [ "$val" == "null" ]; then
      echo ""
    else
      echo "$val"
    fi
  fi
}

clean_name_for_dir() {
  echo "${1//[^[:alnum:]]/-}"
}

current_hash() {
  git rev-parse --short HEAD
}

calc_avg_ops() {
  JQ_Q='group_by(.Elapsed) | map( { elapsed: (.[0].Elapsed / 1000000000) | rint, read: map(if(.Name=="read") then (.Hist.Counts | add) else 0 end) | add, write: map(if(.Name=="write") then (.Hist.Counts | add) else 0 end) | add } ) | ((([.[].read] | add) + ([.[].write] | add)) / ([.[].elapsed] | add) | rint)'
  jq -sc "$JQ_Q" $@
}

stress_sha() { local sha=$1; local test=$2; local count=$3
  abase="artifacts/${sha}-dirty"
  if [ -d "$abase/$test" ]; then
    echo "[$sha] Using stats from existing run"
    return
  fi

  echo "[$sha] Running..."

  args=(
    "run" "^${test}\$"
    "--port" "$((8080+RANDOM % 1000))"
    "--workload" "${abase}/workload"
    "--cockroach" "${abase}/cockroach"
    "--artifacts" "${abase}/"
    "--count" "${count}"
    "--cpu-quota" "640"
  )
  args+=("${@:4}")
  "${abase}/roachtest" "${args[@]}"
}

build_sha() { local sha=$1; local duration_override_mins=$2
  git reset --hard
  git checkout "$sha"

  fullsha=$(git rev-parse "$sha")

  sed -i "s/opts\.duration = 30 \* time\.Minute/opts.duration = $duration_override_mins * time.Minute/"  pkg/cmd/roachtest/tests/kv.go || exit 2
  sed -i "s/ifLocal(c, \"10s\", \"30m\")/ifLocal(c, \"10s\", \"${duration_override_mins}m\")/"  pkg/cmd/roachtest/tests/ycsb.go || exit 2

#  git apply ./scripts/bisection/roachtest.patch || echo "unable to patch roachtest - cluster will not be reused next time"

  # mark dirty since we've applied changes
  abase="artifacts/${sha}-dirty"
  mkdir -p "${abase}"

  # Locations of the binaries.
  rt="${abase}/roachtest"
  wl="${abase}/workload"
  cr="${abase}/cockroach"

  if [ ! -f "${cr}" ]; then
    if gsutil cp "gs://cockroach-edge-artifacts-prod/cockroach/cockroach.linux-gnu-amd64.$fullsha" "${cr}"; then
        echo "Copied cockroach binary from GCS"
    else
        ./dev build "cockroach-short" --cross=linux
        cp "artifacts/cockroach-short" "${cr}"
    fi
  fi

  if [ ! -f "${wl}" ]; then
    if gsutil cp "gs://cockroach-edge-artifacts-prod/cockroach/workload.$fullsha" "${wl}"; then
      echo "Copied workload from GCS"
    else
      ./dev build workload --cross=linux
      cp "artifacts/workload" "${wl}"
    fi
  fi

  if [ ! -f "${rt}" ]; then
    ./dev build roachtest
    cp "bin/roachtest" "${rt}"
  fi

  chmod +x "$cr" "$wl" "$rt"
  git reset --hard
}

# if ops == -1, this is a trapped ^C and we don't yet have a result
prompt_user() { local hash=$1; local ops=$2;

  echo -ne '\a'
  if [[ ops -gt 0 ]]; then
    PS3="[$hash] Average ops/s is $ops. Choose: "
  else
    PS3="[$hash] Interrupt: mark current and continue, or just quit?"
  fi

  select ch in Good Bad Skip Quit
  do
    case $ch in
    "Good")
      if [[ ops -gt 0 ]]; then
        log "[$hash] Average ops/s: [$ops]. User marked as good. Threshold updated."
        set_conf_val "goodThreshold" "$ops"
      else
        set_hash_result "$hash" "USER_GOOD"
        log "[$hash] Interrupted. User marked as good. Bisection will restart with updated bounds"
      fi
      return 0;;
    "Bad")
      if [[ ops -gt 0 ]]; then
        log "[$hash] Average ops/s: [$ops]. User marked as bad. Threshold updated."
        set_conf_val "badThreshold" "$ops"
      else
        set_hash_result "$hash" "USER_BAD"
        log "[$hash] Interrupted. User marked as bad. Bisection will restart with updated bounds"
      fi
      return 1;;
    "Skip")
      if [[ ops -gt 0 ]]; then
        log "[$hash] Average ops/s: [$ops]. User skipped."
      else
        set_hash_result "$hash" "USER_SKIP"
        log "[$hash] Interrupted. User skipped"
      fi
      return 125;;
    "Quit")
      return 200;;
    *)
      echo "Enter a valid choice";;
    esac
  done
}

export BISECT_DIR="$(clean_name_for_dir "$TEST_NAME")/$(clean_name_for_dir "$BRANCH")/$FROM_DATE,$TO_DATE"
export BISECT_LOG="$BISECT_DIR/bisect.log"
export INFO_LOG="$BISECT_DIR/info.log"
export CONF_NAME="$BISECT_DIR/config.json"
export GCE_PROJECT=andrei-jepsen
