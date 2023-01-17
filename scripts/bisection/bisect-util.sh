#bisect helpers

log() { local msg=$1
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ")   $msg" >> "$INFO_LOG"
}

short_hash() { local rev=$1
  git rev-parse --short "$rev"
}

create_conf_if_not_exists() {
  [[ -f $CONF_NAME ]] || echo "{}" > "$CONF_NAME"
}

get_conf_val() { local key=$1
  if [ ! -f "$CONF_NAME" ]; then
    echo ""
  else
    val=$(jq -r "$key" "$CONF_NAME")
    if [[ "$val" == "null" ]]; then
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
  get_conf_val ".hashResults.\"$hash\""
}

calc_avg_ops() { local hash=$1; local test=$2
  jq_exec "artifacts/$hash*/$test/run_*/*.perf/stats.json"
}

jq_exec() {
  jq_expression=$'group_by(.Elapsed) |
        map(
          {
            elapsed: (.[0].Elapsed / 1000000000) | rint,
            count: map(.Hist.Counts | add) | add
          }
        ) |
        ( (([.[].count] | add)) / ([.[].elapsed] | add) | rint )'

  jq -sc "$jq_expression" $@
}

test_hash() { local hash=$1; local test=$2; local count=$3
  {
    abase="artifacts/${hash}"
    if [ -d "$abase/$test" ]; then
      echo "[$hash] Using stats from existing run"
      return
    fi

    echo "[$hash] Running..."

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
  } &> "$BISECT_DIR/$hash-test.log"
}

build_hash() { local hash=$1; local duration_override_mins=$2
 {
    git reset --hard
    git checkout "$hash"

    fullsha=$(git rev-parse "$hash")

    abase="artifacts/${hash}"
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
      if [[ -n $duration_override_mins ]]; then
        echo "Building roachtest with duration override of $duration_override_mins mins"
        sed -i "s/opts\.duration = 30 \* time\.Minute/opts.duration = $duration_override_mins * time.Minute/"  pkg/cmd/roachtest/tests/kv.go || exit 2
        sed -i "s/ifLocal(c, \"10s\", \"30m\")/ifLocal(c, \"10s\", \"${duration_override_mins}m\")/"  pkg/cmd/roachtest/tests/ycsb.go || exit 2
        echo "duration override: $duration_override_mins mins" > "$abase/_dirty"
      fi
      ./dev build roachtest
      cp "bin/roachtest" "${rt}"
    fi

    chmod +x "$cr" "$wl" "$rt"
    git reset --hard
  } &> "$BISECT_DIR/$hash-build.log"
}

# if ops == -1, this is a trapped ^C from which we want to collect user input
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

[[ -n $BISECT_DIR ]] || export BISECT_DIR="."
[[ -d ./pkg/cmd/cockroach ]] || { echo "bisection must be run from cockroach root"; return 1; }

mkdir -p "$BISECT_DIR"

export BISECT_LOG="$BISECT_DIR/bisect.log"
export INFO_LOG="$BISECT_DIR/info.log"
export CONF_NAME="$BISECT_DIR/config.json"
export GCE_PROJECT=andrei-jepsen
