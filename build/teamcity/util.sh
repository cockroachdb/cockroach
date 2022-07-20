# Some common utilities also used by Bazel build configs.

tc_start_block() {
  echo "##teamcity[blockOpened name='$1']"
}

tc_end_block() {
  echo "##teamcity[blockClosed name='$1']"
}

tc_start_progress_block() {
  tc_start_block $1
  echo "##teamcity[progressStart '$1']"
}

tc_end_progress() {
  echo "##teamcity[progressEnd '$1']"
  tc_end_block $1
}
