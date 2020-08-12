#! /usr/bin/env expect -f
#
source [file join [file dirname $argv0] common.tcl]

set recordfile "temp-dirs-record.txt"
set tempprefix "cockroach-temp"
set storedir "logs/mystore"
set tempdir "logs/mystore/temp"
set cwd ""
# Ensure that pwd will only return absolute paths.
cd [file normalize [pwd]]
set ::env(PWD) [pwd]

# We want cwd to be "" if pwd is root since it will mess up our pattern matching.
if {! [string match "/" [pwd]]} {
  set cwd [pwd]
}

proc file_exists {filepath} {
  if {! [ file exist $filepath]} {
    report "MISSING EXPECTED FILE: $filepath"
    exit 1
  }
}

proc file_not_exists {filepath} {
  if {[ file exist $filepath]} {
    report "FILE STILL EXISTS: $filepath"
    exit 1
  }
}

proc glob_exists {pattern} {
  if {[llength [glob -nocomplain -type d $pattern]] == 0} {
    report "MISSING EXPECTED GLOB FILES: $pattern"
    exit 1
  }
}

proc glob_not_exists {pattern} {
  if {[llength [glob -nocomplain -type d $pattern]] > 0} {
    report "GLOB FILES STILL EXIST: $pattern"
    exit 1
  }
}

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that on node startup a temporary subdirectory is created under --temp-dir and recorded to a record file, and on node shutdown the directory is removed."
send "mkdir -p $tempdir\r"
send "$argv start-single-node --insecure --store=$storedir --temp-dir=$tempdir\r"
eexpect "node starting"
eexpect "temp dir:*$tempdir/$tempprefix"
# Verify the temp directory under first store is created.
glob_exists "$tempdir/$tempprefix*"
set rfile [open "$storedir/$recordfile"]
# Verify temp directory path recorded to record file.
if {! [string match "$cwd/$tempdir/$tempprefix*" [gets $rfile]]} {
  report "MISSING DIRECTORY PATH FROM RECORD FILE"
  exit 1
}
close $rfile
interrupt
eexpect "shutdown completed"
# Verify the temp directory is removed.
glob_not_exists "$tempdir/$tempprefix*"
# Verify temp directory path is removed from record file.
if {[file size "$storedir/$recordfile"] > 0} {
  report "RECORD FILE NOT EMPTY"
  exit 1
}
end_test

start_test "Check that on node startup a temporary subdirectory is created under --temp-dir even if store is in-memory and removed on shutdown."
send "mkdir -p $tempdir\r"
send "$argv start-single-node --insecure --store=type=mem,size=1GB --temp-dir=$tempdir\r"
eexpect "node starting"
eexpect "temp dir:*$tempdir/$tempprefix"
# Verify the temp directory under first store is created.
glob_exists "$tempdir/$tempprefix*"
interrupt
eexpect "shutdown completed"
# Verify the temp directory is removed.
glob_not_exists "$tempdir/$tempprefix*"
end_test

start_test "Check that temporary directories in record file are cleaned up during cli startup."
send "mkdir -p $tempdir $storedir/temp1 $storedir/temp2\r"
send "echo foobartext >  $storedir/temp1/foo.txt\r"
# We add the temp directories to the record file.
send "cat > $storedir/$recordfile <<EOF\r$cwd/$storedir/temp1\r$cwd/$storedir/temp2\rEOF\r"
send "$argv start-single-node --insecure --store=$storedir --temp-dir=$tempdir\r"
eexpect "node starting"
eexpect "temp dir:*$cwd/$tempdir/$tempprefix"
# Verify temp1 and temp2 are removed shortly after startup.
file_not_exists "$storedir/temp1"
file_not_exists "$storedir/temp2"
interrupt
eexpect "shutdown completed"
# Verify the temp directory is removed.
glob_not_exists "$tempdir/$tempprefix*"
# Verify store directory still exists.
file_exists $storedir
end_test

start_test "Check that if --temp-dir is unspecified, a temporary directory is created under --store"
send "$argv start-single-node --insecure --store=$storedir\r"
eexpect "node starting"
eexpect "temp dir:*$cwd/$storedir/$tempprefix"
# Verify the temp directory under first store is created.
glob_exists "$storedir/$tempprefix*"
interrupt
eexpect "shutdown completed"
# Verify the temp directory is removed.
glob_not_exists "$storedir/$tempprefix*"
# Verify the store file still exists.
file_exists $storedir
end_test

start_test "Check that temp directory does not get wiped upon subsequent failed cockroach start attempt and that a cockroach instance can be subsequently started up after a shutdown"
send "$argv start-single-node --insecure --store=$storedir --background\r"
eexpect ":/# "
# Try to start up a second cockroach instance with the same store path.
send "$argv start-single-node --insecure --store=$storedir\r"
eexpect "ERROR: could not cleanup temporary directories from record file: could not lock temporary directory $cwd/$storedir/$tempprefix*"
# Verify the temp directory still exists.
glob_exists "$storedir/$tempprefix*"
send "pkill -9 cockroach\r"
# We should be able to start the cockroach instance again.
send "$argv start-single-node --insecure --store=$storedir\r"
eexpect "node starting"
interrupt
eexpect "shutdown completed"
end_test
