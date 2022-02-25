#!/bin/sh

fstab=$TARGET_DIR/etc/fstab
grep -q /bincheck "$fstab" || cat >> "$fstab" <<EOF
/dev/sda1	/bincheck	vfat	defaults	0	0
EOF
