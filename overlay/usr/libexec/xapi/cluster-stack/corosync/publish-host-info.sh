#!/bin/bash

dir=$(dirname $(readlink -f $0))

path=$($dir/get_ha_sr_mount.py)

. /etc/xensource-inventory

nodeid=$(corosync-cmapctl -g runtime.votequorum.this_node_id | awk '{print $4}')
hostuuid=$INSTALLATION_UUID
hostname=$(hostname)

logger "$0: node $nodeid = $hostname = $hostuuid"

BASE=$path/.ha/host

mkdir -p $BASE/$nodeid
echo $hostname > $BASE/$nodeid/hostname
echo $hostuuid > $BASE/$nodeid/hostuuid
