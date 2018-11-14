#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

#
# This is a mechanism for aggregating object sizes and counts across all
# storage nodes (makos) in a region.  Currently, it relies on the contents of
# /poseidon/stor/mako to obtain information about each storage node in a Manta
# deployment.  As a first recourse, we will check for the presence of a summary
# file which may exist if the mako has been configured (via SAPI) to provide
# one, however, in the event that it was not, this script will derive the
# summary itself from the much larger mako manifest.  The compilation of all
# manifest summaries are used to create the overall region report.  For more
# information on format and the kind of information that is exposed, see
# README.md.
#

PID=$$
PID_FILE=/tmp/mako-regional-report.pid
REGION_LISTING="region"
REMOTE_PATH="/poseidon/stor/mako"
SUMMARY_DIR="/tmp/summary"
STATUS=0

if [[ -n "$TRACE" ]]; then
    export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
    set -o xtrace
fi
set -o errexit
set -o pipefail

function warn()
{
	local LNOW=`date`
	echo "$LNOW: $(basename $0): warning: $*" >&2
	STATUS=1
}

function fatal
{
	local LNOW=`date`
	echo "$LNOW: $(basename $0): fatal error: $*" >&2
	rm "$PID_FILE"
	exit 1
}

function log
{
	local LNOW=`date`
	echo "$LNOW: $(basename $0): info: $*" >&2
}

function check_env_var()
{
	var=$1
	name=$2

	if [[ -z "$var" ]]; then
		fatal "$name is not set"
	fi
}

function download_summary()
{
	file=$1

	mget "${REMOTE_PATH}/summary/${file}" > "$SUMMARY_DIR/$file" && true

	if [[ $? -ne 0 ]]; then
		log "Unable to find summary for $file."
		return 1
	fi

	return 0
}

function generate_summary()
{
	log "Downloading mako manifest $file"

	#
	# This will download the contents of a full manifest from
	# /poseidon/stor/mako and attempt to generate the summary. Because
	# this is the most expensive part of the process, we have the option
	# of asking the mako itself to perform this for us by changing the
	# value of the SAPI tunable `MAKO_PROCESS_MANIFEST' to true.  Then, the
	# next time a mako generates a manifest, it will follow up by deriving
	# its own summary for it and storing it in /poseidon/stor/mako/summary.
	# Once the number of storage nodes in a given Manta installation goes
	# beyond a certain number (whatever that might be), this may become
	# necessary in order to complete a full aggregation of a region in a
	# reasonable amount of time.
	#
	mget "${REMOTE_PATH}/${file}" | gawk -M -v PREC="quad" '{
		split($1, x, "/")
		acct=x[3]
		bytes[acct] += $2
		objects[acct]++
		kilobytes[acct] += $4
		total_bytes += $2
		total_objects++
		total_kilobytes += $4

		#
		# If the Manta directory happens to be "tombstone" then x[4]
		# contains the name of the subdirectory which will always be
		# a date.  We want to organize the objects in this part of the
		# tree by their subdirectory name (i.e. its date of creation)
		# so that when analyzing a summary, a determination can be made
		# not only about how much storage we stand to reclaim in overall
		# but also _when_ we stand to reclaim each fraction of the
		# tombstone directory tree.
		#
		if (x[3] == "tombstone") {
			date=x[4]
			tombstone_bytes[date] += $2
			tombstone_kilobytes[date] += $4
			tombstone_objects[date]++
		}
	} END {
		printf("%s\t%s\t%s\t%s\t%s\n", "account", "bytes",
		    "objects", "average size kb", "kilobytes");

		for (date in tombstone_bytes) {
			printf("tombstone_%s\t%f\t%f\t%f\t%f\n", date,
			    tombstone_bytes[date], tombstone_objects[date],
			    tombstone_kilobytes[date] / tombstone_objects[date],
			    tombstone_kilobytes[date]);
		}

		for (acct in bytes) {
			printf("%s\t%f\t%f\t%f\t%f\n",
			    acct, bytes[acct], objects[acct],
			    kilobytes[acct] / objects[acct], kilobytes[acct]);
		}

		if (total_objects == 0) {
			total_avg = 0;
		} else {
			total_avg = total_kilobytes / total_objects;
		}

		printf("%s\t%f\t%f\t%f\t%f\n", "totals", total_bytes,
		    total_objects, total_avg, total_kilobytes);
	}' > "$SUMMARY_DIR/$file" && true

	if [[ $? -ne 0 ]]; then
		rm "$SUMMARY_DIR/$file"
		return 1
	fi

	return 0
}

function get_tombstone()
{
	tsdir=$1
	summary="$SUMMARY_DIR/$sn"
	entry=$(grep "$tsdir" "$summary")

	date=$(echo "$entry" | gawk '{
		split($1, dir, "_");
		printf("%s", dir[2]);
	}')
	objects=$(echo "$entry" | gawk -M -v PREC="quad" '{ print $3 }')
	kilobytes=$(echo "$entry" | gawk -M -v PREC="quad" '{ print $5 }')

	tsobj=$(echo "{}" | json -e "\
		this.date = '${date}';
		this.objects = ${objects};
		this.kilobytes = ${kilobytes};")

	if [[ $? -ne 0 ]]; then
		return 1
	fi

	echo "$tsobj"
	return 0
}

function process_summary()
{
	local sn="$1"
	local dc="$2"
	local region=$3
	local summary="$SUMMARY_DIR/$sn"

	download_summary "$sn" && true

	#
	# It would be great if someone did the heavy lifting for us already
	# but if there is no summary which has already been generated, then
	# we must download the manifest ourselves and generate it.
	#
	if [[ $? -ne 0 ]]; then
		generate_summary "$sn" && true
		if [[ $? -ne 0 ]]; then
			warn "Unable to generate summary for $sn"
			return
		fi
	fi

	local entry=$(grep "totals" $summary)
	local objects=$(echo "$entry" | gawk -M -v PREC="quad" '{ print $3 }')
	local average=$(echo "$entry" | gawk -M -v PREC="quad" '{ print $4 }')
	local kilobytes=$(echo "$entry" | gawk -M -v PREC="quad" '{ print $5 }')
	local jobj=$(echo "{}" | json -e " \
		this.datacenter = '${dc}';
		this.storage_id = '${sn}';
		this.kilobytes = ${kilobytes};
		this.objects = ${objects};
		this.avg = ${average};
		this.tombstone = [];")

	#
	# If we fail to successfully process the fundemantal stats for this
	# mako, short-circuit the rest of this operation and move on to the
	# next one.
	#
	if [[ $? -ne 0 ]]; then
		warn "Failed to process summary for $sn"
		return
	fi

	local tsdirs=($(cat $summary | grep "tombstone_" | awk '{print $1}'))

	for i in ${tsdirs[@]}
	do
		local tsobj=$(get_tombstone $i)
		if [[ $? -ne 0 ]]; then
			warn "Unable to generate tombstone object for $sn/$i"
			continue
		fi
		local jobj=$(echo $jobj |\
		    json -e "this.tombstone.push(${tsobj})")
	done

	echo $jobj >> "$SUMMARY_DIR/$region"

	#
	# If we are unable to update the aggregation file, there is no reason
	# to believe that trying again would yield a different result.  There is
	# probably no point in continuing further.
	#
	if [[ $? -ne 0 ]]; then
		fatal "Unable to update aggregation file $SUMMARY_DIR/$region"
	fi
}

function get_datacenter()
{
	local file="$1"
	local dc=""

	dc=$(minfo $file |grep m-datacenter | gawk '{print $2}') && true
	#
	# If we are unable to obtain the datacenter for this mako, it could be
	# for a number of reasons: maybe minfo did not succeed, or it did
	# succeed but the datacenter was not present in the header information
	# because the mako was not yet running a build with MANTA-4015.  Either
	# way, it's an inconvenience, but not a deal breaker.
	#
	if [[ -z "$dc" ]]; then
		echo "Unknown"
		return
	fi

	echo $dc
}

function process_group()
{
	#
	# Obtain a list of storage nodes in the region.  This is done by
	# observing the contents of /poseidon/stor/mako.  Each storage node in
	# the region will upload a manifest to that directory where the name of
	# the manifest is cleverly named after the storage node itself.  If, for
	# whatever reason, a manifest is not present for that storage node at
	# that time, it is simply not included in the list.
	#
	local storage_nodes=$(mls --type=o /poseidon/stor/mako)

	for i in ${storage_nodes[@]}
	do
		dc=$(get_datacenter "$REMOTE_PATH/$i")
		process_summary  "$i" "$dc" "$REGION_LISTING"
	done
}

#
# If an instance of this script is already running, then exit.
#
LAST_PID=$(cat $PID_FILE 2>/dev/null) && true

if [[ -n "$LAST_PID" ]]; then
    ps -p $LAST_PID > /dev/null && true
    if [[ $? -eq 0 ]]; then
        echo "$0 process still running.  Exiting."
        exit 1
    fi
fi

#
# No other instances of the script are currently running.  Record our pid.
#
echo -n $PID > $PID_FILE

#
# To even have a chance at succeeding, these environment variables must be set.
#
check_env_var "$MANTA_USER" "MANTA_USER"
check_env_var "$MANTA_KEY_ID" "MANTA_KEY_ID"
check_env_var "$MANTA_URL" "MANTA_URL"

if [ -d "$SUMMARY_DIR" ]; then
	rm -rf "$SUMMARY_DIR"
fi

mkdir "$SUMMARY_DIR"

process_group

#
# Convert the aggregated summary to JSON
#
cat "$SUMMARY_DIR/$REGION_LISTING" |\
    json -g > "$SUMMARY_DIR/${REGION_LISTING}.json"

mmkdir "$REMOTE_PATH/summary"
if [[ $? -ne 0 ]]; then
	fatal "Unable to create summary directory in manta"
fi

mput -f $SUMMARY_DIR/${REGION_LISTING}.json "$REMOTE_PATH/summary"
if [[ $? -ne 0 ]]; then
	fatal "Unable to upload $SUMMARY_DIR/${REGION_LISTING}.json to manta"
fi

rm "$PID_FILE"
exit $STATUS
