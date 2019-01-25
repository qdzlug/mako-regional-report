#!/bin/bash
#
# This is a very, very, very quick and dirty script to take a series of mako
# manifests from MANTA-3664 and turn them into a series of prometheus-readable
# data for object count, object logical bytes, object physical bytes.
#
# All numbers are rounded to the nearest whole number, and each piece
# of data is tagged with the shrimp id, and the account.
#
#

OUTPUTFILE="prom.out"

# Clear the file
rm $OUTPUTFILE

# Header for objects....
echo "# HELP manta_object_stored The total number of objects stored." >> $OUTPUTFILE
echo "# TYPE manta_object_stored gauge" >> $OUTPUTFILE

# Pull objects - assumes we have set the MANTA vars right
for THESHRIMP in $(mfind /poseidon/stor/mako/summary -t o --name=stor); do
mget -q "$THESHRIMP" | grep -E -v "totals|account" | awk -v x=$(basename "$THESHRIMP") '{printf "manta_object_stored{account='"'"'%s'"'"',mako='"'"'%s'"'"'} %3.0f\n", $1,x,$3}' | sed "s/'/\"/g" >> $OUTPUTFILE
done

# Header for objects....
echo "# HELP manta_object_logical_bytes The total logical size of objects stored." >> $OUTPUTFILE
echo "# TYPE manta_object_logical_bytes gauge" >> $OUTPUTFILE

# Pull objects - assumes we have set the MANTA vars right
for THESHRIMP in $(mfind /poseidon/stor/mako/summary -t o --name=stor); do
mget -q "$THESHRIMP" | grep -E -v "totals|account" | awk -v x=$(basename "$THESHRIMP") '{printf "manta_object_logical_bytes{account='"'"'%s'"'"',mako='"'"'%s'"'"'} %3.0f\n", $1,x,$2}' | sed "s/'/\"/g" >> $OUTPUTFILE
done

# Header for objects....
echo "# HELP manta_object_phys_kilobytes The total physical size of objects stored." >> $OUTPUTFILE
echo "# TYPE manta_object_phys_kilobytes gauge" >> $OUTPUTFILE

# Pull objects - assumes we have set the MANTA vars right
for THESHRIMP in $(mfind /poseidon/stor/mako/summary -t o --name=stor); do
mget -q "$THESHRIMP" | grep -E -v "totals|account" | awk -v x=$(basename "$THESHRIMP") '{printf "manta_object_phys_kilobytes{account='"'"'%s'"'"',mako='"'"'%s'"'"'} %3.0f\n", $1,x,$2}' | sed "s/'/\"/g" >> $OUTPUTFILE
done
