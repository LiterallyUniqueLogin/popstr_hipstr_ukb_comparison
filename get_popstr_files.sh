#!/bin/bash

# get a print out of each file in the directory
# then only print out ID and name lines (appears in that order)
# then get IDs and names on the same line
# then reverse the order
# then sort so that .cram and .cram.crai files appear together in that order
# then only print out the IDs (not the names)
# then print out cram ID and cram.crai ID on the same line
dx describe 'Bulk/Previous WGS releases/GATK and GraphTyper WGS/Microsatellites [150k release]/*' | \
awk '($1=="ID" || $1 == "Name") {print $2}' | \
paste - - | \
awk '{print $2 " " $1}' | \
sort | \
awk '{print $2}' | \
paste - - >> popstr_file_ids.txt
# this seems to duplicate each file, so need to manually remove the second half of the file
