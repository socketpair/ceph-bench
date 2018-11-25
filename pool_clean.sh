#!/bin/bash

set -e -u

pool=prod

images=$( rbd -p "$pool" ls )

regexp='rbd_directory$|rbd_children$|rbd_info$'
for image in $images; do
    prefix=$( rbd --format=json info "$pool/$image" | jq -r .block_name_prefix )
    [[ "$prefix" =~ ^rbd_data\.([a-f0-9]+)$ ]] || exit 1
    #echo "$image - $prefix"
    image_id=${BASH_REMATCH[1]}
    regexp="$regexp|rbd_[a-z_]+[.]$image_id([.][a-f0-9]+)?"
    regexp="$regexp|rbd_id[.]$image"
done
regexp="^($regexp)\$"

rados -p "$pool" ls | egrep -v "$regexp" > extra.txt

# xargs rados -p "$pool" rm < extra.txt

#while read line; do
#    rados -p "$pool" stat "$line"
#done < extra.txt > sizes.txt
