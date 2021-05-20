#!/bin/bash

# Converts time format from 10m31.32254s to 10.522
perl -pe 's{([0-9]*)m([0-9]*\.[0-9]*)s}{$1+$2/60}ge' "$1" | sed 's/\([0-9]*\.\)\([0-9][0-9][0-9]\)[0-9]*/\1\2/g'
