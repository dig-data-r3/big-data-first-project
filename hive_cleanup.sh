#!/bin/bash

rm derby.log
rm -rf metastore_db
schematool -dbType derby -initSchema  
