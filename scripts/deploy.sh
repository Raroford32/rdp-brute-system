#!/bin/bash
echo "Deploying server..."
./bin/server &
echo "Deploying client..."
./bin/client &
