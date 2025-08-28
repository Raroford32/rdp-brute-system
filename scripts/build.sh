#!/bin/bash

# Exit on error
set -e

# Get the project root directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." &> /dev/null && pwd )"

echo "Changing to project root: $DIR"
cd "$DIR"

echo "Creating bin directory..."
mkdir -p bin

echo "Building unified executable..."
go build -ldflags "-s -w" -tags sqlite_omit_load_extension -o bin/rdp-brute-unified ./cmd/unified

echo "Building server..."
go build -ldflags "-s -w" -tags sqlite_omit_load_extension -o bin/rdp-server ./server/cmd/server

echo "Building client..."
go build -ldflags "-s -w" -o bin/rdp-client ./client/cmd/client

echo "Build complete."
echo ""
echo "Available executables:"
echo "  bin/rdp-brute-unified  - Single executable with server, client, and web dashboard"
echo "  bin/rdp-server         - Server component only"
echo "  bin/rdp-client         - Client component only"
echo ""
echo "Usage examples:"
echo "  ./bin/rdp-brute-unified                    # Run both server and client"
echo "  ./bin/rdp-brute-unified -mode=server       # Run server only"
echo "  ./bin/rdp-brute-unified -mode=client       # Run client only"
echo "  ./bin/rdp-brute-unified -help              # Show help"
