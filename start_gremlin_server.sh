#!/bin/zsh

# ==== CONFIGURE PATH ====
GREMLIN_SERVER_DIR="./apache-tinkerpop-gremlin-server-3.7.3"
SERVER_CONF_FILE="conf/gremlin-server.yaml"   # Change if you use a different config

# ==== START GREMLIN SERVER ====
echo "Starting Gremlin Server with config: $SERVER_CONF_FILE"
cd "$GREMLIN_SERVER_DIR" || { echo "Server directory not found!"; exit 1; }

# Start in foreground (so logs print to your terminal)
bin/gremlin-server.sh "$SERVER_CONF_FILE"
