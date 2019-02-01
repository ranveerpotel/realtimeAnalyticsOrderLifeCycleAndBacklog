#!/bin/bash
# ScyllaDB node initialization script
set -euo pipefail

# Configure scylla.yaml with DC-aware settings
cat >> /etc/scylla/scylla.yaml <<EOF
cluster_name: 'realtimeanalytics'
num_tokens: 256
endpoint_snitch: Ec2MultiRegionSnitch
authenticator: PasswordAuthenticator
authorizer: CassandraAuthorizer
EOF

# Apply keyspace schema once the node is up
until cqlsh -e "describe keyspaces" 2>/dev/null; do
    sleep 5
done

cqlsh -f /opt/schema.cql
