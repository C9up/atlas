#!/usr/bin/env bash
# A throwaway PostgreSQL that speaks TLS only and requires a client
# certificate, for tests/integration/pg-tls.test.ts.
#
#   scripts/pg-tls-stack.sh start <dir>   # certificates + container, writes <dir>/stack.json
#   scripts/pg-tls-stack.sh stop  <dir>
#
#   ATLAS_TEST_PG_TLS_DIR=<dir> pnpm exec vitest run tests/integration/pg-tls.test.ts
#
# Every key is generated into <dir>; nothing is committed. The server
# certificate names `localhost` only, so connecting to 127.0.0.1 is the
# name-mismatch case. `pg_hba.conf` has no plaintext rule and authenticates
# with the client certificate (`cert`): its CN is the database user.
#
# Needs openssl and docker (or podman, through CONTAINER=podman).
set -euo pipefail

CONTAINER="${CONTAINER:-docker}"
IMAGE="${PG_IMAGE:-postgres:16-alpine}"
PORT="${PG_TLS_PORT:-55433}"
NAME="atlas-pg-tls"

cmd="${1:-}"
DIR="${2:-}"
[ -n "$cmd" ] && [ -n "$DIR" ] || { echo "usage: $0 start|stop <dir>" >&2; exit 2; }

if [ "$cmd" = "stop" ]; then
	"$CONTAINER" rm -f "$NAME" >/dev/null 2>&1 || true
	exit 0
fi
[ "$cmd" = "start" ] || { echo "usage: $0 start|stop <dir>" >&2; exit 2; }

mkdir -p "$DIR/server"
cd "$DIR"

key() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null; }

# $1 name, $2 CA prefix, $3 subject, $4 extensions
issue() {
	key "$1.key"
	openssl req -new -key "$1.key" -subj "$3" -out "$1.csr" 2>/dev/null
	printf '%s\n' "$4" >"$1.ext"
	openssl x509 -req -in "$1.csr" -CA "$2.crt" -CAkey "$2.key" -CAcreateserial \
		-days 2 -sha256 -extfile "$1.ext" -out "$1.crt" 2>/dev/null
	rm -f "$1.csr" "$1.ext"
}

ca() {
	key "$1.key"
	openssl req -x509 -new -key "$1.key" -subj "$2" -days 2 -sha256 \
		-addext "basicConstraints=critical,CA:TRUE" \
		-addext "keyUsage=critical,keyCertSign,cRLSign" -out "$1.crt" 2>/dev/null
}

ca ca "/CN=atlas test CA"
ca other-ca "/CN=atlas unrelated CA"
issue server ca "/CN=localhost" "subjectAltName=DNS:localhost
extendedKeyUsage=serverAuth"
issue client ca "/CN=app" "extendedKeyUsage=clientAuth"
issue stranger other-ca "/CN=app" "extendedKeyUsage=clientAuth"
openssl pkcs8 -topk8 -in client.key -passout pass:secret -out client-encrypted.key 2>/dev/null

cp server.crt server.key ca.crt server/
cat >server/pg_hba.conf <<'EOF'
local   all  all        trust
hostssl all  all  all   cert
EOF
chmod 644 server/*

"$CONTAINER" rm -f "$NAME" >/dev/null 2>&1 || true
# `z` relabels the mount for SELinux (Fedora), where the container would see an
# empty directory otherwise; elsewhere it does nothing.
# PostgreSQL refuses a key it does not own (or root) with a mode above 0640,
# and a bind mount keeps the host's owner: copy the files first, then hand
# over to the image's entrypoint.
"$CONTAINER" run -d --name "$NAME" -p "127.0.0.1:$PORT:5432" \
	-e POSTGRES_USER=app -e POSTGRES_PASSWORD=unused -e POSTGRES_DB=app \
	-v "$DIR/server:/certs-src:ro,z" \
	--entrypoint /bin/sh "$IMAGE" -c '
		mkdir -p /certs && cp /certs-src/* /certs/ &&
		chown postgres:postgres /certs/* && chmod 600 /certs/server.key &&
		exec docker-entrypoint.sh postgres -c ssl=on \
			-c ssl_cert_file=/certs/server.crt -c ssl_key_file=/certs/server.key \
			-c ssl_ca_file=/certs/ca.crt -c hba_file=/certs/pg_hba.conf' >/dev/null

# Over TCP: the entrypoint's first, initialising server listens on the socket
# only, so this answers once the real one is up.
ready=""
for _ in $(seq 1 60); do
	if "$CONTAINER" exec "$NAME" pg_isready -h 127.0.0.1 -U app >/dev/null 2>&1; then
		ready=1
		break
	fi
	sleep 1
done
[ -n "$ready" ] || { "$CONTAINER" logs "$NAME" >&2; echo "PostgreSQL did not start" >&2; exit 1; }

cat >stack.json <<EOF
{ "port": $PORT, "dir": "$DIR" }
EOF
echo "PostgreSQL TLS on 127.0.0.1:$PORT, certificates in $DIR"
