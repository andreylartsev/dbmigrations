#!/bin/sh
#
# Reproduces the local (Alpine container) environment needed to run the
# dbmigration test suites: git, PostgreSQL 18, a python venv with pytest +
# psycopg, the 'host.docker.internal' hosts entry and the Linux tool paths in
# dbmigration.toml.
#
# Safe to re-run: every step is idempotent. Must run as root (default in the
# container). The original dbmigration.toml is backed up once to
# /tmp/opencode/dbmigration.toml.bak so it can be restored later.

set -eu

REPO_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
TOML="$REPO_DIR/dbmigrations/dbmigration.toml"
BACKUP="/tmp/opencode/dbmigration.toml.bak"
VENV="/tmp/opencode/venv"
PGDATA="/var/lib/postgresql/data"
PGPORT=5432
PGUSER=postgres
PGDB=test1

echo "==> 1/6 Installing system packages (git, postgresql18, python3, pip)"
apk add git postgresql18 postgresql18-client python3 py3-pip

echo "==> 2/6 Initializing and starting PostgreSQL on port $PGPORT"
mkdir -p /run/postgresql
chown postgres:postgres /run/postgresql
if [ ! -f "$PGDATA/PG_VERSION" ]; then
    install -d -o postgres -g postgres /var/lib/postgresql
    su postgres -c "initdb -D '$PGDATA'"
fi

# Prepend trust rules (first match wins) only once
if ! grep -q "dbmigration-import-trust-rules" "$PGDATA/pg_hba.conf"; then
    TMP=$(mktemp)
    cat > "$TMP" <<EOF
# dbmigration-import-trust-rules (local dev only)
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
EOF
    cat "$PGDATA/pg_hba.conf" >> "$TMP"
    chown postgres:postgres "$TMP"
    mv "$TMP" "$PGDATA/pg_hba.conf"
fi

if ! su postgres -c "pg_ctl -D '$PGDATA' status" >/dev/null 2>&1; then
    su postgres -c "pg_ctl -D '$PGDATA' -l /var/lib/postgresql/pg.log start"
fi

echo "==> 3/6 Creating database '$PGDB' if missing"
if ! su postgres -c "psql -h 127.0.0.1 -U $PGUSER -tAc \"SELECT 1 FROM pg_database WHERE datname='$PGDB'\"" 2>/dev/null | grep -q 1; then
    su postgres -c "createdb -h 127.0.0.1 -U $PGUSER $PGDB"
fi

echo "==> 4/6 Adding host entries"
if ! grep -q "host.docker.internal" /etc/hosts; then
    echo "127.0.0.1 host.docker.internal" >> /etc/hosts
fi

echo "==> 5/6 Creating python venv with pytest + psycopg"
if [ ! -x "$VENV/bin/python" ]; then
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install --upgrade pip
    "$VENV/bin/pip" install pytest "psycopg[binary]"
fi

echo "==> 6/6 Pointing dbmigration.toml tools at the Linux binaries"
if [ ! -f "$BACKUP" ]; then
    cp "$TOML" "$BACKUP"
fi
"$VENV/bin/python" - "$TOML" <<'PY'
import sys

path = sys.argv[1]
with open(path, encoding="utf-8", newline="") as f:
    src = f.read()
lines = src.splitlines(keepends=True)
out = []
in_macbook_block = False
for line in lines:
    stripped = line.strip()
    eol = line[len(line.rstrip("\r\n")):]
    if stripped == '#executable = "/usr/bin/psql"':
        out.append('executable = "/usr/bin/psql"' + eol)
    elif "psql.exe" in stripped and not stripped.startswith("#"):
        out.append("#" + line)  # disable Windows psql entry
    elif stripped == '#executable = "/usr/bin/pg_restore"':
        out.append('executable = "/usr/bin/pg_restore"' + eol)
    elif stripped == '#args = ["--exit-on-error", "-d", "${dbname}", "${file}"]':
        out.append('args = ["--exit-on-error", "-d", "${dbname}", "${file}"]' + eol)
    elif "/opt/homebrew/bin/pg_restore" in stripped:
        out.append(("#" if not line.strip().startswith("#") else "") + line)
        if "executable" in stripped:
            in_macbook_block = True
    elif in_macbook_block and stripped.startswith("args"):
        out.append("#" + line)  # disable the OSX pg_restore args entry
        in_macbook_block = False
    else:
        out.append(line)
with open(path, "w", encoding="utf-8", newline="") as f:
    f.write("".join(out))
print("toml patched")
PY

echo
echo "Environment ready."
echo "Run unit tests:   (cd '$REPO_DIR/dbmigrations' && USER_PASSWORD=dummy '$VENV/bin/python' -m pytest unit_tests/ -q)"
echo "Run full tests:   (cd '$REPO_DIR/dbmigrations' && USER_PASSWORD=dummy '$VENV/bin/python' -m pytest tests/ -q)"