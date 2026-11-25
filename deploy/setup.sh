#!/usr/bin/env bash
# One-time setup of a fresh Ubuntu 24.04 server. Run as root:
#   sudo REPO_URL=git@github.com:<owner>/<repo>.git bash setup.sh
# Safe to run again: each step skips what is already done.
set -euo pipefail

REPO_URL=${REPO_URL:-git@github.com:annoyinglyanonymous/Signal-based-Outbound.git}
BRANCH=${BRANCH:-main}
BASE=/opt/truckshield
KEY=$BASE/.ssh/id_ed25519
as_app() { sudo -u truckshield -H "$@"; }

echo "== packages and timezone"
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip git util-linux curl >/dev/null
timedatectl set-timezone Asia/Kathmandu

echo "== app user and folders"
id truckshield >/dev/null 2>&1 || useradd --system --create-home --home-dir "$BASE" --shell /bin/bash truckshield
install -d -o truckshield -g truckshield "$BASE/data" "$BASE/.ssh"
touch "$BASE/run.lock" && chown truckshield:truckshield "$BASE/run.lock"

echo "== swap (DuckDB is capped at 1 GB, but swap keeps a spike from killing the run)"
if [ -z "$(swapon --show)" ]; then
  fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap -q /swapfile && swapon /swapfile
  grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

free_gb=$(df --output=avail -BG "$BASE" | tail -1 | tr -dc '0-9')
[ "$free_gb" -lt 20 ] && echo "WARNING: only ${free_gb} GB free; the weekly census run needs about 15 GB"

echo "== code"
[ -f "$KEY" ] || as_app ssh-keygen -q -t ed25519 -N "" -f "$KEY" -C truckshield-deploy
grep -q github.com "$BASE/.ssh/known_hosts" 2>/dev/null || as_app sh -c "ssh-keyscan -q github.com >> $BASE/.ssh/known_hosts"
if [ ! -d "$BASE/app/.git" ]; then
  if ! as_app git clone -q --branch "$BRANCH" "$REPO_URL" "$BASE/app"; then
    echo
    echo "Could not clone $REPO_URL. Add this key to the GitHub repo"
    echo "(Settings > Deploy keys > Add deploy key, read-only), then run this script again:"
    echo
    cat "$KEY.pub"
    exit 1
  fi
fi

echo "== Python environment"
[ -d "$BASE/venv" ] || as_app python3 -m venv "$BASE/venv"
as_app "$BASE/venv/bin/pip" install -q --upgrade pip
as_app "$BASE/venv/bin/pip" install -q -r "$BASE/app/requirements.txt"

echo "== settings"
if [ ! -f /etc/truckshield.env ]; then
  install -m 640 -o root -g truckshield "$BASE/app/.env.example" /etc/truckshield.env
fi

echo "== systemd timers"
install -m 644 "$BASE"/app/deploy/systemd/*.service "$BASE"/app/deploy/systemd/*.timer /etc/systemd/system/
systemctl daemon-reload

if grep -Eq '^SUPABASE_DB_URL=.+' /etc/truckshield.env; then
  systemctl enable --now truckshield-daily.timer truckshield-weekly.timer
  systemctl list-timers 'truckshield*' --no-pager
  echo
  echo "Done. First load: sudo systemctl start truckshield-weekly.service  (2 to 3 hours)"
  echo "Follow it with:  journalctl -u truckshield-weekly -f"
else
  echo
  echo "Almost done. Fill in /etc/truckshield.env (sudo nano /etc/truckshield.env), then run this script again."
fi
