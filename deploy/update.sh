#!/usr/bin/env bash
# Deploy the latest main, or roll back to an earlier commit. Run as root:
#   sudo bash /opt/truckshield/app/deploy/update.sh            latest main
#   sudo bash /opt/truckshield/app/deploy/update.sh 1a2b3c4    a specific commit (rollback)
set -euo pipefail
BASE=/opt/truckshield
REF=${1:-origin/main}
as_app() { sudo -u truckshield -H "$@"; }

cd "$BASE/app"
before=$(git rev-parse --short HEAD)
as_app git fetch -q origin
as_app git checkout -q --detach "$REF"
as_app "$BASE/venv/bin/pip" install -q -r requirements.txt
install -m 644 deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
systemctl daemon-reload
systemctl restart truckshield-daily.timer truckshield-weekly.timer
echo "deployed $(git rev-parse --short HEAD) (was $before). To roll back: sudo bash $0 $before"
systemctl list-timers 'truckshield*' --no-pager
