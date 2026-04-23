#!/bin/bash

# ============================================================
#   Apache Airflow Info Script
#   Displays container status, URLs, and credentials
# ============================================================

CONTAINER_NAME="airflow-standalone2"
PASSWORD_FILE="/opt/airflow/simple_auth_manager_passwords.json.generated"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║        🚀  Apache Airflow - Access Info          ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# -------------------------------
# Check Docker
# -------------------------------
if ! command -v docker &>/dev/null; then
    echo "❌ Docker is not installed or not in PATH."
    exit 1
fi

# -------------------------------
# Check container running
# -------------------------------
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "❌ Container '${CONTAINER_NAME}' is NOT running."
    echo ""
    echo "   ▶ Start it with:"
    echo "     docker start ${CONTAINER_NAME}"
    echo ""
    exit 1
fi

echo "✅ Container Status : Running (${CONTAINER_NAME})"

# -------------------------------
# Get host port mapping
# -------------------------------
HOST_PORT=$(docker inspect \
  --format='{{range $p, $conf := .NetworkSettings.Ports}}{{if eq $p "8080/tcp"}}{{(index $conf 0).HostPort}}{{end}}{{end}}' \
  "${CONTAINER_NAME}")

# Fallback if port not found
HOST_PORT=${HOST_PORT:-8080}

# -------------------------------
# Get Local IP
# -------------------------------
HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}')

if [ -z "$HOST_IP" ]; then
    HOST_IP=$(ip route get 1 | awk '{print $7; exit}')
fi

# -------------------------------
# Get Public IP (multiple fallbacks)
# -------------------------------
PUBLIC_IP=$(curl -s --max-time 2 ifconfig.me || \
            curl -s --max-time 2 api.ipify.org || \
            curl -s --max-time 2 icanhazip.com)

if [ -z "$PUBLIC_IP" ]; then
    PUBLIC_IP="Unavailable"
fi

# -------------------------------
# Display URLs
# -------------------------------
echo ""
echo "🌐 Airflow URL (Local)  : http://${HOST_IP}:${HOST_PORT}"
echo "🌍 Airflow URL (Public) : http://${PUBLIC_IP}:${HOST_PORT}"
echo "👤 Username             : admin"

# -------------------------------
# Get password from container
# -------------------------------
RAW=$(docker exec "${CONTAINER_NAME}" cat "${PASSWORD_FILE}" 2>/dev/null)

if [ -z "$RAW" ]; then
    echo "🔑 Password             : ⚠️  Could not read password file"
    echo "   Expected at: ${PASSWORD_FILE}"
else
    PASSWORD=$(echo "$RAW" | python3 -c \
        "import sys,json; d=json.load(sys.stdin); print(list(d.values())[0])" \
        2>/dev/null)

    if [ -z "$PASSWORD" ]; then
        echo "🔑 Password             : $RAW"
    else
        echo "🔑 Password             : ${PASSWORD}"
    fi
fi

# -------------------------------
# Notes
# -------------------------------
echo ""
echo "──────────────────────────────────────────────────"
echo "ℹ️  Notes:"
echo "   • Public URL works only if port ${HOST_PORT} is open"
echo "   • Configure router port forwarding if running locally"
echo "   • Allow port in firewall / cloud security group"
echo "──────────────────────────────────────────────────"
echo ""
