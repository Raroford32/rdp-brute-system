#!/bin/bash

# Remote Management System - One-Click Installation Script
# Optimized for maximum performance and security

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  Remote Management System Installer    ║${NC}"
echo -e "${GREEN}║  One-Click Server Setup                ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
   echo -e "${RED}Please run as root (use sudo)${NC}"
   exit 1
fi

# Detect OS
if [ -f /etc/debian_version ]; then
    OS="debian"
elif [ -f /etc/redhat-release ]; then
    OS="redhat"
else
    echo -e "${RED}Unsupported OS${NC}"
    exit 1
fi

echo -e "${YELLOW}[1/7] Installing system dependencies...${NC}"
if [ "$OS" = "debian" ]; then
    apt-get update -qq
    apt-get install -y -qq curl wget git build-essential python3 python3-pip nodejs npm golang-go sqlite3 nginx certbot python3-certbot-nginx ufw fail2ban
else
    yum install -y curl wget git gcc make python3 python3-pip nodejs npm golang sqlite nginx certbot python3-certbot-nginx firewalld fail2ban
fi

echo -e "${YELLOW}[2/7] Setting up firewall...${NC}"
if [ "$OS" = "debian" ]; then
    ufw --force enable
    ufw allow 22/tcp
    ufw allow 8443/tcp
    ufw allow 443/tcp
    ufw allow 80/tcp
    ufw reload
else
    systemctl start firewalld
    firewall-cmd --permanent --add-port=22/tcp
    firewall-cmd --permanent --add-port=8443/tcp
    firewall-cmd --permanent --add-port=443/tcp
    firewall-cmd --permanent --add-port=80/tcp
    firewall-cmd --reload
fi

echo -e "${YELLOW}[3/7] Creating application directory...${NC}"
mkdir -p /opt/remote-management
cd /opt/remote-management

echo -e "${YELLOW}[4/7] Installing server components...${NC}"
cp -r /workspace/server /opt/remote-management/
cp -r /workspace/dashboard /opt/remote-management/
cp -r /workspace/client /opt/remote-management/

cd /opt/remote-management/server
npm install --production

cd /opt/remote-management/dashboard
npm install
npm run build

echo -e "${YELLOW}[5/7] Building client payload generator...${NC}"
cd /opt/remote-management/client
go build -ldflags="-s -w" -o ../server/payload-generator payload-generator.go

echo -e "${YELLOW}[6/7] Setting up database...${NC}"
cd /opt/remote-management/server
node setup-db.js

echo -e "${YELLOW}[7/7] Creating systemd service...${NC}"
cat > /etc/systemd/system/remote-management.service << EOF
[Unit]
Description=Remote Management System
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/remote-management/server
ExecStart=/usr/bin/node server.js
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=remote-management
Environment="NODE_ENV=production"

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable remote-management
systemctl start remote-management

# Generate SSL certificate
echo -e "${YELLOW}Generating self-signed SSL certificate...${NC}"
mkdir -p /opt/remote-management/server/ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout /opt/remote-management/server/ssl/server.key \
    -out /opt/remote-management/server/ssl/server.cert \
    -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"

# Get server IP
SERVER_IP=$(hostname -I | awk '{print $1}')

echo ""
echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  Installation Complete!                ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}Dashboard URL:${NC} https://${SERVER_IP}:8443"
echo -e "${GREEN}Default Login:${NC} admin / changeme"
echo ""
echo -e "${YELLOW}Please change the default password after first login!${NC}"
echo ""
echo -e "${GREEN}Service Status:${NC}"
systemctl status remote-management --no-pager | head -n 5

echo ""
echo -e "${GREEN}To manage the service:${NC}"
echo "  Start:   systemctl start remote-management"
echo "  Stop:    systemctl stop remote-management"
echo "  Restart: systemctl restart remote-management"
echo "  Logs:    journalctl -u remote-management -f"