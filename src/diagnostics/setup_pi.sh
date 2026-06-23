#!/bin/bash
# Noble Gas Braider Monitor — Pi Setup Script
# Run once after copying files to the Pi:
#   bash setup_pi.sh

set -e

echo "=================================================="
echo " Noble Gas Braider Monitor — Pi Setup"
echo "=================================================="

echo ""
echo ">> Installing Python dependencies..."
pip3 install pycomm3 flask --break-system-packages

echo ""
echo ">> Creating log directory..."
mkdir -p ~/braider_logs

echo ""
echo ">> Installing systemd service..."
sudo cp braider_monitor.service /etc/systemd/system/braider_monitor.service

# Update the service file user if not 'pi'
CURRENT_USER=$(whoami)
if [ "$CURRENT_USER" != "pi" ]; then
    sudo sed -i "s/User=pi/User=$CURRENT_USER/" /etc/systemd/system/braider_monitor.service
    sudo sed -i "s|/home/pi|/home/$CURRENT_USER|g" /etc/systemd/system/braider_monitor.service
fi

sudo systemctl daemon-reload
sudo systemctl enable braider_monitor.service
sudo systemctl start braider_monitor.service

echo ""
echo ">> Waiting 5 seconds for service to start..."
sleep 5

echo ""
echo ">> Service status:"
sudo systemctl status braider_monitor.service --no-pager

echo ""
echo "=================================================="
echo " Setup complete!"
echo " Dashboard: http://$(hostname -I | awk '{print $1}'):5000"
echo " Logs:      ~/braider_logs/"
echo " Service:   sudo systemctl status braider_monitor"
echo " Restart:   sudo systemctl restart braider_monitor"
echo " Logs live: journalctl -u braider_monitor -f"
echo "=================================================="
