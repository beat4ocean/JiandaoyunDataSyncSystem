#!/bin/bash

chmod +x /opt/JiandaoyunDataSyncSystem/run.sh

#sudo vim /etc/systemd/system/jiandaoyun-sync-system.service
sudo tee /etc/systemd/system/jiandaoyun-sync-system.service > /dev/null << 'EOF'
[Unit]
Description=Jiandaoyun Data Sync System
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=/opt/JiandaoyunDataSyncSystem
ExecStart=/opt/miniconda3/envs/python3/bin/python -u /opt/JiandaoyunDataSyncSystem/run.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

# 重新加载
sudo systemctl daemon-reload
# 启动服务
sudo systemctl enable --now jiandaoyun-sync-system.service
# 检查服务状态
sudo systemctl status jiandaoyun-sync-system.service
# 查看详细日志
sudo journalctl -u jiandaoyun-sync-system.service -f