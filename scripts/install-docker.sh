#!/usr/bin/env bash
# 모니터링 VM(Ubuntu 24.04)에 Docker CE 설치. root로 1회 실행:
#   ssh -t monitoring@172.30.0.30 'sudo bash ~/monitoring/scripts/install-docker.sh'
# 출처: https://docs.docker.com/engine/install/ubuntu/
set -euo pipefail

TARGET_USER="${SUDO_USER:-monitoring}"

apt-get update
apt-get install -y ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc

. /etc/os-release
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
  > /etc/apt/sources.list.d/docker.list

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

systemctl enable --now docker
usermod -aG docker "$TARGET_USER"   # 이후 compose는 sudo 없이. 재로그인 뒤 반영

docker --version
docker compose version
echo "완료. ${TARGET_USER} 계정은 재로그인 후 sudo 없이 docker 사용 가능"
