#!/usr/bin/env bash
# deploy.sh — Automation script to compile, build, and deploy the MapReduce cluster.

set -euo pipefail

# Formatting colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color
BLUE='\033[0;34m'
YELLOW='\033[0;33m'

echo -e "${BLUE}==================================================${NC}"
echo -e "${BLUE}     MapReduce Cluster Deployment Pipeline        ${NC}"
echo -e "${BLUE}==================================================${NC}"

# 1. Check Docker Daemon status
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}[ERROR] Docker daemon is not running. Please start Docker first.${NC}"
    exit 1
fi

# 2. Initialize environment file if missing
echo -e "\n${YELLOW}[1/5] Checking environment configuration...${NC}"
if [ ! -f .env ]; then
    echo -e "${YELLOW}No .env file found at root. Creating from .env.example...${NC}"
    if [ -f .env.example ]; then
        cp .env.example .env
        echo -e "${GREEN}✓ Created .env successfully.${NC}"
    else
        echo -e "${RED}[ERROR] .env.example is missing. Cannot initialize .env.${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✓ .env file exists.${NC}"
fi

# Load variables from .env
# shellcheck disable=SC1091
source .env

# 3. Build coordinator and worker binaries locally
echo -e "\n${YELLOW}[2/5] Compiling local binaries...${NC}"
if cd MapReduce; then
    CGO_ENABLED=0 GOOS=linux go build -o bin/coordinator ./cmd/coordinator
    CGO_ENABLED=0 GOOS=linux go build -o bin/worker ./cmd/worker
    cd ..
    echo -e "${GREEN}✓ Coordinator & Worker compiled successfully at MapReduce/bin/${NC}"
else
    echo -e "${RED}[ERROR] Failed to navigate to MapReduce directory.${NC}"
    exit 1
fi

# 4. Stop existing services to avoid collisions
echo -e "\n${YELLOW}[3/5] Cleaning up any existing containers...${NC}"
docker compose down

# 5. Boot up Docker Compose
echo -e "\n${YELLOW}[4/5] Starting Docker Compose cluster...${NC}"
if docker compose up --build -d; then
    echo -e "${GREEN}✓ Containers built and started successfully.${NC}"
else
    echo -e "${RED}[ERROR] Failed to start Docker Compose containers.${NC}"
    exit 1
fi

# 6. Verify status
echo -e "\n${YELLOW}[5/5] Checking container status...${NC}"
docker compose ps

echo -e "\n${GREEN}==================================================${NC}"
echo -e "${GREEN}     MapReduce Cluster Deployed Successfully!     ${NC}"
echo -e "${GREEN}==================================================${NC}"
echo -e "Services are mapped to the following local ports:"
echo -e "  - ${BLUE}coordinator (gRPC Scheduler)${NC} : localhost:${COORDINATOR_PORT:-9090}"
echo -e "  - ${BLUE}MinIO Console (S3 Browser)${NC}   : http://localhost:${MINIO_CONSOLE_PORT:-9001} (User/Pass: ${MINIO_ROOT_USER:-minioadmin}/${MINIO_ROOT_PASSWORD:-minioadmin})"
echo -e "  - ${BLUE}Prometheus (Metrics Server)${NC}  : http://localhost:${PROMETHEUS_PORT:-9092}"
echo -e "  - ${BLUE}Grafana (Dashboards)${NC}         : http://localhost:${GRAFANA_PORT:-3000} (User/Pass: admin/${GRAFANA_ADMIN_PASSWORD:-admin})"
echo -e "\nTo inspect the logs of running worker nodes, use:"
echo -e "  ${BLUE}docker compose logs -f worker${NC}"
echo -e "${BLUE}==================================================${NC}"
