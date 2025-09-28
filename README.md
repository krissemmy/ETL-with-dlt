
# Ethereum Blockchain ETL Pipeline

A simple ETL pipeline that extracts Ethereum block data and loads it into PostgreSQL for analysis with Grafana.

## Prerequiste

- Docker and Docker-Compose
- Python

## Quick Start

### 1. Setup Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

```
# Clone repo
git clone https://github.com/krissemmy/ETL-with-dlt.git
cd ETL-with-dlt

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Services

```bash
# Start PostgreSQL and Grafana
docker compose up -d
```

### 3. Configure Database

The pipeline uses these default PostgreSQL credentialsm comfigure at `.dlt/secrets.toml`:
```
cat > .dlt/secrets.toml <<EOF
[destination.postgres.credentials]
database = "dwh"
username = "postgres"
password = "postgres"
host = "localhost"
port = 5432
connect_timeout = 15
sslmode = "disable"
EOF
```

### 4. Run Pipeline

**One-time run:**

```bash
python rest_api_pipeline.py
```

**Scheduled run (every 30 minutes):**

```bash
# Add to crontab
crontab -e

# Add this line:
*/30 * * * * /bin/bash -c 'echo "$(date) - Starting cron job for Ethereum block data" >> ~/logs/dq_checrest_api_pipeline.log' && /home/ubuntu/venv/bin/python /home/ubuntu/ETL-with-dlt/rest_api_pipeline.py >> ~/logs/rest_api_pipeline.log 2>&1
```

## Grafana Setup

1. **Access Grafana:** http://localhost:3000
   - Username: `admin`
   - Password: `admin`

2. **Add PostgreSQL Data Source:**
   - Go to `Configuration > Data Sources > Add data source`
   - Select PostgreSQL
   - Host: `postgres:5432`
   - Database: `dwh`
   - User: `postgres`
   - Password: `postgres`
   - Schema: `ethereum`
   - Test & Save

3. **Create Visualization:**
   - Go to `Dashboards > New > Add visualization`
   - Select your PostgreSQL data source
   - Use this query for highest transaction count per day:

   ```sql
   SELECT 
     DATE(to_timestamp(timestamp)) as day,
     MAX(transactions_count) as max_transactions
   FROM ethereum.blocks 
   GROUP BY DATE(to_timestamp(timestamp))
   ORDER BY day DESC
   ```

## What It Does

- Fetches the latest 150 Ethereum blocks from a public RPC endpoint
- Extracts block metadata (number, timestamp, gas usage, transaction count)
- Loads data into PostgreSQL with automatic deduplication
- Provides real-time blockchain analytics via Grafana

## Data Schema

The `ethereum.blocks` table contains:

- `number`: Block number
- `timestamp`: Unix timestamp
- `transactions_count`: Number of transactions
- `gas_used`: Gas consumed
- `gas_limit`: Gas limit
- `size`: Block size in bytes
- `hash`: Block hash
- `parent_hash`: Previous block hash


## (Optional) To install Docker and DOcker Compose on ur server

```
#!/bin/bash

# Update system
sudo apt-get update
sudo apt-get upgrade -y

### REemove unofficial docker packages 
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do sudo apt-get remove $pkg; done

# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

sudo apt-get install make docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y
source ~/.bashrc
```