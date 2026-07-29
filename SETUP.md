# Setup Guide — DataHub DAG Generator Agent

Local environment setup for dev/testing. Follow the steps in order to avoid the common issues noted below.

## 1. System Requirements

Supports macOS, Linux, and Windows (via WSL2). Team members on different OSes follow the same workflow from step 3 onward — only the initial Docker/uv install differs.

- Docker (Docker Desktop on macOS/Windows, Docker Engine on Linux) installed and running
- **Python 3.10+** (required — the latest DataHub CLI no longer supports Python 3.9 or below)
- `uv` to manage Python versions (recommended for the whole team, to avoid everyone using a different venv setup)
- At least 13GB free disk space for Docker, 8GB free RAM

### 1a. Installing Docker per OS

**macOS:**
Download Docker Desktop: https://www.docker.com/products/docker-desktop
```bash
brew install --cask docker-desktop
```
Open the Docker Desktop app and wait until the whale icon shows "Running".

**Windows (WSL2 is required — do not run commands directly in PowerShell/CMD):**
1. Install WSL2 first (open PowerShell as Administrator):
   ```powershell
   wsl --install
   ```
   Restart if prompted.
2. Install Ubuntu from the Microsoft Store (or it may already be included by `wsl --install`).
3. Download Docker Desktop for Windows: https://www.docker.com/products/docker-desktop
   During install, make sure **"Use WSL 2 based engine"** is checked.
4. After installing, open Docker Desktop → Settings → Resources → WSL Integration → enable it for your Ubuntu distro.
5. Run every command in this guide **inside the Ubuntu (WSL) terminal**, not PowerShell/CMD.

**Linux (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install docker.io docker-compose-plugin
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
```
After the last command, log out/back in (or run `newgrp docker`) so you don't need `sudo` for every docker command.

Verify Docker is working (same for all 3 OSes):
```bash
docker ps
```

### 1b. Installing `uv` per OS

**macOS:**
```bash
brew install uv
```

**Linux / WSL (Windows):**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
After installing, open a new terminal or run:
```bash
source $HOME/.local/bin/env
```

Verify (same for all 3 OSes):
```bash
uv --version
```

Install Python 3.11 via uv (same for all 3 OSes):
```bash
uv python install 3.11
uv python list
```

## 2. Clone the repo

```bash
git clone git@github.com:tdm291104/datahub-dag-generator.git
cd datahub-dag-generator
```

## 3. Create a virtual environment — use Python 3.11 explicitly

```bash
uv venv datahub-env --python 3.11
source datahub-env/bin/activate
python3 --version    # must show Python 3.11.x — NOT macOS's system Python 3.9
```

⚠️ **Common mistake**: using `python3 -m venv` instead of `uv venv` will make macOS default to its system Python 3.9 (built with LibreSSL, incompatible with the newer DataHub CLI). Always use `uv venv --python 3.11` as shown above.

## 4. Install Python dependencies

```bash
pip install -r requirements.txt
```

This installs three packages:

| Package | Purpose |
|---|---|
| `acryl-datahub[sqlalchemy]==1.5.0.6` | DataHub CLI + Python SDK for lineage traversal and write-back |
| `anthropic==0.120.2` | Claude API client — drives the agentic loop |
| `mcp==2.0.0` | MCP Python client — connects to the DataHub MCP Server subprocess |

**The DataHub MCP Server** (`mcp-server-datahub`) runs as a subprocess via `uvx` — no separate install needed. `uvx` ships with `uv` and downloads the package on first use (~30 seconds, cached afterward).

⚠️ **Common mistake**: installing just `uv pip install acryl-datahub` (without `[sqlalchemy]`) will later fail with:
```
ModuleNotFoundError: No module named 'sqlalchemy'
```
Use `pip install -r requirements.txt` which already includes the correct extras.

⚠️ **Version must match the server**: the DataHub server (Docker quickstart) currently runs `v1.5.0.6`. The CLI needs to match (`==1.5.0.6`); otherwise you'll get a "Client-Server Incompatible" warning.

Verify:
```bash
datahub --version
# should print: acryl-datahub, version 1.5.0.6
```

## 5. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your values:

```
ANTHROPIC_API_KEY=sk-ant-...        # get yours at https://console.anthropic.com
DATAHUB_SERVER=http://localhost:8080  # default when running DataHub locally
```

Load the variables into your current shell (do this in every new terminal session):
```bash
export $(cat .env | grep -v '#' | xargs)
```

Or add it to `~/.zshrc` / `~/.bashrc` to load automatically:
```bash
echo 'export ANTHROPIC_API_KEY=sk-ant-...' >> ~/.zshrc
```

⚠️ **`.env` is in `.gitignore`** — it will never be committed. Never paste your API key directly into code or commit it.

## 6. Disable DataHub telemetry (optional but recommended — avoids commands "hanging" due to network timeouts)

```bash
export DATAHUB_TELEMETRY_ENABLED=false
```
Add this line to `~/.zshrc` (or `~/.bashrc` on Linux) so you don't need to set it again in every new session:
```bash
echo 'export DATAHUB_TELEMETRY_ENABLED=false' >> ~/.zshrc
```

## 7. Prepare Docker — check disk space before quickstart

DataHub quickstart requires at least **13GB of free space** in the Docker disk image.

```bash
docker system df
```

If you have less than 13GB reclaimable/available, clean up first:
```bash
docker builder prune -a
docker system prune -a
```

Recommended: go to **Docker Desktop → Settings → Resources → Advanced** and increase the **Disk usage limit** to 60GB+ to avoid running into this again during the hackathon.

⚠️ **Windows/WSL2 specific**: the WSL virtual disk (`ext4.vhdx`) can grow over time and won't automatically shrink back, even after deleting data. If your C: drive later fills up unexpectedly, compact the virtual disk by running this in PowerShell (as Admin) after shutting down WSL with `wsl --shutdown`:
```powershell
diskpart
# inside diskpart:
select vdisk file="C:\Users\<user>\AppData\Local\Docker\wsl\data\ext4.vhdx"
compact vdisk
```

## 8. Run DataHub Quickstart

```bash
datahub docker quickstart
```

The first run takes 10-20 minutes (pulling several Docker images). Wait until you see:
```
✔ DataHub is now running
```

Access the UI: **http://localhost:9002**
```
username: datahub
password: datahub
```

### Stop or remove DataHub

Stop the containers while keeping their data (so a later `datahub docker quickstart` reuses it):
```bash
docker compose -p datahub -f ~/.datahub/quickstart/docker-compose.yml --profile quickstart down
```

Remove DataHub and its local data completely:
```bash
datahub docker nuke
```

## 9. Load the sample nyc-taxi dataset

```bash
mkdir -p data && cd data
git clone --depth 1 --filter=blob:none --sparse https://github.com/datahub-project/static-assets.git
cd static-assets
git sparse-checkout set datasets/nyc-taxi
cd datasets/nyc-taxi
```

Generate the staleness variant (no Kaggle download needed):
```bash
python create_db.py --pipeline-from-existing
```

Ingest both variants:
```bash
datahub ingest -c ingest.yaml
datahub ingest -c ingest_pipeline.yaml
```

Add lineage:
```bash
python add_lineage.py --all
```

Add tags and glossary terms — use `setup_demo_metadata.py` from the repo root, **not** the dataset's `add_metadata.py`. The original script emits one tag at a time, causing each emission to overwrite the previous. The fixed version collects all tags per table and emits them in one call:
```bash
cd -    # back to repo root
python setup_demo_metadata.py
```

Verify in the UI (http://localhost:9002): search `mart_daily_summary`, check the **Lineage** tab (should show raw_trips → staging_trips → mart_daily_summary) and the **Tags** panel (should show `daily_refresh`, `pipeline_stage`). The **Glossary** panel should show `Freshness SLA`, `Empty Load`, `Pipeline Stage`.

## 10. What must NEVER be committed to git

`.gitignore` already includes the following lines — **do not remove them**:
```
datahub-env/
__pycache__/
*.pyc
.env
.datahubenv
data/
```

Before committing anything, always run `git status` and confirm `datahub-env/` and `data/` are NOT in the list of files staged for commit.

## Troubleshooting quick reference

| Error | Cause | Fix |
|---|---|---|
| `ERROR: ANTHROPIC_API_KEY is not set` | Missing env var | `export ANTHROPIC_API_KEY=sk-ant-...` or load `.env` with `export $(cat .env \| grep -v '#' \| xargs)` |
| `ValueError: ANTHROPIC_API_KEY environment variable is not set` | Same as above | Same fix |
| `NotOpenSSLWarning` when running `datahub version` | Using macOS system Python 3.9 (LibreSSL) | Recreate the venv with `uv venv --python 3.11` |
| `datahub version` hangs with no output | The subcommand tries to reach the server; use `datahub --version` instead | Not a real error, safe to ignore |
| `ModuleNotFoundError: No module named 'sqlalchemy'` | Installed `acryl-datahub` without the extra | `pip install -r requirements.txt` |
| `ModuleNotFoundError: No module named 'datahub_classify'` | Same cause — missing sqlalchemy extra | `pip install -r requirements.txt` |
| `ModuleNotFoundError: No module named 'mcp'` | Installed only DataHub, not full requirements | `pip install -r requirements.txt` |
| `uvx: command not found` | `uv` not installed or not on PATH | Install uv (step 1b) and open a new terminal |
| MCP server takes 30s on first tool call | `uvx` downloading `mcp-server-datahub` for the first time | Normal — cached after first run |
| `docker quickstart` reports insufficient disk space | Docker disk image nearly full (build cache) | `docker builder prune -a`, then raise the disk limit in Docker Desktop |
| `Client-Server Incompatible` | CLI version differs from server version | Install the matching `acryl-datahub==<server_version>` |
| Command hangs for a long time, logs full of `Retrying... track.datahubproject.io` | Telemetry trying to reach the network, blocked/timing out | `export DATAHUB_TELEMETRY_ENABLED=false` |
| `mart_daily_summary` has no tags in DataHub UI | `add_metadata.py` overwrites tags one by one | Run `python setup_demo_metadata.py` from the repo root instead |
