# 📧 PEC Archiver - Archiviazione Automatica PEC Aruba

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Sistema batch containerizzato per l'archiviazione automatica giornaliera delle caselle PEC Aruba. Progettato per gestire fino a **20.000 PEC al giorno** con alta affidabilità.

## 📌 Funzionalità Principali

- ✅ **Archiviazione automatica** - Backup giornaliero programmato alle 01:00
- ✅ **Backup manuale** - Script dedicato per backup di date specifiche o intervalli
- ✅ **Multi-account** - Gestione simultanea di multiple caselle PEC
- ✅ **Indicizzazione completa** - Generazione automatica di index.csv e index.json
- ✅ **Compressione sicura** - Archivi .tar.gz con digest SHA256
- ✅ **Containerizzato** - Deployment semplice con Docker
- ✅ **Resiliente** - Retry automatico con backoff esponenziale
- ✅ **Sicuro** - Connessioni IMAP SSL/TLS, supporto variabili d'ambiente

## 🚀 Quick Start

### 1. Clonare il repository

```bash
git clone https://github.com/radicisoluzioni/SogertPecBackup.git
cd SogertPecBackup
```

### 2. Configurare le credenziali

```bash
cp config/config.yaml.example config/config.yaml
# Modificare config.yaml con i propri dati
```

### 3. Avviare il servizio

```bash
# Creare la directory di archivio
sudo mkdir -p /srv/pec-archive

# Avviare con Docker Compose
docker compose up -d
```

### 4. Verificare lo stato

```bash
docker compose logs -f pec-archiver
```

## 📂 Struttura dell'Archivio

```
/data/pec-archive/
└── <account>/
    └── <YYYY>/
        └── <YYYY-MM-DD>/
            ├── INBOX/
            │   ├── 001_message.eml
            │   └── 002_message.eml
            ├── Posta_inviata/
            │   └── 001_message.eml
            ├── index.csv
            ├── index.json
            ├── summary.json
            ├── archive-<account>-<date>.tar.gz
            └── digest.sha256
```

## ⚙️ Configurazione

Il file `config/config.yaml` contiene tutte le impostazioni:

```yaml
# Percorso base per l'archivio
base_path: /data/pec-archive

# Numero di worker paralleli
concurrency: 4

# Policy di retry per errori di connessione
retry_policy:
  max_retries: 3
  initial_delay: 5      # secondi
  backoff_multiplier: 2 # backoff esponenziale

# Impostazioni IMAP
imap:
  timeout: 30           # secondi
  batch_size: 100       # messaggi per batch

# Orario di esecuzione dello scheduler
scheduler:
  run_time: "01:00"

# Account PEC da archiviare
accounts:
  - username: account1@pec.it
    password: ${PEC_PASSWORD_1}  # Usa variabile d'ambiente
    host: imaps.pec.aruba.it
    port: 993
    folders:
      - INBOX
      - Posta inviata
```

## 🐳 Docker Compose

```yaml
version: "3.9"

services:
  pec-archiver:
    build: .
    image: pec-archiver:latest
    container_name: pec-archiver
    restart: unless-stopped
    environment:
      - TZ=Europe/Rome
      - PEC_ARCHIVE_CONFIG=/app/config/config.yaml
      - PEC_PASSWORD_1=your_password_here
    volumes:
      - ./config:/app/config:ro
      - /srv/pec-archive:/data/pec-archive
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

## 📆 Script di Backup

### Script Principale (`main.py`)

Esegue lo scheduler automatico o backup on-demand:

```bash
# Avviare lo scheduler (attende l'orario programmato)
python -m src.main

# Eseguire backup immediato (data di ieri)
python -m src.main --run-now

# Backup di una data specifica
python -m src.main --run-now --date 2024-01-15
```

| Opzione | Descrizione |
|---------|-------------|
| `--run-now`, `-r` | Esegue il backup immediatamente |
| `--date`, `-d` | Data da archiviare (formato YYYY-MM-DD) |
| `--config`, `-c` | Percorso al file di configurazione |
| `--log-level`, `-l` | Livello di logging (DEBUG, INFO, WARNING, ERROR) |

### Script Backup Intervalli (`backup_range.py`)

Per casi di emergenza o recupero di periodi specifici:

```bash
# Backup di un giorno specifico
python -m src.backup_range --date 2024-01-15

# Backup di un intervallo di date
python -m src.backup_range --date-from 2024-01-15 --date-to 2024-01-22

# Backup di una settimana
python -m src.backup_range --date-from 2024-01-15 --date-to 2024-01-21
```

| Opzione | Descrizione |
|---------|-------------|
| `--date`, `-d` | Data singola da backuppare (formato YYYY-MM-DD) |
| `--date-from`, `-f` | Data iniziale dell'intervallo (formato YYYY-MM-DD) |
| `--date-to`, `-t` | Data finale dell'intervallo (formato YYYY-MM-DD) |
| `--config`, `-c` | Percorso al file di configurazione |
| `--log-level`, `-l` | Livello di logging (DEBUG, INFO, WARNING, ERROR) |

### Esempi con Docker

```bash
# Backup immediato di ieri
docker compose exec pec-archiver python -m src.main --run-now

# Backup di una data specifica
docker compose exec pec-archiver python -m src.main --run-now --date 2024-01-15

# Backup di un intervallo
docker compose exec pec-archiver python -m src.backup_range \
    --date-from 2024-01-15 --date-to 2024-01-21
```

## 📑 File Generati

| File | Descrizione |
|------|-------------|
| `*.eml` | Messaggi email in formato standard |
| `index.csv` | Indice dei messaggi in formato CSV |
| `index.json` | Indice dei messaggi in formato JSON |
| `summary.json` | Riepilogo dell'operazione con statistiche |
| `archive-<account>-<date>.tar.gz` | Archivio compresso della giornata |
| `digest.sha256` | Hash SHA256 per verifica integrità |

## 🧱 Architettura

```
┌─────────────────┐
│   Scheduler     │ ──► Esegue alle 01:00
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Account Workers │ ──► Parallelismo configurabile
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌───────┐ ┌───────┐
│ IMAP  │ │Storage│
│Client │ │Module │
└───────┘ └───────┘
              │
    ┌─────────┼─────────┐
    ▼         ▼         ▼
┌───────┐ ┌───────┐ ┌───────┐
│Indexer│ │Compress││Report │
└───────┘ └───────┘ └───────┘
```

### Moduli

| Modulo | Descrizione |
|--------|-------------|
| `scheduler.py` | Gestisce la pianificazione giornaliera |
| `worker.py` | Processa singoli account PEC |
| `imap_client.py` | Gestisce connessioni IMAP con retry |
| `storage.py` | Salva messaggi e gestisce directory |
| `indexing.py` | Genera indici CSV e JSON |
| `compression.py` | Crea archivi tar.gz e digest SHA256 |
| `reporting.py` | Genera summary.json e report aggregati |
| `config.py` | Carica e valida configurazione YAML |

## 🔐 Sicurezza

- **Connessioni crittografate**: IMAP SSL/TLS (porta 993)
- **Variabili d'ambiente**: Password non in chiaro nel config
- **Config read-only**: Volume montato in sola lettura
- **Utente non-root**: Container eseguito come `appuser`
- **Digest SHA256**: Verifica integrità degli archivi

## ♻️ Gestione Errori

- **Retry automatico**: Backoff esponenziale configurabile
- **Logging completo**: Tutti gli errori vengono registrati
- **Report dettagliati**: Errori salvati in `summary.json`
- **Graceful degradation**: Continua con altri account in caso di errore

## 📈 Performance

- **Ottimizzato per 20.000+ PEC/giorno**
- **Parallelismo configurabile** (default: 4 worker)
- **Batch IMAP regolabili** (default: 100 messaggi)
- **Timeout configurabile** (default: 30 secondi)

## 🛠️ Sviluppo Locale

### Prerequisiti

- Python 3.11+
- pip

### Setup

```bash
# Installare dipendenze
pip install -r requirements.txt

# Eseguire test
python -m pytest tests/ -v

# Eseguire localmente
python -m src.main --run-now --config config/config.yaml
```

### Test

```bash
# Eseguire tutti i test
python -m pytest tests/ -v

# Test con coverage
python -m pytest tests/ -v --cov=src
```

## 📋 Requisiti di Sistema

- Docker 20.10+
- Docker Compose 2.0+
- Spazio disco sufficiente per `/srv/pec-archive`
- Connettività di rete verso server IMAP PEC

## 📄 Licenza

Questo progetto è distribuito sotto licenza MIT. Vedere il file `LICENSE` per maggiori dettagli.

## 🤝 Contribuire

Le contribuzioni sono benvenute! Si prega di aprire una issue per discutere le modifiche proposte.

---

**Sviluppato da [Radici Soluzioni](https://github.com/radicisoluzioni)**
