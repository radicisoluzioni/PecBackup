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
- ✅ **Modalità S3 Sync** - Archiviazione locale non compressa + backup giornaliero su S3
- ✅ **Containerizzato** - Deployment semplice con Docker
- ✅ **Resiliente** - Retry automatico con backoff esponenziale
- ✅ **Sicuro** - Connessioni IMAP SSL/TLS, supporto variabili d'ambiente
- ✅ **Notifiche email** - Report giornalieri e alert in caso di errori
- ✅ **REST API** - Ricerca e download email archiviate via API

## 🌐 Modalità di Backup

### Modalità Standard (default)
- Download giornaliero delle email alle 01:00
- Salvataggio in formato .eml
- Generazione indici JSON/CSV
- Compressione in archivio .tar.gz
- Conservazione locale degli archivi compressi

### Modalità S3 Sync
- Download giornaliero delle email alle 01:00
- **Salvataggio locale in struttura diretta (senza cartelle per data)**
- **Mirror completo della casella - i messaggi non vengono mai cancellati localmente**
- Salvataggio in formato .eml non compresso localmente
- Generazione indici JSON/CSV globali
- Creazione archivio .tar.gz giornaliero (con struttura temporanea per data)
- **Upload automatico su Amazon S3 con organizzazione per data**
- **Eliminazione archivio locale dopo upload (ma non le email)**
- Ideale per mantenere un mirror locale completo e consultabile + backup cloud sicuro giornaliero

## 🚀 Quick Start

### 1. Clonare il repository

```bash
git clone https://github.com/radicisoluzioni/PecBackup.git
cd PecBackup
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

### Modalità Standard
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
            ├── archive-<account>-<date>.tar.gz  # Conservato localmente
            └── digest.sha256
```

### Modalità S3 Sync
```
/data/pec-archive/                         # Locale - Mirror diretto della casella
└── <account>/
    ├── INBOX/
    │   ├── 001_message.eml               # Non compresso, consultabile
    │   ├── 002_message.eml               # Tutti i messaggi mai cancellati
    │   └── 003_message.eml
    ├── Posta_inviata/
    │   ├── 001_message.eml
    │   └── 002_message.eml
    ├── index.csv                          # Indice globale
    └── index.json                         # Indice globale

s3://my-bucket/pec-backups/                # S3 - Archivi giornalieri
└── <account>/
    └── <YYYY>/
        └── <YYYY-MM-DD>/
            ├── archive-<account>-<date>.tar.gz  # Upload su S3
            └── digest.sha256
```

## ⚙️ Configurazione

Il file `config/config.yaml` contiene tutte le impostazioni:

```yaml
# Percorso base per l'archivio
base_path: /data/pec-archive

# Modalità di backup: "standard" o "s3_sync"
# - standard: Archivia e comprime localmente (default)
# - s3_sync: Mantiene email non compresse localmente, carica archivi giornalieri su S3
backup_mode: standard

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

# Configurazione S3 (richiesta solo con backup_mode: "s3_sync")
s3:
  bucket: my-pec-backups     # Nome del bucket S3
  region: eu-west-1          # Regione AWS
  prefix: pec-backups        # Prefisso per le chiavi S3
  aws_access_key_id: ${AWS_ACCESS_KEY_ID}      # Opzionale
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}  # Opzionale

# Notifiche email (opzionale)
notifications:
  enabled: true
  send_on: "always"  # "always" o "error"
  recipients:
    - admin@example.com
  smtp:
    host: smtp.example.com
    port: 587
    username: ${SMTP_USERNAME}
    password: ${SMTP_PASSWORD}
    use_tls: true

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

## ☁️ Configurazione S3 (Modalità S3 Sync)

Il sistema supporta sia Amazon S3 che servizi S3-compatible come **Hetzner Object Storage**, **MinIO**, e altri.

### Prerequisiti

#### Per AWS S3

1. **Account AWS** con accesso a S3
2. **Bucket S3** creato nella regione desiderata
3. **Credenziali AWS** (Access Key ID e Secret Access Key) oppure ruolo IAM

#### Per Hetzner Object Storage

1. **Account Hetzner Cloud**
2. **Bucket creato** tramite la Hetzner Cloud Console
3. **Credenziali S3** (Access Key e Secret Key) generate dalla console Hetzner

### Setup Bucket

#### AWS S3

```bash
# Creare un bucket S3 (tramite AWS CLI)
aws s3 mb s3://my-pec-backups --region eu-west-1

# Verificare l'accesso
aws s3 ls s3://my-pec-backups
```

#### Hetzner Object Storage

1. Accedere alla [Hetzner Cloud Console](https://console.hetzner.cloud/)
2. Navigare a **Object Storage**
3. Creare un nuovo bucket nella regione desiderata
4. Generare le credenziali S3 (Access Key e Secret Key)

### Configurazione in config.yaml

#### Per AWS S3

```yaml
# Abilitare la modalità S3 Sync
backup_mode: s3_sync

# Configurazione S3
s3:
  bucket: my-pec-backups
  region: eu-west-1
  prefix: pec-backups  # Directory nel bucket
  
  # Credenziali (opzionale - può usare IAM roles)
  aws_access_key_id: ${AWS_ACCESS_KEY_ID}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

#### Per Hetzner Object Storage

```yaml
# Abilitare la modalità S3 Sync
backup_mode: s3_sync

# Configurazione Hetzner S3
s3:
  bucket: my-pec-backups
  region: eu-central-1  # o altra regione
  prefix: pec-backups
  
  # Endpoint Hetzner S3 (richiesto)
  # Formato: https://<bucket-name>.s3.<region>.hetzner.cloud
  endpoint_url: https://my-pec-backups.s3.eu-central-1.hetzner.cloud
  
  # Credenziali Hetzner (richieste)
  aws_access_key_id: ${AWS_ACCESS_KEY_ID}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

### Credenziali

#### Opzione 1: Variabili d'Ambiente (Consigliata)

Nel file `.env` o `docker-compose.yml`:

```bash
# Per AWS S3 o Hetzner Object Storage
# Nota: Hetzner usa nomi di campo compatibili con AWS per le credenziali S3
AWS_ACCESS_KEY_ID=your-access-key-here
AWS_SECRET_ACCESS_KEY=your-secret-key-here
```

#### Opzione 2: Ruolo IAM (Solo per AWS S3 su EC2/ECS)

Se l'applicazione gira su EC2 o ECS, configurare un ruolo IAM con policy S3:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-pec-backups/*",
        "arn:aws:s3:::my-pec-backups"
      ]
    }
  ]
}
```

### Vantaggi della Modalità S3 Sync

- ✅ **Mirror completo della casella** - Copia locale esatta della casella PEC senza cancellazioni
- ✅ **Archivio locale consultabile** - Email non compresse, ricercabili via API
- ✅ **Backup cloud sicuro** - Copia giornaliera su S3 con alta affidabilità
- ✅ **Risparmio spazio locale** - Solo email, archivi giornalieri su S3
- ✅ **Organizzazione per data su S3** - Archivi giornalieri facilmente identificabili
- ✅ **Disaster recovery** - Backup geograficamente distribuito
- ✅ **Compliance** - Storage class ottimizzabili (Standard-IA, Glacier)
- ✅ **Flessibilità provider** - Supporto AWS S3, Hetzner, MinIO e altri servizi S3-compatible

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
      # Credenziali S3 (opzionale, solo per modalità s3_sync)
      - AWS_ACCESS_KEY_ID=your_aws_key
      - AWS_SECRET_ACCESS_KEY=your_aws_secret
      # Notifiche email (opzionale)
      - SMTP_USERNAME=your_smtp_username
      - SMTP_PASSWORD=your_smtp_password
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
| `notifications.py` | Invia notifiche email con report e alert |
| `config.py` | Carica e valida configurazione YAML |
| `api.py` | REST API per ricerca e download email |
| `api_server.py` | Server FastAPI per l'API REST |

## 🔌 REST API

Il sistema include una REST API per cercare e scaricare le email archiviate.

### Avvio dell'API

L'API viene avviata automaticamente con Docker Compose:

```bash
docker compose up -d pec-api
```

L'API sarà disponibile su `http://localhost:8000`.

### Documentazione API

La documentazione interattiva è disponibile su:
- **Swagger UI**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc

### Endpoints

| Metodo | Endpoint | Descrizione |
|--------|----------|-------------|
| GET | `/api/v1/health` | Health check |
| GET | `/api/v1/accounts` | Lista degli account archiviati |
| GET | `/api/v1/accounts/{account}/dates?year=YYYY` | Date archiviate per un account |
| GET | `/api/v1/accounts/{account}/emails/{date}` | Email di una data specifica |
| GET | `/api/v1/search` | Ricerca email con filtri |
| GET | `/api/v1/accounts/{account}/emails/{date}/{folder}/{filename}` | Download email (.eml) |
| GET | `/api/v1/accounts/{account}/archive/{date}` | Download archivio (.tar.gz) |

### Esempi di Ricerca

```bash
# Ricerca per oggetto
curl "http://localhost:8000/api/v1/search?subject=fattura"

# Ricerca per mittente
curl "http://localhost:8000/api/v1/search?from=mittente@pec.it"

# Ricerca per destinatario
curl "http://localhost:8000/api/v1/search?to=destinatario@pec.it"

# Ricerca per intervallo di date
curl "http://localhost:8000/api/v1/search?date_from=2024-01-01&date_to=2024-01-31"

# Ricerca combinata
curl "http://localhost:8000/api/v1/search?subject=fattura&account=account1&date_from=2024-01-01"
```

### Download Email

```bash
# Lista email di una data
curl "http://localhost:8000/api/v1/accounts/account1/emails/2024-01-15"

# Download singola email
curl -O "http://localhost:8000/api/v1/accounts/account1/emails/2024-01-15/INBOX/123_subject.eml"

# Download archivio compresso
curl -O "http://localhost:8000/api/v1/accounts/account1/archive/2024-01-15"
```

## 📬 Notifiche Email

Il sistema può inviare notifiche email con il report giornaliero del backup e alert in caso di errori.

### Configurazione

```yaml
notifications:
  # Abilita/disabilita le notifiche
  enabled: true
  
  # Quando inviare: "always" (sempre) o "error" (solo in caso di errori)
  send_on: "always"
  
  # Destinatari (uno o più indirizzi email)
  recipients:
    - admin@example.com
    - backup-team@example.com
  
  # Configurazione server SMTP
  smtp:
    host: smtp.example.com
    port: 587
    username: ${SMTP_USERNAME}
    password: ${SMTP_PASSWORD}
    sender: pec-archiver@example.com  # Opzionale
    use_tls: true  # true per TLS (porta 587), false per SSL (porta 465)
```

### Opzioni di Invio

| Valore `send_on` | Comportamento |
|------------------|---------------|
| `always` | Invia notifica dopo ogni backup (successo o errore) |
| `error` | Invia notifica solo quando si verificano errori |

### Contenuto della Notifica

La notifica include:
- ✅ Data del backup
- ✅ Stato generale (successo/errori)
- ✅ Numero di account processati
- ✅ Numero di messaggi archiviati
- ✅ Dettaglio per ogni account
- ✅ Eventuali errori riscontrati

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
