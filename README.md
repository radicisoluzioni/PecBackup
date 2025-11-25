# Archivio Giornaliero PEC Aruba (Docker & Docker Compose)

## 📌 Panoramica

Questo progetto realizza un sistema batch per l'archiviazione
giornaliera delle caselle PEC Aruba.\
Il servizio gira all'interno di un container Docker e ogni notte alle
01:00:

-   si connette via IMAP alle caselle PEC,
-   scarica i messaggi del giorno precedente,
-   salva i messaggi in formato .eml,
-   genera index.csv e index.json,
-   crea un archivio .tar.gz,
-   produce digest.sha256 e summary.json.

Ottimizzato per circa 20.000 PEC/giorno.

## 🧱 Architettura del Sistema

### Componenti

-   Main Scheduler
-   Account Worker
-   Moduli: IMAP, Storage, Indexing, Compressione, Reporting

### Flusso

1.  Scheduler calcola la data precedente.
2.  Avvia i worker.
3.  Ogni worker processa cartelle IMAP, salva .eml, genera indici e
    archivio.

## 📂 Struttura dell'Archivio

    /data/pec-archive/
      <account>/
        <YYYY>/
          <YYYY-MM-DD>/
            INBOX/
            Posta_inviata/
            index.csv
            index.json
            summary.json
            archive-<account>-<date>.tar.gz
            digest.sha256

## ⚙️ Configurazione

Configurazione YAML montata come volume e include: - base_path -
concurrency - retry_policy - accounts (username, password, cartelle
IMAP)

## 🐳 Docker Compose

Esempio:

    version: "3.9"

    services:
      pec-archiver:
        image: pec-archiver:latest
        container_name: pec-archiver
        restart: unless-stopped
        environment:
          - TZ=Europe/Rome
          - PEC_ARCHIVE_CONFIG=/app/config/config.yaml
        volumes:
          - ./config:/app/config:ro
          - /srv/pec-archive:/data/pec-archive

## 📑 File generati

-   index.csv\
-   index.json\
-   summary.json\
-   archive-`<account>`{=html}-`<date>`{=html}.tar.gz\
-   digest.sha256

## 🔐 Sicurezza

-   Connessioni IMAP SSL/TLS\
-   Config montata ro\
-   Supporto variabili d'ambiente

## ♻️ Gestione errori

-   Retry con backoff progressivo\
-   Logging nel container\
-   Errori in summary.json

## 📈 Performance

-   Progettato per 20.000 PEC/giorno\
-   Parallelismo configurabile\
-   Batch IMAP regolabili

## 🛠️ Prerequisiti

-   Docker\
-   docker-compose\
-   Spazio disco /srv/pec-archive

## 🚀 Deploy

1.  Clonare repo\
2.  Aggiungere config/config.yaml\
3.  Creare /srv/pec-archive\
4.  Avviare: `docker compose up -d`\
5.  Monitorare: `docker compose logs -f pec-archiver`

## 📆 Backup manuale (Date specifiche o Intervalli)

Per casi di emergenza in cui è necessario effettuare il backup di un giorno
specifico o di un intervallo di date, è disponibile lo script `backup_range.py`.

### Backup di un giorno specifico

```bash
python -m src.backup_range --date 2024-01-15
```

### Backup di un intervallo di date

```bash
python -m src.backup_range --date-from 2024-01-15 --date-to 2024-01-22
```

### Backup di una settimana

```bash
python -m src.backup_range --date-from 2024-01-15 --date-to 2024-01-21
```

### Opzioni disponibili

| Opzione | Descrizione |
|---------|-------------|
| `--date`, `-d` | Data singola da backuppare (formato YYYY-MM-DD) |
| `--date-from`, `-f` | Data iniziale dell'intervallo (formato YYYY-MM-DD) |
| `--date-to`, `-t` | Data finale dell'intervallo (formato YYYY-MM-DD) |
| `--config`, `-c` | Percorso al file di configurazione |
| `--log-level`, `-l` | Livello di logging (DEBUG, INFO, WARNING, ERROR) |

### Esempi con Docker

```bash
# Backup di un giorno specifico
docker compose exec pec-archiver python -m src.backup_range --date 2024-01-15

# Backup di una settimana
docker compose exec pec-archiver python -m src.backup_range \
    --date-from 2024-01-15 --date-to 2024-01-21
```
