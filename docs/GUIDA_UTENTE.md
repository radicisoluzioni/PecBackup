# 📚 Guida Utente - PEC Archiver

Questa guida fornisce istruzioni dettagliate per l'installazione, configurazione e utilizzo del sistema PEC Archiver.

## Indice

1. [Introduzione](#introduzione)
2. [Installazione](#installazione)
3. [Configurazione](#configurazione)
4. [Utilizzo](#utilizzo)
5. [Backup Manuale](#backup-manuale)
6. [Monitoraggio](#monitoraggio)
7. [Risoluzione Problemi](#risoluzione-problemi)

---

## Introduzione

PEC Archiver è un sistema automatizzato per il backup giornaliero delle caselle PEC Aruba. Il sistema:

- Si connette automaticamente alle caselle PEC configurate
- Scarica tutti i messaggi del giorno precedente
- Salva i messaggi in formato .eml standard
- Genera indici consultabili (CSV e JSON)
- Crea archivi compressi con verifica di integrità

### Perché usare PEC Archiver?

- **Conformità legale**: Le PEC hanno valore legale e devono essere conservate
- **Sicurezza**: Backup indipendente dal provider
- **Automazione**: Nessun intervento manuale richiesto
- **Verificabilità**: Digest SHA256 per ogni archivio

---

## Installazione

### Prerequisiti

- Sistema operativo Linux (consigliato Ubuntu 20.04+)
- Docker 20.10 o superiore
- Docker Compose 2.0 o superiore
- Almeno 2GB di RAM disponibile
- Spazio disco sufficiente per gli archivi

### Procedura di Installazione

#### 1. Installare Docker (se non presente)

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install docker.io docker-compose-plugin

# Verificare l'installazione
docker --version
docker compose version
```

#### 2. Clonare il Repository

```bash
git clone https://github.com/radicisoluzioni/SogertPecBackup.git
cd SogertPecBackup
```

#### 3. Creare la Directory di Archivio

```bash
sudo mkdir -p /srv/pec-archive
sudo chown $USER:$USER /srv/pec-archive
```

#### 4. Configurare il Sistema

```bash
cp config/config.yaml.example config/config.yaml
nano config/config.yaml  # Modificare con i propri dati
```

#### 5. Avviare il Servizio

```bash
docker compose up -d
```

---

## Configurazione

### File di Configurazione

Il file `config/config.yaml` contiene tutte le impostazioni del sistema.

#### Parametri Principali

| Parametro | Descrizione | Default |
|-----------|-------------|---------|
| `base_path` | Directory di salvataggio archivi | `/data/pec-archive` |
| `concurrency` | Numero di worker paralleli | `4` |
| `scheduler.run_time` | Orario di esecuzione giornaliera | `01:00` |

#### Configurazione Account PEC

```yaml
accounts:
  - username: mia-pec@pec.it
    password: ${PEC_PASSWORD}  # Variabile d'ambiente
    host: imaps.pec.aruba.it
    port: 993
    folders:
      - INBOX
      - Posta inviata
```

#### Variabili d'Ambiente

Per sicurezza, le password non devono essere salvate in chiaro. Usare variabili d'ambiente:

```bash
# Nel file docker-compose.yml
environment:
  - PEC_PASSWORD=la_mia_password_sicura
```

Oppure usare un file `.env`:

```bash
# .env
PEC_PASSWORD=la_mia_password_sicura
```

### Configurazione Retry

Per connessioni instabili, configurare la policy di retry:

```yaml
retry_policy:
  max_retries: 3        # Tentativi massimi
  initial_delay: 5      # Secondi tra tentativi
  backoff_multiplier: 2 # Moltiplicatore backoff
```

### Configurazione IMAP

```yaml
imap:
  timeout: 30      # Timeout connessione in secondi
  batch_size: 100  # Messaggi per batch
```

---

## Utilizzo

### Avvio del Servizio

```bash
# Avviare in background
docker compose up -d

# Visualizzare i log
docker compose logs -f pec-archiver

# Fermare il servizio
docker compose down
```

### Comandi Disponibili

#### Scheduler Automatico

Il servizio si avvia automaticamente e esegue il backup ogni giorno all'ora configurata:

```bash
# Avvio standard (scheduler attivo)
docker compose up -d
```

#### Backup Immediato

Per eseguire un backup immediato (data di ieri):

```bash
docker compose exec pec-archiver python -m src.main --run-now
```

#### Backup di una Data Specifica

```bash
docker compose exec pec-archiver python -m src.main --run-now --date 2024-01-15
```

---

## Backup Manuale

### Script backup_range.py

Per situazioni di emergenza o recupero di periodi specifici, usare lo script `backup_range.py`.

#### Backup di un Singolo Giorno

```bash
docker compose exec pec-archiver python -m src.backup_range --date 2024-01-15
```

#### Backup di un Intervallo di Date

```bash
docker compose exec pec-archiver python -m src.backup_range \
    --date-from 2024-01-15 \
    --date-to 2024-01-22
```

#### Esempio: Recupero di una Settimana

```bash
# Recupera tutti i messaggi dal 15 al 21 gennaio 2024
docker compose exec pec-archiver python -m src.backup_range \
    --date-from 2024-01-15 \
    --date-to 2024-01-21
```

#### Output del Backup

Al termine, viene mostrato un riepilogo:

```
============================================================
BACKUP RANGE JOB COMPLETED
============================================================
Date range: 2024-01-15 to 2024-01-21
Days processed: 7
Days successful: 7
Days with errors: 0
------------------------------------------------------------
Total accounts processed: 14
Total accounts successful: 14
Total messages: 1523
Total errors: 0
============================================================
```

---

## Monitoraggio

### Visualizzare i Log

```bash
# Log in tempo reale
docker compose logs -f pec-archiver

# Ultimi 100 log
docker compose logs --tail=100 pec-archiver
```

### Verificare lo Stato del Container

```bash
docker compose ps
```

### Verificare gli Archivi Creati

```bash
# Lista archivi di oggi
ls -la /srv/pec-archive/*/$(date +%Y)/$(date +%Y-%m-%d)/

# Verificare integrità di un archivio
cd /srv/pec-archive/account@pec.it/2024/2024-01-15/
sha256sum -c digest.sha256
```

### File di Summary

Ogni backup genera un file `summary.json` con le statistiche:

```json
{
  "account": "account@pec.it",
  "date": "2024-01-15",
  "messages_count": 150,
  "folders": {
    "INBOX": 120,
    "Posta inviata": 30
  },
  "archive_size": 15234567,
  "duration_seconds": 45.2,
  "errors": []
}
```

---

## Risoluzione Problemi

### Errore: Connessione IMAP Fallita

**Sintomo**: `IMAP login failed` nei log

**Soluzioni**:
1. Verificare username e password
2. Controllare che l'host IMAP sia corretto
3. Verificare la connettività di rete

```bash
# Test connessione
docker compose exec pec-archiver python -c "
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
result = sock.connect_ex(('imaps.pec.aruba.it', 993))
print('OK' if result == 0 else 'ERRORE')
sock.close()
"
```

### Errore: Spazio Disco Insufficiente

**Sintomo**: `No space left on device`

**Soluzioni**:
1. Verificare lo spazio disponibile: `df -h /srv/pec-archive`
2. Eliminare archivi vecchi se necessario
3. Espandere il disco

### Errore: Timeout IMAP

**Sintomo**: `Connection timed out`

**Soluzioni**:
1. Aumentare il timeout nel config:
   ```yaml
   imap:
     timeout: 60  # Aumentato da 30
   ```
2. Ridurre il batch_size:
   ```yaml
   imap:
     batch_size: 50  # Ridotto da 100
   ```

### Container non si Avvia

**Sintomo**: Container in stato `Exited`

**Soluzioni**:
```bash
# Verificare i log di errore
docker compose logs pec-archiver

# Ricostruire l'immagine
docker compose build --no-cache
docker compose up -d
```

### Verificare Configurazione

```bash
# Test della configurazione
docker compose exec pec-archiver python -c "
from src.config import load_config
config = load_config()
print('Configurazione OK')
print(f'Accounts: {len(config[\"accounts\"])}')
"
```

---

## Supporto

Per problemi o domande:
- Aprire una issue su GitHub
- Contattare il supporto tecnico

---

**Ultima modifica**: Novembre 2024
