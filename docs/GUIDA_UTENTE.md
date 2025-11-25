# 📚 Guida Utente - PEC Archiver

Questa guida fornisce istruzioni dettagliate per l'installazione, configurazione e utilizzo del sistema PEC Archiver.

## Indice

1. [Introduzione](#introduzione)
2. [Installazione](#installazione)
3. [Configurazione](#configurazione)
4. [Notifiche Email](#notifiche-email)
5. [Utilizzo](#utilizzo)
6. [Backup Manuale](#backup-manuale)
7. [Monitoraggio](#monitoraggio)
8. [Risoluzione Problemi](#risoluzione-problemi)

---

## Introduzione

PEC Archiver è un sistema automatizzato per il backup giornaliero delle caselle PEC Aruba. Il sistema:

- Si connette automaticamente alle caselle PEC configurate
- Scarica tutti i messaggi del giorno precedente
- Salva i messaggi in formato .eml standard
- Genera indici consultabili (CSV e JSON)
- Crea archivi compressi con verifica di integrità
- Invia notifiche email con report giornalieri e alert in caso di errori

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

## Notifiche Email

Il sistema può inviare notifiche email con il report giornaliero del backup. Questa funzionalità è opzionale e può essere abilitata o disabilitata.

### Abilitare le Notifiche

Aggiungere la sezione `notifications` al file `config/config.yaml`:

```yaml
notifications:
  # Abilita le notifiche (true/false)
  enabled: true
  
  # Quando inviare le notifiche:
  # - "always": dopo ogni backup (successo o errore)
  # - "error": solo in caso di errori
  send_on: "always"
  
  # Destinatari (uno o più indirizzi email)
  recipients:
    - admin@azienda.it
    - it-team@azienda.it
  
  # Configurazione server SMTP
  smtp:
    host: smtp.azienda.it
    port: 587
    username: ${SMTP_USERNAME}
    password: ${SMTP_PASSWORD}
    sender: pec-archiver@azienda.it  # Opzionale
    use_tls: true
```

### Configurare le Variabili d'Ambiente per SMTP

Nel file `docker-compose.yml`, aggiungere le credenziali SMTP:

```yaml
environment:
  - SMTP_USERNAME=username_smtp
  - SMTP_PASSWORD=password_smtp
```

Oppure usare un file `.env`:

```bash
# .env
SMTP_USERNAME=username_smtp
SMTP_PASSWORD=password_smtp
```

### Opzioni di Invio

| Valore | Descrizione |
|--------|-------------|
| `always` | Invia notifica dopo ogni backup, indipendentemente dal risultato |
| `error` | Invia notifica solo quando si verificano errori durante il backup |

### Destinatari Multipli

È possibile configurare uno o più destinatari:

```yaml
# Singolo destinatario
recipients: admin@azienda.it

# Più destinatari
recipients:
  - admin@azienda.it
  - backup-team@azienda.it
  - responsabile@azienda.it
```

### Disabilitare le Notifiche

Per disabilitare le notifiche, impostare `enabled: false`:

```yaml
notifications:
  enabled: false
```

### Esempio di Email di Notifica

La notifica include:

- **Data del backup** archiviato
- **Stato generale**: successo o errori
- **Statistiche**:
  - Numero di account processati
  - Account con successo
  - Account con errori
  - Totale messaggi archiviati
- **Dettaglio per account**: stato e numero di messaggi
- **Eventuali errori**: descrizione degli errori riscontrati

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

### Errore: Notifiche Email non Inviate

**Sintomo**: Le notifiche non arrivano

**Soluzioni**:
1. Verificare che `notifications.enabled` sia `true`
2. Controllare la configurazione SMTP:
   ```bash
   docker compose exec pec-archiver python -c "
   from src.config import load_config
   config = load_config()
   notifications = config.get('notifications', {})
   print(f'Enabled: {notifications.get(\"enabled\")}')
   print(f'Recipients: {notifications.get(\"recipients\")}')
   print(f'SMTP Host: {notifications.get(\"smtp\", {}).get(\"host\")}')
   "
   ```
3. Verificare le credenziali SMTP
4. Controllare i log per errori:
   ```bash
   docker compose logs pec-archiver | grep -i notification
   ```

### Errore: Connessione SMTP Fallita

**Sintomo**: `SMTP error` o `Connection refused` nei log

**Soluzioni**:
1. Verificare host e porta del server SMTP
2. Controllare se `use_tls` è corretto per la porta:
   - Porta 587: `use_tls: true` (STARTTLS)
   - Porta 465: `use_tls: false` (SSL diretto)
3. Verificare che il firewall consenta la connessione

---

## Supporto

Per problemi o domande:
- Aprire una issue su GitHub
- Contattare il supporto tecnico

---

**Ultima modifica**: Novembre 2024
