# 🔧 Riferimento Tecnico - PEC Archiver

Documentazione tecnica per sviluppatori e amministratori di sistema.

## Indice

1. [Architettura](#architettura)
2. [Moduli](#moduli)
3. [API Interna](#api-interna)
4. [Formato File](#formato-file)
5. [Estensibilità](#estensibilità)

---

## Architettura

### Diagramma di Flusso

```
┌────────────────────────────────────────────────────────────┐
│                     PEC ARCHIVER                           │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  ┌─────────────┐    ┌─────────────────────────────────┐   │
│  │  Scheduler  │───▶│        Account Workers          │   │
│  │  (01:00)    │    │    (ThreadPoolExecutor)         │   │
│  └─────────────┘    └──────────────┬──────────────────┘   │
│                                    │                       │
│         ┌──────────────────────────┼────────────────┐     │
│         │                          │                │     │
│         ▼                          ▼                ▼     │
│  ┌─────────────┐          ┌─────────────┐   ┌───────────┐ │
│  │ IMAP Client │          │   Storage   │   │  Indexer  │ │
│  │  (SSL/TLS)  │          │   (.eml)    │   │(CSV/JSON) │ │
│  └─────────────┘          └──────┬──────┘   └─────┬─────┘ │
│                                  │                │       │
│                           ┌──────┴────────────────┘       │
│                           ▼                               │
│                    ┌─────────────┐                        │
│                    │ Compression │                        │
│                    │ (.tar.gz)   │                        │
│                    └──────┬──────┘                        │
│                           │                               │
│                           ▼                               │
│                    ┌─────────────┐                        │
│                    │  Reporting  │                        │
│                    │(summary.json)                        │
│                    └─────────────┘                        │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### Componenti Principali

| Componente | File | Descrizione |
|------------|------|-------------|
| Entry Point | `main.py` | Punto di ingresso principale |
| Backup Range | `backup_range.py` | Script per backup intervalli |
| Scheduler | `scheduler.py` | Pianificazione giornaliera |
| Worker | `worker.py` | Elaborazione singolo account |
| IMAP Client | `imap_client.py` | Connessione e fetch messaggi |
| Storage | `storage.py` | Salvataggio file .eml |
| Indexer | `indexing.py` | Generazione indici |
| Compression | `compression.py` | Creazione archivi |
| Reporting | `reporting.py` | Generazione summary |
| Notifications | `notifications.py` | Notifiche email |
| Config | `config.py` | Gestione configurazione |

---

## Moduli

### scheduler.py

```python
class PECScheduler:
    """Scheduler principale per archiviazione PEC."""
    
    def __init__(self, config: dict = None, config_path: str = None):
        """
        Inizializza lo scheduler.
        
        Args:
            config: Dizionario di configurazione
            config_path: Percorso al file di configurazione
        """
    
    def run_archive_job(self, target_date: datetime = None) -> dict:
        """
        Esegue il job di archiviazione per tutti gli account.
        
        Args:
            target_date: Data da archiviare (default: ieri)
        
        Returns:
            Report aggregato
        """
    
    def start(self) -> None:
        """Avvia lo scheduler in loop infinito."""
    
    def run_once(self, target_date: datetime = None) -> dict:
        """Esegue il job una volta immediatamente."""
```

### worker.py

```python
class AccountWorker:
    """Worker per elaborazione singolo account PEC."""
    
    def __init__(
        self,
        account_config: dict,
        base_path: str,
        retry_policy: dict = None,
        imap_settings: dict = None
    ):
        """
        Inizializza il worker.
        
        Args:
            account_config: Configurazione account
            base_path: Percorso base archivio
            retry_policy: Policy di retry
            imap_settings: Impostazioni IMAP
        """
    
    def process(self, target_date: datetime) -> str:
        """
        Elabora l'account per una data specifica.
        
        Args:
            target_date: Data da archiviare
        
        Returns:
            Percorso al file summary.json
        """
```

### imap_client.py

```python
class IMAPClient:
    """Client IMAP con supporto SSL/TLS."""
    
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: int = 993,
        timeout: int = 30
    ):
        """Inizializza il client IMAP."""
    
    def connect(self) -> None:
        """Stabilisce connessione SSL/TLS."""
    
    def fetch_messages_by_date(
        self,
        folder: str,
        target_date: datetime,
        batch_size: int = 100
    ) -> Generator[tuple[Message, bytes, str], None, None]:
        """
        Recupera messaggi per data specifica.
        
        Yields:
            Tuple di (Message, raw_email, UID)
        """

def with_retry(func, max_retries=3, initial_delay=5, backoff_multiplier=2):
    """
    Esegue funzione con retry e backoff esponenziale.
    """
```

### storage.py

```python
class Storage:
    """Gestione storage per file .eml e struttura directory."""
    
    def __init__(self, base_path: str):
        """Inizializza lo storage."""
    
    def create_directory_structure(
        self,
        account: str,
        date: datetime,
        folders: list
    ) -> str:
        """
        Crea struttura directory per account e data.
        
        Returns:
            Percorso alla directory creata
        """
    
    def save_eml(
        self,
        account: str,
        date: datetime,
        folder: str,
        uid: str,
        message: Message,
        raw_email: bytes
    ) -> str:
        """
        Salva messaggio come file .eml.
        
        Returns:
            Percorso al file salvato
        """
```

### indexing.py

```python
class Indexer:
    """Generatore di indici CSV e JSON."""
    
    def __init__(self, base_path: str):
        """Inizializza l'indexer."""
    
    def add_message(
        self,
        message: Message,
        uid: str,
        folder: str,
        filepath: str
    ) -> None:
        """Aggiunge messaggio all'indice."""
    
    def generate_csv(self) -> str:
        """Genera index.csv e ritorna il percorso."""
    
    def generate_json(self) -> str:
        """Genera index.json e ritorna il percorso."""
    
    def get_stats(self) -> dict:
        """Ritorna statistiche dei messaggi indicizzati."""
```

### compression.py

```python
def create_archive(
    source_path: str,
    account_name: str,
    date: datetime
) -> str:
    """
    Crea archivio .tar.gz.
    
    Returns:
        Percorso all'archivio creato
    """

def create_digest(archive_path: str) -> str:
    """
    Crea digest SHA256 per l'archivio.
    
    Returns:
        Percorso al file digest
    """

def verify_archive(archive_path: str, digest_path: str) -> bool:
    """Verifica integrità dell'archivio."""
```

### config.py

```python
def load_config(config_path: str = None) -> dict:
    """
    Carica configurazione da file YAML.
    
    Args:
        config_path: Percorso al file (default: PEC_ARCHIVE_CONFIG env var)
    
    Returns:
        Dizionario di configurazione
    
    Raises:
        ConfigError: Se configurazione non valida
    """

def validate_config(config: dict) -> None:
    """Valida la configurazione."""

def expand_env_vars(value: Any) -> Any:
    """Espande variabili d'ambiente (${VAR_NAME})."""
```

### notifications.py

```python
def send_notification(
    config: dict,
    report: dict,
    target_date: datetime,
    force_send: bool = False
) -> bool:
    """
    Invia notifica email con il report del backup.
    
    Args:
        config: Configurazione notifiche
        report: Report aggregato del backup
        target_date: Data archiviata
        force_send: Forza invio anche se disabilitato
    
    Returns:
        True se la notifica è stata inviata, False altrimenti
    
    Raises:
        NotificationError: Se l'invio fallisce
    """

def format_report_html(report: dict, target_date: datetime) -> str:
    """Formatta il report in HTML per email."""

def format_report_text(report: dict, target_date: datetime) -> str:
    """Formatta il report in testo semplice."""

def validate_notification_config(config: dict) -> list[str]:
    """
    Valida la configurazione delle notifiche.
    
    Returns:
        Lista di messaggi di errore (vuota se valida)
    """
```

---

## Formato File

### index.csv

```csv
uid,folder,date,from,to,subject,filepath
123,INBOX,2024-01-15T10:30:00,sender@pec.it,recipient@pec.it,Oggetto,INBOX/001_message.eml
```

### index.json

```json
{
  "generated_at": "2024-01-15T02:15:30",
  "total_messages": 150,
  "messages": [
    {
      "uid": "123",
      "folder": "INBOX",
      "date": "2024-01-15T10:30:00",
      "from": "sender@pec.it",
      "to": "recipient@pec.it",
      "subject": "Oggetto",
      "filepath": "INBOX/001_message.eml"
    }
  ]
}
```

### summary.json

```json
{
  "account": "account@pec.it",
  "date": "2024-01-15",
  "start_time": "2024-01-16T01:00:05",
  "end_time": "2024-01-16T01:02:30",
  "duration_seconds": 145.2,
  "stats": {
    "total_messages": 150,
    "folders": {
      "INBOX": 120,
      "Posta inviata": 30
    }
  },
  "archive": {
    "path": "archive-account-2024-01-15.tar.gz",
    "size_bytes": 15234567,
    "digest": "sha256:abc123..."
  },
  "errors": []
}
```

### digest.sha256

```
abc123def456...  archive-account-2024-01-15.tar.gz
```

---

## Estensibilità

### Aggiungere un Nuovo Provider PEC

1. Modificare `imap_client.py` se necessario per gestire peculiarità del provider
2. Aggiungere configurazione account con host e porta corretti

### Aggiungere Formato di Esportazione

1. Creare nuovo modulo (es. `export_pdf.py`)
2. Integrare nel flusso del worker dopo l'indicizzazione

### Personalizzare Compressione

1. Modificare `compression.py`
2. Supporto per formati alternativi (zip, 7z, etc.)

### Notifiche Email

Le notifiche email sono già integrate nel sistema. Per personalizzarle:

1. Modificare `notifications.py` per:
   - Cambiare il formato del report (HTML/testo)
   - Aggiungere allegati
   - Personalizzare i contenuti

2. Per aggiungere altri canali (Slack, Teams, etc.):
   - Estendere `notifications.py` con nuove funzioni
   - Aggiungere configurazione nel file YAML

#### Configurazione Notifiche

```yaml
notifications:
  enabled: true                    # Abilita/disabilita
  send_on: "always"               # "always" o "error"
  recipients:                      # Lista destinatari
    - admin@example.com
  smtp:
    host: smtp.example.com
    port: 587
    username: ${SMTP_USERNAME}
    password: ${SMTP_PASSWORD}
    sender: pec-archiver@example.com
    use_tls: true
```

---

## Test

### Eseguire Test

```bash
# Tutti i test
python -m pytest tests/ -v

# Con coverage
python -m pytest tests/ -v --cov=src

# Test specifico
python -m pytest tests/test_config.py -v
```

### Struttura Test

```
tests/
├── __init__.py
├── test_backup_range.py   # Test script backup intervalli
├── test_compression.py    # Test compressione
├── test_config.py         # Test configurazione
├── test_indexing.py       # Test indicizzazione
├── test_notifications.py  # Test notifiche email
└── test_storage.py       # Test storage
```

---

## Performance

### Ottimizzazioni

- **ThreadPoolExecutor**: Parallelismo per account multipli
- **Batch IMAP**: Fetch messaggi in batch configurabili
- **Streaming**: Salvataggio messaggi senza caricarli tutti in memoria

### Tuning

| Parametro | Effetto | Consigliato |
|-----------|---------|-------------|
| `concurrency` | Worker paralleli | 4-8 |
| `batch_size` | Messaggi per fetch | 50-200 |
| `timeout` | Timeout connessione | 30-60s |

### Monitoraggio Risorse

```bash
# Memoria e CPU del container
docker stats pec-archiver

# Spazio disco archivio
du -sh /srv/pec-archive/*
```

---

**Versione**: 1.0  
**Data**: Novembre 2024
