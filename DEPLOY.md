# Deploy

## Binary (Linux)

```bash
# Descargar de GitHub Releases
wget https://github.com/cgmark101/nexo_nanobroker/releases/latest/download/nanobroker-linux
chmod +x nanobroker-linux

# Ejecutar con valores por defecto
./nanobroker-linux

# Con argumentos personalizados
./nanobroker-linux --host 0.0.0.0 --port 8000 --database /data/broker.db --janitor-interval 60

# Variables de entorno (prioridad menor que CLI)
export NANOBROKER_DB_FILE=/data/broker.db
export NANOBROKER_PORT=8000
./nanobroker-linux

# Systemd service (/etc/systemd/system/nanobroker.service)
: '
[Unit]
Description=NanoBroker
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/nanobroker-linux --database /var/lib/nanobroker/broker.db
Restart=always
User=nanobroker

[Install]
WantedBy=multi-user.target
'
```

## Binary (Windows)

```powershell
# Descargar de GitHub Releases
Invoke-WebRequest -Uri "https://github.com/cgmark101/nexo_nanobroker/releases/latest/download/nanobroker-win64.exe" -OutFile nanobroker.exe

# Ejecutar con valores por defecto
.\nanobroker.exe

# Con argumentos personalizados
.\nanobroker.exe --host 0.0.0.0 --port 8000 --database C:\data\broker.db --janitor-interval 60

# Variables de entorno (prioridad menor que CLI)
$env:NANOBROKER_DB_FILE = "C:\data\broker.db"
$env:NANOBROKER_PORT = "8000"
.\nanobroker.exe

# Windows Service (con NSSM)
: '
nssm install NanoBroker "C:\nanobroker\nanobroker.exe" "--database C:\nanobroker\broker.db"
nssm start NanoBroker
'
```

## Docker

```bash
# Usar la imagen publicada (cuando esté disponible)
# docker pull ghcr.io/cgmark101/nexo_nanobroker:latest

# O construir desde el Dockerfile incluido
docker build -t nanobroker .
docker run -d \
  --name nanobroker \
  -p 8000:8000 \
  -v nanobroker_data:/data \
  -e NANOBROKER_DB_FILE=/data/broker.db \
  nanobroker

# Con docker-compose.yml incluido
docker compose up -d

# Variables de entorno disponibles
: '
NANOBROKER_DB_FILE         Ruta a la base SQLite      default: broker_local.db
NANOBROKER_HOST            Dirección de escucha        default: 0.0.0.0
NANOBROKER_PORT            Puerto HTTP                 default: 8000
NANOBROKER_LOG_LEVEL       DEBUG/INFO/WARNING/ERROR    default: INFO
NANOBROKER_DB_TIMEOUT      Timeout SQLite (segundos)   default: 1
NANOBROKER_JANITOR_INTERVAL_SEC  Intervalo janitor     default: 30
'

# Verificar health
curl http://localhost:8000/health
```