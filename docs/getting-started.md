# Guía de Inicio Rápido

Cómo ejecutar el pipeline ETL de IoT completo.

## Prerrequisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- Archivo `.env` configurado con credenciales de la API (copiar de `.env.example`)

## Opción A: Ejecución manual (desarrollo)

Ideal para desarrollo, debugging y ejecuciones puntuales.

```bash
# 1. Construir imagen
docker-compose build

# 2. Ejecutar fases del pipeline

# Extract: descarga datos de API → data/raw/
docker-compose run --rm etl python -m extract.extractor

# Transform: limpia y modela → data/output/
docker-compose run --rm etl python -m transform.transformer

# Load: exporta a CSV → data/exports/
docker-compose run --rm etl python -m load.loader

# 3. Verificar resultados
ls data/exports/
```

**Ventajas:** Simple, rápido para iterar, no consume recursos extras.

📖 Documentación detallada: [etl-docker.md](etl-docker.md)

---

## Opción B: Ejecución orquestada (producción)

Ideal para ejecuciones programadas y monitoreo.

```bash
# 1. Construir imagen ETL (desde raíz)
docker-compose build

# 2. Inicializar Airflow (solo primera vez)
cd orchestration
docker-compose up airflow-init

# 3. Levantar Airflow
docker-compose up -d

# 4. Abrir UI y activar DAG
#    http://localhost:8080 (admin/admin)
```

**Ventajas:** Scheduling automático, reintentos, logs centralizados, UI de monitoreo.

📖 Documentación detallada: [orchestration-docker.md](orchestration-docker.md)

---

## Comparativa

| Aspecto | Manual | Orquestado |
|---------|--------|------------|
| Comando | `docker-compose run --rm etl` | Trigger desde UI/CLI |
| Scheduling | No | Sí (`@daily`, cron, etc.) |
| Reintentos | No | Sí (configurable) |
| Monitoreo | Logs en terminal | UI web + logs persistentes |
| Recursos | ~2.4GB (solo durante ejecución) | ~4GB (Airflow siempre corriendo) |
| Uso típico | Desarrollo, testing | Producción, pipelines recurrentes |

---

## Flujo de datos

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Extract   │ ──▶ │  Transform  │ ──▶ │    Load     │
└─────────────┘     └─────────────┘     └─────────────┘
      │                   │                   │
      ▼                   ▼                   ▼
  data/raw/          data/output/       data/exports/
  (Parquet)          (Parquet)          (CSV)
   Bronze              Gold              Export
```

---

## Verificar instalación

```bash
# Construir y ejecutar tests
docker-compose build
docker-compose run --rm test

# Resultado esperado: 94 tests passed
```

---

## Estructura del proyecto

```
iot-etl-pipeline/
├── docker-compose.yml      # Contenedor ETL
├── Dockerfile              # Imagen PySpark
├── .env                    # Variables de entorno (no commitear)
├── config/
│   └── path_config.py      # Rutas centralizadas
├── src/
│   ├── extract/            # Fase 1: Extracción
│   ├── transform/          # Fase 2: Transformación
│   └── load/               # Fase 3: Carga
├── data/
│   ├── raw/                # Datos crudos
│   ├── processed/          # Datos limpios
│   ├── output/             # Modelo dimensional
│   └── exports/            # CSVs finales
├── orchestration/
│   ├── docker-compose.yml  # Servicios Airflow
│   └── dags/               # Definiciones DAG
└── docs/                   # Esta documentación
```

---

## Comandos frecuentes

```bash
# === Desarrollo ===
docker-compose build                              # Construir imagen
docker-compose run --rm test                      # Correr tests
docker-compose run --rm etl bash                  # Shell interactivo

# === Airflow ===
cd orchestration
docker-compose up -d                              # Levantar servicios
docker-compose logs -f airflow-scheduler          # Ver logs
docker-compose exec airflow-scheduler \
  airflow dags trigger iot_etl_pipeline           # Trigger manual
docker-compose down                               # Detener

# === Limpieza ===
docker-compose down -v --rmi local                # Reset completo
```
