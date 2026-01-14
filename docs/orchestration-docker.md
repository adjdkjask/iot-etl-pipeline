# Orquestación con Airflow

Airflow como orquestador para ejecutar el pipeline ETL de forma programada.

## Requisitos

- Docker Desktop corriendo
- Imagen ETL construida (ver [etl-docker.md](etl-docker.md))

## Estructura

```
orchestration/
├── docker-compose.yml   # Airflow + Postgres
├── dags/
│   └── iot_etl_dag.py   # DAG del pipeline
├── logs/                # Logs de ejecución
└── plugins/             # Extensiones (vacío)
```

## Inicio rápido

```bash
cd orchestration

# 0. Configurar variables de entorno (primera vez)
#    Editar .env en la raíz del proyecto y ajustar ETL_PROJECT_ROOT

# 1. Inicializar base de datos y usuario admin (solo primera vez)
docker-compose up airflow-init

# 2. Levantar servicios en background
docker-compose up -d

# 3. Acceder a la UI
#    URL: http://localhost:8080
#    Usuario: admin (por defecto, cambiar en producción)
#    Password: admin (por defecto, cambiar en producción)

## Seguridad y buenas prácticas

- Cambia las credenciales por defecto (`admin`/`admin`) y el secret key antes de exponer el sistema.
- El servicio `airflow-scheduler` corre como root para poder usar Docker-in-Docker. En producción, considera alternativas más seguras.
```

## Comandos

### Gestión de servicios

```bash
# Ver estado de contenedores
docker-compose ps

# Ver logs en tiempo real
docker-compose logs -f airflow-scheduler

# Reiniciar servicios
docker-compose restart
```

### Gestión del DAG

```bash
# Listar DAGs disponibles
docker-compose exec airflow-scheduler airflow dags list

# Ejecutar DAG manualmente
docker-compose exec airflow-scheduler airflow dags trigger iot_etl_pipeline

# Ver estado de ejecuciones
docker-compose exec airflow-scheduler airflow dags list-runs -d iot_etl_pipeline

# Probar DAG sin persistir en DB
docker-compose exec airflow-scheduler airflow dags test iot_etl_pipeline 2026-01-01
```

## Detener servicios

```bash
# Detener contenedores (preserva datos)
docker-compose down

# Reset completo (elimina base de datos)
docker-compose down -v
```

## DAG: iot_etl_pipeline

Ejecuta tres tareas secuenciales usando `DockerOperator`:

```
[Extract] → [Transform] → [Load]
   │            │            │
   ▼            ▼            ▼
data/raw    data/output  data/exports
```

Cada tarea:
1. Levanta un contenedor `iot-etl-pipeline-etl`
2. Ejecuta el módulo Python correspondiente
3. Se elimina automáticamente al terminar

### Configuración

Variables de entorno opcionales en `orchestration/docker-compose.yml`:

| Variable | Descripción | Default |
|----------|-------------|---------|
| `ETL_IMAGE_NAME` | Imagen Docker ETL | `iot-etl-pipeline-etl:latest` |
| `ETL_PROJECT_ROOT` | Ruta al proyecto en el host | Obligatoria (definir en `.env`; ver `.env.example`) |

### Schedule

Por defecto: `@daily` (medianoche UTC)

Modificar en `dags/iot_etl_dag.py`:
```python
schedule_interval="@hourly"  # Cada hora
schedule_interval="0 6 * * *"  # 6 AM diario
schedule_interval=None  # Solo manual
```

## Logs

Los logs se guardan en `orchestration/logs/` con la estructura:

```
logs/
├── dag_id=iot_etl_pipeline/
│   └── run_id=<timestamp>/
│       └── task_id=<extract|transform|load>/
│           └── attempt=N.log
└── scheduler/
    └── <fecha>/
        └── iot_etl_dag.py.log
```

### Leer logs desde CLI

```bash
# Logs del scheduler
docker-compose exec airflow-scheduler \
  cat /opt/airflow/logs/scheduler/2026-01-14/iot_etl_dag.py.log

# Logs de una tarea específica
docker-compose exec airflow-scheduler \
  cat "/opt/airflow/logs/dag_id=iot_etl_pipeline/run_id=<ID>/task_id=extract/attempt=1.log"
```

## Troubleshooting

### DAG no aparece en la UI

```bash
# Verificar errores de parsing
docker-compose exec airflow-scheduler airflow dags list-import-errors
```

### Tarea falla con error de Docker

Verificar que:
1. La imagen ETL existe: `docker images | grep iot-etl-pipeline`
2. El socket Docker está montado en el scheduler
3. El scheduler corre como root (necesario para Docker-in-Docker)
