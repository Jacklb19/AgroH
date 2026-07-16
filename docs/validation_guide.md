# Guía de Validación — AgroIA Colombia

Guía paso a paso para reproducir los resultados del proyecto, ejecutar las pruebas estadísticas ANOVA y verificar el comportamiento del modelo predictivo.

---

## Requisitos Previos

### Software Requerido

| Herramienta | Versión mínima | Propósito |
|-------------|---------------|-----------|
| Python | 3.11+ | Pipeline ETL y modelos |
| Node.js | 18+ | Frontend Next.js |
| PostgreSQL | 15 | Base de datos local (opcional) |
| Git | — | Control de versiones |

### Instalación del Entorno Python

```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/AgroIA-Colombia.git
cd AgroIA-Colombia

# Crear entorno virtual
python -m venv venv

# Activar el entorno (Windows)
venv\Scripts\activate

# Activar el entorno (Linux/Mac)
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

### Dependencias Python (`requirements.txt`)

```
pandas>=2.0
numpy>=1.24
scipy>=1.10
statsmodels>=0.14
matplotlib>=3.7
xgboost>=2.0
optuna>=3.0
shap>=0.42
scikit-learn>=1.3
sqlalchemy>=2.0
psycopg2-binary>=2.9
python-dotenv>=1.0
requests>=2.31
rich>=13.0
```

### Configuración de Variables de Entorno

Crear archivo `.env` en la raíz del proyecto:

```bash
cp .env.example .env
```

Editar `.env` con tus credenciales:

```env
# Base de datos Supabase
SUPABASE_DB_HOST=tu-host.supabase.co
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=tu-password

# Token Socrata (datos.gov.co) — opcional pero recomendado
SOCRATA_TOKEN=tu-token-socrata

# Año de fin del pipeline
PIPELINE_YEAR_END=2025
```

---

## Validación 1: Pruebas Estadísticas ANOVA

### Descripción
Las pruebas ANOVA validan las hipótesis del proyecto sobre la relación entre clima y producción agrícola en Colombia. Pueden ejecutarse **sin conexión a base de datos** si los archivos CSV de datos están presentes.

### Ejecución

```bash
# Ejecutar todas las pruebas ANOVA (modo básico)
python -m validate.anova_tests

# Ejecutar con salida detallada (estadísticos + Tukey HSD)
python -m validate.anova_tests --verbose
```

### Resultados Esperados

La ejecución debe producir:

**Consola (con --verbose):**
```
Test 1: Precipitación vs. Fase ENSO
  Levene: F=XX.XX, p=0.000XXX
  ANOVA:  F=XXX.XX, p=0.000XXX ***
  Tukey HSD: El Niño vs La Niña: p=0.000XXX (significativo)
             El Niño vs Neutro:  p=0.000XXX (significativo)
             La Niña vs Neutro:  p=0.000XXX (significativo)

Test 2: Precio de Insumos vs. Tipo
  [...]

Test 3: Precipitación vs. Trimestre
  [...]

Test 4: Precipitación NASA vs. Municipio
  [...]

RESUMEN: 4/4 pruebas estadísticamente significativas (p < 0.001)
```

**Archivos generados en `data/quality_reports/`:**
```
anova_enso_lluvia.png          ← Boxplot ENSO vs. precipitación
anova_insumos_precio.png       ← Boxplot tipo insumo vs. precio
anova_estacionalidad_lluvia.png ← Boxplot trimestre vs. precipitación
anova_nasa_municipio.png       ← Boxplot municipio vs. precipitación NASA
```

### Criterios de Validación ANOVA

| Prueba | Criterio de éxito |
|--------|-----------------|
| Test de Levene | p documentado (aunque viole homocedasticidad, el ANOVA de un solo factor es robusto) |
| ANOVA F-test | **p < 0.05** para considerarse estadísticamente significativo |
| Tukey HSD | Al menos un par de grupos con diferencia significativa |

---

## Validación 2: Calidad de Datos

### Descripción
Verifica que la base de datos cumple los estándares de calidad definidos para el proyecto.

### Requisito
Conexión activa a PostgreSQL (Supabase). Configurar `.env` correctamente.

### Ejecución

```bash
# Reporte de calidad de datos
python -m validate.quality_report

# Auditoría completa de fuentes y limpieza
python -m validate.audit_clean
python -m validate.audit_sources
```

### Resultados Esperados

```
╔══════════════════════════════════════════════════════╗
║          Reporte de Calidad de Datos                 ║
╠══════════════════════════════════════════════════════╣
║ municipios_con_cobertura_climatica  │  87.3%  │  OK  ║
║ registros_sin_municipio             │   0.0%  │  OK  ║
║ municipios_rendimiento_nulo         │   2.1%  │  OK  ║
║ modelos_activos_duplicados          │   0     │  OK  ║
║ estaciones_sin_municipio            │   3.4%  │  OK  ║
║ duplicados_clima_mensual            │   0     │  OK  ║
║ cobertura_temporal_clima_anios      │   8     │  OK  ║
║ trimestres_enso_faltantes           │   0     │  OK  ║
╚══════════════════════════════════════════════════════╝
```

### Criterios de Éxito

| Check | Umbral de éxito |
|-------|----------------|
| `municipios_con_cobertura_climatica` | ≥ 80% |
| `registros_sin_municipio` | = 0% |
| `municipios_rendimiento_nulo` | ≤ 5% |
| `modelos_activos_duplicados` | = 0 |
| `estaciones_sin_municipio` | ≤ 5% |
| `duplicados_clima_mensual` | = 0 |

---

## Validación 3: Pipeline ETL Completo

### Descripción
Verifica que el pipeline extrae, limpia y carga los datos correctamente.

### Ejecución Paso a Paso

```bash
# Paso 1: Solo ETL Core (Producción + Clima IDEAM)
python run_pipeline.py --mode etl --once

# Paso 2: Verificar que los datos cargaron
python -m validate.quality_report

# Paso 3: ETL Extendido (Precios, Suelos, ENSO)
python run_pipeline.py --mode etl_extended --once

# Paso 4: Pipeline completo
python run_pipeline.py --mode all --once
```

### Verificación Manual Rápida (SQL)

```sql
-- Verificar registros cargados en tablas principales
SELECT 'fact_produccion_agricola' AS tabla, COUNT(*) AS registros FROM fact_produccion_agricola
UNION ALL
SELECT 'fact_clima_mensual', COUNT(*) FROM fact_clima_mensual
UNION ALL
SELECT 'fact_alerta_enso', COUNT(*) FROM fact_alerta_enso
UNION ALL
SELECT 'fact_precios_insumos', COUNT(*) FROM fact_precios_insumos
UNION ALL
SELECT 'dim_municipio', COUNT(*) FROM dim_municipio
UNION ALL
SELECT 'dim_estacion_ideam', COUNT(*) FROM dim_estacion_ideam;
```

**Resultados esperados (mínimos para que el sistema funcione):**

| Tabla | Registros mínimos |
|-------|------------------|
| `fact_produccion_agricola` | > 50.000 |
| `fact_clima_mensual` | > 10.000 |
| `fact_alerta_enso` | > 1.000 |
| `dim_municipio` | > 1.000 |
| `dim_estacion_ideam` | > 500 |

---

## Validación 4: Modelo Predictivo

### Descripción
Verifica que el modelo XGBoost se entrena correctamente y produce predicciones razonables.

### Requisito
Datos en la base de datos (`fact_produccion_agricola` y `fact_clima_mensual` poblados).

### Ejecución

```bash
# Entrenar y evaluar el modelo
python -m models.train_rendimiento
```

### Resultados Esperados

```json
{
  "model_name": "xgboost_rendimiento",
  "metrics": {
    "mae": 0.42,
    "rmse": 0.71,
    "r2": 0.83,
    "n_train": 45000,
    "n_test": 11000,
    "n_features": 35,
    "optuna_trials": 200,
    "split_year": 2022
  }
}
```

### Criterios de Éxito del Modelo

| Métrica | Umbral de éxito | Descripción |
|---------|----------------|-------------|
| **R²** | ≥ 0.70 | El modelo explica al menos el 70% de la varianza |
| **MAE** | ≤ 1.0 t/ha | Error promedio menor a 1 tonelada por hectárea |
| **RMSE** | ≤ 1.5 t/ha | RMSE menor a 1.5 t/ha |

### Verificar Predicciones en la Base de Datos

```sql
-- Ver predicciones generadas con su explicación SHAP
SELECT 
    m.nombre_municipio,
    c.nombre_cultivo,
    t.anio,
    pr.rendimiento_predicho_t_ha,
    pr.intervalo_confianza_inferior,
    pr.intervalo_confianza_superior,
    pr.shap_top
FROM pred_rendimiento pr
JOIN dim_municipio m ON m.id_municipio = pr.id_municipio
JOIN dim_cultivo c ON c.id_cultivo = pr.id_cultivo
JOIN dim_tiempo t ON t.id_tiempo = pr.id_tiempo
WHERE pr.rendimiento_predicho_t_ha IS NOT NULL
ORDER BY t.anio DESC, pr.rendimiento_predicho_t_ha DESC
LIMIT 20;
```

---

## Validación 5: Aplicación Web

### Instalación del Frontend

```bash
cd web
npm install
```

### Ejecución en Modo Desarrollo

```bash
npm run dev
# → http://localhost:3000
```

### Checklist de Validación Manual

Abrir el navegador en `http://localhost:3000` y verificar:

#### Página de Inicio (`/`)
- [ ] KPIs se cargan (municipios en alerta, cultivos analizados)
- [ ] Mapa SVG de Colombia se renderiza correctamente
- [ ] Alertas ENSO activas aparecen en el panel lateral

#### Dashboards (`/dashboard`)
- [ ] Gráfico de producción histórica por cultivo se carga
- [ ] Filtro por municipio funciona
- [ ] Gráfico de clima (precipitación/temperatura) responde al filtro

#### Predicción (`/prediccion`)
- [ ] Formulario acepta: municipio, cultivo, año, fase ENSO, lluvia esperada
- [ ] Respuesta devuelve rendimiento predicho con intervalo de confianza
- [ ] Panel SHAP muestra las variables más influyentes

#### Asistente IA (`/asistente`)
- [ ] Chat responde preguntas en español
- [ ] Memoria de sesión funciona entre mensajes
- [ ] Respuestas incluyen datos relevantes del contexto agro-climático

#### Metodología (`/metodologia`)
- [ ] Gráficos ANOVA (boxplots) se visualizan correctamente
- [ ] Tabla de métricas del modelo muestra R², MAE, RMSE
- [ ] Descripción del pipeline es legible

---

## Solución de Problemas Comunes

| Problema | Causa probable | Solución |
|----------|---------------|---------|
| `Connection refused` al conectar a BD | Credenciales `.env` incorrectas | Verificar `SUPABASE_DB_HOST` y `SUPABASE_DB_PASSWORD` |
| `ModuleNotFoundError: xgboost` | XGBoost no instalado | `pip install xgboost` |
| Gráficos ANOVA no se generan | Directorio `data/quality_reports/` no existe | `mkdir data/quality_reports` |
| Feature Store vacío | No hay datos en `fact_produccion_agricola` | Ejecutar `python run_pipeline.py --mode etl --once` |
| Frontend no conecta a Supabase | Variables `.env` del frontend | Verificar `NEXT_PUBLIC_SUPABASE_URL` en `web/.env` |
| `ANOVA test: no data for group` | Datos insuficientes en la BD | Ejecutar el ETL completo primero |

---

## Ejecución del Demo Completo

Para una demostración completa del sistema desde cero:

```bash
# 1. Configurar entorno
pip install -r requirements.txt

# 2. Configurar credenciales
cp .env.example .env  # Editar con credenciales reales

# 3. Ejecutar pipeline completo
python run_pipeline.py --mode all --once

# 4. Ejecutar pruebas ANOVA
python -m validate.anova_tests --verbose

# 5. Verificar calidad
python -m validate.quality_report

# 6. Iniciar aplicación web
cd web && npm install && npm run dev
```

El demo completo (pasos 3-4) puede tardar **30-90 minutos** dependiendo de la velocidad de conexión y el volumen de datos disponibles en las APIs.

---

*Versión del documento: 1.0 — Julio 2026*
