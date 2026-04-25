# AgroIA Colombia — Pipeline de Datos Agrícolas

Proyecto del hackathon **Datos al Ecosistema 2026**.  
Sistema ETL + modelos de IA para monitoreo de rendimiento agrícola y alertas climáticas en Colombia.

---

## Para los integrantes del equipo — primeros pasos

Si acabás de clonar el repo, seguí estos pasos en orden. **No necesitás descargar ni mandar nada por WhatsApp** — todos los archivos de datos manuales ya están incluidos en el repositorio (SIPSA, SIPRA, CNA, boletines PDF).

### Paso 1 — Clonar el repositorio

```bash
git clone <URL-del-repo>
cd AgroH
```

### Paso 2 — Instalar dependencias

```bash
pip install -r requirements.txt
```

### Paso 3 — Crear el archivo `.env`

Crear un archivo llamado `.env` en la raíz del proyecto. Pedirle a Steven los valores y copiarlos exactamente así:

```env
# Conexión a PostgreSQL / Supabase
SUPABASE_DB_HOST=<host>
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=<contraseña>

# URLs fijas de boletines ENSO (respaldo si el scraping del IDEAM falla)
# Separadas por coma, sin espacios. Dejar vacío si no se tienen.
ENSO_BOLETIN_URLS=
```

> **Nunca subas el `.env` al repositorio.** Ya está en el `.gitignore`.

#### Conectar con Supabase

1. Ir a [supabase.com](https://supabase.com) → tu proyecto → **Settings → Database**
2. Copiar los datos de conexión en el `.env`:

```env
SUPABASE_DB_HOST=db.xxxxxxxxxxxx.supabase.co
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=<tu-contraseña-de-supabase>
```

3. El pipeline detecta automáticamente que es Supabase (host distinto de localhost) y activa SSL.

### Paso 4 — Correr el pipeline

```bash
# Todo de una sola vez (recomendado para primera vez)
python run_pipeline.py --once --mode all
```

Si querés correrlo por partes:

```bash
python run_pipeline.py --once --mode core       # DIVIPOLA, producción, clima, dimensiones
python run_pipeline.py --once --mode extended   # SIPSA, SIPRA, ENSO, CNA, insumos
python run_pipeline.py --once --mode models     # Entrena los dos modelos de IA
```

La primera ejecución descarga los datos de APIs externas (~5-10 minutos). Las siguientes son más rápidas porque usa caché local.

### Paso 5 — (Opcional) Shapefile de Frontera Agrícola

Este archivo pesa 765 MB y no puede subirse a Git. El pipeline funciona perfectamente sin él. Si necesitás la capa de Frontera Agrícola:

1. Ir a [sipra.upra.gov.co](https://sipra.upra.gov.co) → Descarga de Información → Frontera Agrícola
2. Descomprimir y copiar todos los archivos (`Frontera_Agricola_Jun2025.*`) a:
   ```
   data/raw/manual/sipra/
   ```

---

## Qué hace este proyecto

Descarga, limpia y carga datos agrícolas y climáticos de Colombia en una base de datos PostgreSQL con esquema estrella. Luego entrena dos modelos de IA y deja todo listo para conectar con Power BI o una página web.

### Fuentes de datos

| Fuente | Qué contiene | Cómo se obtiene |
|---|---|---|
| datos.gov.co (Socrata API) | Producción agrícola, DIVIPOLA, estaciones IDEAM, clima | Automático |
| IDEAM — ideam.gov.co | Boletines ENSO en PDF | **Automático** (scraping) |
| SIPSA / DANE | Precios mayoristas de alimentos 2013-2026 | Archivos en el repo |
| SIPRA / UPRA | Aptitud de suelos para arroz, maíz, papa | Archivos en el repo |
| CNA 2014 / DANE | Censo Nacional Agropecuario por departamento | Archivo en el repo |

---

## Módulos del proyecto

```
AgroH/
├── config/
│   ├── settings.py               # Rutas, años, URLs de fuentes y variables de entorno
│   └── synonyms_municipios.csv   # 225+ equivalencias de nombres de municipios
│
├── extract/                      # Descarga y lectura de datos
│   ├── extract_divipola.py       # DIVIPOLA — códigos municipales oficiales (con caché)
│   ├── extract_produccion.py     # Producción agrícola A04/A05 datos.gov.co (con caché)
│   ├── extract_ideam_estaciones.py  # Estaciones meteorológicas IDEAM (con caché)
│   ├── extract_ideam_clima.py    # Clima mensual IDEAM — precipitación y temperatura
│   ├── extract_ideam_pdf.py      # Boletines ENSO: scraping automático + PDFs locales
│   ├── extract_municipios_geo.py # Genera polígonos Voronoi desde centroides DIVIPOLA
│   ├── extract_sipra.py          # Capas GeoJSON de aptitud SIPRA (archivos del repo)
│   ├── extract_sipsa.py          # Precios SIPSA desde Excel multi-hoja (archivos del repo)
│   └── extract_insumos.py        # Precios insumos: API → archivos manuales → sintético
│
├── clean/
│   ├── clean_municipios.py       # Normaliza nombres de municipios a códigos DIVIPOLA
│   ├── clean_clima.py            # Unifica clima a granularidad mensual
│   ├── clean_precios.py          # Normaliza precios SIPSA (coalescing multi-columna)
│   ├── clean_suelo.py            # Agrega aptitud de suelo por municipio y cultivo
│   └── clean_insumos.py          # Normaliza precios de insumos agrícolas
│
├── load/
│   ├── schema.sql                # DDL completo — crea las 17 tablas si no existen
│   ├── db.py                     # Conexión SQLAlchemy + upsert() con sanitización NaN
│   ├── load_dimensions.py        # Carga dimensiones
│   └── load_facts.py             # Carga tablas de hechos y predicciones
│
├── models/
│   ├── train_rendimiento.py      # XGBoost Regressor — predicción t/ha
│   └── train_alerta_climatica.py # XGBoost Classifier — riesgo BAJO/MEDIO/ALTO
│
├── validate/
│   └── quality_report.py         # Reporte automático de calidad con alertas
│
├── data/
│   └── raw/
│       ├── manual/
│       │   ├── sipra/            # GeoJSON aptitud SIPRA (en el repo)
│       │   ├── sipsa/            # Excel de precios SIPSA 2013-2026 (en el repo)
│       │   ├── cna/              # CSV Censo Agropecuario 2014 (en el repo)
│       │   └── municipios/       # GeoJSON Voronoi municipal (se genera solo)
│       └── boletines_pdf/        # PDFs ENSO descargados automáticamente del IDEAM
│
├── logs/                         # Logs de ejecución (no se suben a git)
├── run_pipeline.py               # Orquestador principal con APScheduler
└── requirements.txt
```

---

## Cómo funciona el scraping de boletines ENSO

El pipeline descarga automáticamente los boletines PDF del IDEAM sin intervención manual:

1. **Scraping automático** — escanea las primeras 5 páginas de `ideam.gov.co/nuestra-entidad/meteorologia/boletines`, filtra PDFs con palabras clave ENSO/agroclimático y los descarga
2. **URLs fijas** desde `ENSO_BOLETIN_URLS` en el `.env` como respaldo si el scraping falla
3. **PDFs locales** en `data/raw/boletines_pdf/` — cualquier PDF ahí es procesado automáticamente

El sistema reconoce todos los formatos de nombre que usa el IDEAM:

| Formato | Ejemplo | Resultado |
|---|---|---|
| Mes abreviado + año | `IDEAM_ENSO_boletin_jul2025.pdf` | 2025 T3 |
| Mes completo + año | `boletin_agroclimatico_nacional_marzo_2026.pdf` | 2026 T1 |
| Mes + día + año | `04_enso_ifn_abr_24_2026.pdf` | 2026 T2 |
| Trimestre explícito | `boletin_2025_T3.pdf` | 2025 T3 |
| Sin fecha | `17411`, `protocolo_boletin.pdf` | Omitido |

---

## Base de datos — 17 tablas

El pipeline crea y pobla automáticamente todas las tablas al correr por primera vez.

### Dimensiones

| Tabla | Descripción | Registros |
|---|---|---|
| `dim_region_natural` | 5 regiones naturales de Colombia | 5 |
| `dim_tiempo` | Meses 2007–2025 | 228 |
| `dim_municipio` | Municipios con código DIVIPOLA | 1.122 |
| `dim_cultivo` | Cultivos con familia y ciclo | 223 |
| `dim_estacion_ideam` | Estaciones meteorológicas IDEAM | 9.400 |
| `dim_central_abastos` | Centrales de abasto SIPSA | 92 |

### Hechos

| Tabla | Descripción | Registros |
|---|---|---|
| `fact_produccion_agricola` | Área, producción y rendimiento por municipio/cultivo/año | 145.322 |
| `fact_clima_mensual` | Precipitación, temperatura, humedad mensual por estación | 1.858 |
| `fact_alerta_enso` | Fase ENSO, SPI e índices climáticos por región/trimestre | 24 |
| `fact_precios_mayoristas` | Precio COP/kg por producto y central de abastos | 21.249 |
| `fact_aptitud_suelo` | Aptitud agrícola SIPRA por municipio y cultivo | 51 |
| `fact_censo_agropecuario` | Indicadores CNA 2014 por departamento | 33 |
| `fact_precios_insumos` | Precios de fertilizantes, agroquímicos, mano de obra | 936 |

### Predicciones y modelos

| Tabla | Descripción | Registros |
|---|---|---|
| `pred_rendimiento` | Rendimiento predicho t/ha con intervalo de confianza | 145.322 |
| `pred_alerta_climatica` | Riesgo BAJO/MEDIO/ALTO por municipio y mes | 1.578 |
| `model_version` | Versiones y métricas de modelos entrenados | 3 |

---

## Modelos de IA

### Modelo 1 — Rendimiento Agrícola (XGBoost Regressor)

Predice cuántas toneladas por hectárea se esperan en un municipio para un cultivo dado.

- **Features:** año, área sembrada, área cosechada, temperatura media/máx/mín, precipitación
- **Métricas:** R² = 0.75 | MAE = 4.04 t/ha | RMSE = 12.09 t/ha
- **Salida:** 145.322 predicciones en `pred_rendimiento` con intervalo de confianza (±MAE)

### Modelo 2 — Alerta Climática (XGBoost Classifier)

Clasifica el riesgo climático por municipio como BAJO / MEDIO / ALTO.

- **Features:** precipitación, temperatura, humedad, brillo solar, fase ENSO, SPI, anomalía de precipitación
- **Nota:** mejora con más datos IDEAM históricos (desbloquear `.skip` files)
- **Salida:** 1.578 predicciones en `pred_alerta_climatica`

---

## Caché y archivos `.skip`

El pipeline no re-descarga datos si ya existen localmente:

- **CSV/Parquet en `data/raw/`** — si el archivo existe se usa directamente. Borrarlo para forzar re-descarga.
- **Archivos `.skip` en `data/raw/clima/`** — marcan años que fallaron por timeout. Para reintentar:

```bash
# Windows
del data\raw\clima\clima_combinado_mensual_2023.skip

# Linux/Mac
rm data/raw/clima/clima_combinado_mensual_2023.skip

# Luego correr de nuevo
python run_pipeline.py --once --mode core
```

---

## Modo automático (scheduler semanal)

```bash
# ETL core cada lunes 2:00 AM + modelos cada lunes 4:00 AM (hora Bogotá)
python run_pipeline.py

# En segundo plano sin ventana (Windows)
pythonw run_pipeline.py
```

---

## Conectar Power BI

1. Abrir Power BI Desktop
2. **Obtener datos → PostgreSQL**
3. Ingresar el host y nombre de la BD del `.env`
4. Usar **DirectQuery** para datos en tiempo real o **Import** para snapshots
5. Las tablas principales para cada dashboard:

| Dashboard | Tablas |
|---|---|
| Clima & Precipitaciones | `fact_clima_mensual`, `dim_estacion_ideam`, `dim_municipio` |
| Rendimiento de Cultivos | `fact_produccion_agricola`, `pred_rendimiento`, `dim_cultivo` |
| Precios de Mercado | `fact_precios_mayoristas`, `dim_central_abastos` |
| Alertas Climáticas | `pred_alerta_climatica`, `fact_alerta_enso`, `dim_region_natural` |
| Insumos & Suelo | `fact_precios_insumos`, `fact_aptitud_suelo`, `fact_censo_agropecuario` |

---

## Integrar en una página web

Los dashboards de Power BI se pueden embeber en cualquier sitio web una vez publicados en **Power BI Service**:

1. Publicar el reporte en Power BI Service (cuenta gratuita)
2. **Archivo → Publicar en web** → copiar el iframe generado
3. Pegar el iframe en el HTML de tu página:

```html
<iframe
  title="AgroIA - Rendimiento"
  width="100%" height="600"
  src="https://app.powerbi.com/reportEmbed?reportId=XXXX"
  frameborder="0" allowFullScreen>
</iframe>
```

Para el chatbot conectado a los datos:
- El backend (Flask o FastAPI) consulta PostgreSQL y llama a la API de Claude
- El widget de chat se agrega al HTML como un componente JavaScript

---

## Datos en el repo vs datos que descarga el pipeline

| Dato | ¿Está en el repo? | Observación |
|---|---|---|
| SIPRA — aptitud arroz/maíz/papa (GeoJSON) | ✅ Sí | `data/raw/manual/sipra/` |
| SIPSA — precios 2013-2026 (Excel) | ✅ Sí | `data/raw/manual/sipsa/` |
| CNA 2014 (CSV) | ✅ Sí | `data/raw/manual/cna/` |
| Boletines ENSO (PDF) | ✅ Los del repo | Se descargan más automáticamente |
| Producción agrícola A04/A05 | ❌ Se descarga | datos.gov.co API |
| Estaciones IDEAM | ❌ Se descarga | datos.gov.co API |
| DIVIPOLA | ❌ Se descarga | datos.gov.co API |
| Clima mensual IDEAM | ❌ Se descarga | datos.gov.co API |
| Frontera Agrícola (shapefile) | ❌ 765 MB | Descargar de sipra.upra.gov.co |

---

## Próximos pasos

- [ ] Conectar Power BI a PostgreSQL/Supabase (5 dashboards)
- [ ] Publicar en Power BI Service y obtener iframes
- [ ] Armar página web con los 5 dashboards embebidos
- [ ] API de clima en tiempo real (OpenWeatherMap) para datos cada 15 minutos
- [ ] Chatbot con Claude API — lee la BD y da recomendaciones en lenguaje natural por municipio

---

## Equipo

Hackathon Datos al Ecosistema 2026 — AgroIA Colombia
