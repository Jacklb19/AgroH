# AgroIA Colombia 🌾 — Plataforma de Inteligencia Agrícola 2026

Bienvenido al repositorio de **AgroH / AgroIA Colombia**, una plataforma de inteligencia de datos diseñada para la resiliencia y el modelamiento agrícola en Colombia. Este sistema integra múltiples fuentes de datos públicos nacionales (clima, producción, precios, suelos y censos) bajo un pipeline ETL profesional y robusto, con validación de calidad basada en contratos semánticos y modelos predictivos de aprendizaje automático.

---

## 🏗️ Arquitectura de Datos y Star Schema

El proyecto está diseñado bajo una arquitectura de almacenamiento en estrella (**Star Schema**) implementada sobre PostgreSQL (Supabase), permitiendo consultas analíticas de alto rendimiento para tableros como Power BI o consumo de modelos IA.

```mermaid
graph TD
    %% Fuentes de datos crudos
    subgraph Fuentes de Datos (Raw)
        R_IDEAM[IDEAM Clima]
        R_PROD[MADR Producción]
        R_SIPSA[SIPSA Precios]
        R_SIPRA[SIPRA Suelos]
        R_CNA[DANE Censo]
        R_DIVIPOLA[DANE DIVIPOLA]
    end

    %% Módulos de Limpieza y Contratos
    subgraph Capa de Calidad (clean_*)
        C_CLIMA[clean_clima.py]
        C_PROD[clean_produccion.py]
        C_SIPSA[clean_precios.py / clean_sipsa.py]
        C_SIPRA[clean_suelo.py / clean_sipra.py]
        C_DIVIPOLA[clean_divipola.py]
        
        CONTRACTS{Validation Contracts}
    end

    %% Base de Datos
    subgraph Data Warehouse (Supabase PostgreSQL)
        DQL[(data_quality_log)]
        
        subgraph Dimensiones (dim_*)
            D_MPIO[dim_municipio]
            D_TIME[dim_tiempo]
            D_CULT[dim_cultivo]
            D_ESTA[dim_estacion_ideam]
            D_CENT[dim_central_abastos]
        end
        
        subgraph Tablas de Hechos (fact_*)
            F_PROD[fact_produccion_agricola]
            F_CLIMA[fact_clima_mensual]
            F_PRECIOS[fact_precios_mayoristas]
            F_SUELO[fact_aptitud_suelo]
            F_CENSO[fact_censo_agropecuario]
        end
    end

    %% Conexión de Flujo
    R_IDEAM --> C_CLIMA
    R_PROD --> C_PROD
    R_SIPSA --> C_SIPSA
    R_SIPRA --> C_SIPRA
    R_DIVIPOLA --> C_DIVIPOLA

    C_CLIMA & C_PROD & C_SIPSA & C_SIPRA & C_DIVIPOLA --> CONTRACTS
    
    CONTRACTS -->|Inválidos (CSV)| CSV_OUT[data/processed/invalid/]
    CONTRACTS -->|Métricas de Calidad| DQL
    CONTRACTS -->|Válidos| D_MPIO
    CONTRACTS -->|Válidos| D_TIME
    CONTRACTS -->|Válidos| D_CULT
    CONTRACTS -->|Válidos| D_ESTA
    CONTRACTS -->|Válidos| D_CENT

    D_MPIO & D_TIME & D_CULT & D_ESTA & D_CENT --> F_PROD & F_CLIMA & F_PRECIOS & F_SUELO & F_CENSO
```

### 1. Dimensiones (`dim_*`)
*   `dim_region_natural`: Regiones naturales de Colombia.
*   `dim_municipio`: Catálogo de municipios y departamentos indexado por código DIVIPOLA de 5 dígitos, con latitud/longitud de centroides.
*   `dim_tiempo`: Eje temporal mensualizado que asocia fases ENSO (El Niño / La Niña).
*   `dim_cultivo`: Catálogo estandarizado de cultivos, ciclos (transitorio/permanente) y familias botánicas.
*   `dim_estacion_ideam`: Catálogo de estaciones climáticas con coordenadas y asociación municipal.
*   `dim_central_abastos`: Red de centrales mayoristas del país.

### 2. Hechos (`fact_*`)
*   `fact_produccion_agricola`: Áreas sembradas, cosechadas, producción en toneladas y rendimiento por hectárea.
*   `fact_clima_mensual`: Consolidado mensual de lluvias, temperaturas (media, min, max), humedad relativa y brillo solar.
*   `fact_precios_mayoristas`: Precios mínimos, máximos y promedios transados de productos agrícolas.
*   `fact_aptitud_suelo`: Aptitud municipal para cultivos específicos según la UPRA/SIPRA.
*   `fact_censo_agropecuario`: Variables agregadas del CNA de cultivos permanentes vs. transitorios.

---

## 🚀 Flujo de Ejecución y Validaciones

La ejecución oficial del pipeline integra el validador semántico **Pandera** y reporta métricas directo a la base de datos.

### Comando Recomendado (Producción)

Para ejecutar el pipeline end-to-end con validación rigurosa de calidad de datos, usa:

```bash
python run_pipeline.py --mode all --once --use-clean-modules
```

> [!TIP]
> **Modo Scheduler:** Si omites el flag `--once`, el pipeline iniciará en segundo plano (utilizando `APScheduler`) programado para ejecutarse todos los **lunes a las 02:00 AM** hora de Bogotá.

### Modos de Ejecución (`--mode`)
1.  `core`: Ejecuta DIVIPOLA, Producción Agrícola y Clima IDEAM.
2.  `extended`: Ejecuta NOAA ENSO, SIPSA Precios, SIPRA Suelos y CNA Censo.
3.  `models`: Construye el Feature Store e induce los modelos predictivos de Machine Learning.
4.  `all` (Default): Ejecuta todos los anteriores secuencialmente.

---

## 🛡️ Contratos de Calidad y Robustez

El pipeline implementa una política no destructiva para rechazar datos corruptos o físicamente imposibles, asegurando la integridad transaccional.

### 1. Contratos Semánticos (`Pandera`)
Ubicados en la carpeta `clean/`, definen límites duros y tipos de datos esperados para cada fuente:
*   **Códigos Municipales:** Deben cumplir la norma DIVIPOLA (5 dígitos numéricos).
*   **Límites Físicos Climáticos:** Precipitación mensual entre `[0, 3000]` mm/mes, temperaturas en `[-10, 50]`°C, humedad en `[0, 100]`%.
*   **Reglas de Producción Agrícola:** El área cosechada jamás puede superar el área sembrada. Los rendimientos y producciones no pueden ser negativos.

### 2. Auditoría e Historial de Calidad (`data_quality_log`)
Cada ejecución del pipeline con `--use-clean-modules` registra los resultados en la tabla `data_quality_log` en PostgreSQL:
*   `fuente`: Módulo evaluado (ej. `produccion_agricola`, `clima_ideam`).
*   `total_raw`: Total de registros ingresados.
*   `total_validos`: Registros que superaron las reglas de calidad y se cargaron en BD.
*   `total_invalidos`: Registros descartados.
*   `error_summary_json`: Conteo y resumen agrupado de las reglas infringidas (ej. `schema_error`, `business_rule_error`).

### 3. Registro de Descartes (Archivos Inválidos)
Los datos que no superan las validaciones son almacenados de forma segura en formato CSV dentro del directorio local:
```path
data/processed/invalid/
```
Esto permite a los ingenieros de datos auditar los problemas de origen sin detener el flujo de negocio del pipeline.

---

## 📂 Estructura del Proyecto

El repositorio está estrictamente organizado bajo las siguientes subcarpetas para garantizar mantenibilidad:

```path
AgroH/
├── clean/               # Módulos de limpieza base y contratos Pandera
│   ├── clean_clima.py
│   ├── clean_divipola.py
│   ├── clean_insumos.py
│   ├── clean_municipios.py
│   ├── clean_precios.py
│   ├── clean_produccion.py
│   ├── clean_sipra.py
│   ├── clean_sipsa.py
│   ├── clean_suelo.py
│   ├── quality_checks.py
│   └── validation_contracts*.py
├── config/              # Parámetros, variables y homónimos de municipios
│   ├── settings.py
│   └── synonyms_municipios.csv
├── data/                # Almacén de archivos de datos locales
│   ├── raw/             # Datos crudos extraídos de APIs/fuentes
│   └── processed/       # Parquets limpios y descartes invalid/*.csv
├── docs/                # Documentación del flujo y guías de calidad
│   └── data_quality_workflow.md
├── extract/             # Conectores y scrappers de APIs (IDEAM, NOAA, Socrata)
│   ├── extract_produccion.py
│   └── extract_clima.py
├── load/                # Conexiones e inserción SQL en PostgreSQL
│   ├── db.py            # Motor de base de datos
│   ├── schema.sql       # DDL completo de la BD
│   ├── load_dimensions.py
│   └── load_facts.py
├── logs/                # Directorio autogenerado para logs detallados del ETL
├── models/              # Almacén del Feature Store e inducción de modelos IA
│   ├── build_features.py
│   ├── train_rendimiento.py
│   └── train_alerta_climatica.py
├── validate/            # Auditoría post-limpieza y verificación de BD
│   ├── audit_clean.py
│   └── quality_report.py
├── requirements.txt     # Dependencias de librerías Python
└── run_pipeline.py      # Orquestador y planificador central
```

---

## ⚠️ Componentes Deprecados (`Legacy Mode`)

El flujo legacy de limpieza (que se ejecuta omitiendo `--use-clean-modules`) se encuentra actualmente **DEPRECADO**. 
Se mantiene exclusivamente por razones de compatibilidad transitoria y no cuenta con validación por contratos ni auditoría en base de datos. 
Las ramas obsoletas en `run_pipeline.py` están marcadas y su eliminación está prevista en la siguiente versión mayor.

---

## 🛠️ Instalación y Requisitos

1.  **Python 3.10+**
2.  Instala las dependencias necesarias:
    ```bash
    pip install -r requirements.txt
    ```
3.  Configura tus variables de conexión en un archivo `.env` en la raíz del proyecto:
    ```env
    SUPABASE_DB_USER=postgres.[tu_usuario]
    SUPABASE_DB_PASS=[tu_contraseña]
    SUPABASE_DB_HOST=[tu_host]
    SUPABASE_DB_PORT=5432
    SUPABASE_DB_NAME=postgres
    ```
