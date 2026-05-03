# 🌿 AgroIA Colombia: Sistema de Inteligencia para la Resiliencia Agrícola

![Status](https://img.shields.io/badge/Status-Production--Ready-success?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3.9+-blue?style=for-the-badge&logo=python)
![Database](https://img.shields.io/badge/Database-PostgreSQL%20%7C%20Supabase-green?style=for-the-badge&logo=postgresql)
![ML](https://img.shields.io/badge/ML-XGBoost%20%7C%20Scikit--Learn-orange?style=for-the-badge)

**AgroIA Colombia** (AgroH) es una plataforma profesional de ingeniería de datos y aprendizaje automático diseñada para fortalecer la toma de decisiones en el sector agropecuario colombiano. El sistema integra múltiples fuentes de datos abiertos (clima, producción, precios y suelos) para generar modelos predictivos de rendimiento y alertas climáticas.

---

## 🚀 Funcionalidades Principales

### 🔄 Pipeline ETL (Extracción, Transformación y Carga)
El proyecto cuenta con un orquestador robusto (`run_pipeline.py`) dividido en fases:

1.  **ETL Core**:
    *   **Geografía**: Normalización de DIVIPOLA y mapeo de regiones naturales.
    *   **Producción**: Procesamiento de datos históricos de producción agrícola (Área sembrada, cosechada, rendimiento).
    *   **Clima (IDEAM)**: Extracción automatizada de series históricas de precipitación y variables combinadas desde Socrata (V4).
    *   **Dimensiones**: Gestión de catálogos maestros para municipios, cultivos y estaciones meteorológicas.

2.  **ETL Extendido**:
    *   **Precios Mayoristas (SIPSA)**: Monitoreo de precios en centrales de abastos.
    *   **Insumos (IPIA)**: Seguimiento a costos de producción.
    *   **Suelos (SIPRA/UPRA)**: Análisis de aptitud de suelo por municipio.
    *   **Fenómenos Climáticos**: Integración con el índice ONI de la **NOAA** para el monitoreo de El Niño/La Niña (ENSO).
    *   **Censo Nacional Agropecuario (CNA)**: Consolidación de datos estructurales del campo.

### 🧠 Inteligencia Artificial y Machine Learning
*   **Feature Store**: Generación automática de variables latentes cruzando clima, precios y geografía.
*   **Modelos de Rendimiento**: Predicción de toneladas por hectárea mediante algoritmos de regresión (XGBoost/Random Forest).
*   **Alertas Climáticas**: Clasificación de riesgos climáticos para la planificación de siembras.

### 📊 Monitoreo y Calidad
*   **Reportes de Calidad**: Validación automática de integridad, nulos y consistencia estadística.
*   **UI por Consola**: Interfaz elegante basada en `Rich` con barras de progreso y paneles informativos.
*   **Scheduler**: Programación de tareas mediante `APScheduler` para ejecuciones semanales automáticas.

---

## 🛠️ Arquitectura del Proyecto

```bash
AgroH/
├── clean/          # Lógica de limpieza y normalización de datos
├── config/         # Configuraciones centrales y gestión de variables de entorno
├── data/           # Almacenamiento local de archivos (Raw/Processed)
├── extract/        # Scripts de extracción (APIs, Web Scraping, Socrata)
├── load/           # Módulos de carga a BD (SQLAlchemy/PostgreSQL)
├── logs/           # Registros de ejecución del pipeline
├── models/         # Entrenamiento y persistencia de modelos de Machine Learning
├── utils/          # Funciones de apoyo (geometría, procesamiento de texto)
├── validate/       # Lógica de reportes de calidad y auditoría
├── run_pipeline.py # Orquestador principal del sistema
└── requirements.txt# Dependencias del proyecto
```

---

## 💻 Instalación y Configuración

### 1. Clonar y Preparar el Entorno
```powershell
# Clonar el repositorio
git clone <url-del-repo>
cd AgroH

# Crear entorno virtual
python -m venv venv
.\venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

### 2. Variables de Entorno
Crea un archivo `.env` en la raíz del proyecto basándote en la siguiente estructura:
```env
# Database (Supabase / PostgreSQL)
SUPABASE_DB_HOST=tu_host
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASS=tu_password

# API Tokens
SOCRATA_TOKEN=tu_token_opcional

# Configuración Pipeline
PIPELINE_YEAR_END=2026
CLIMA_YEAR_START=2010
```

---

## 📖 Modo de Uso

El sistema se opera a través de `run_pipeline.py`.

*   **Ejecución Única (All-in-one)**:
    ```powershell
    python run_pipeline.py --mode all --once
    ```
*   **Ejecutar solo Modelos IA**:
    ```powershell
    python run_pipeline.py --mode models --once
    ```
*   **Activar Modo Scheduler (Servidor)**:
    ```powershell
    python run_pipeline.py --mode all
    ```
    *Esto programará el pipeline para ejecutarse todos los lunes a las 02:00 AM.*

---

## 📡 Fuentes de Datos
*   **Datos Abiertos Colombia**: DIVIPOLA, Producción EVA, Catálogo IDEAM.
*   **IDEAM**: Series climatológicas mensuales.
*   **DANE**: Precios SIPSA y Microdatos CNA.
*   **UPRA**: Aptitud de suelos SIPRA.
*   **NOAA**: Oceanic Niño Index (ONI).

---

## 👥 Contribuciones
Desarrollado para el fortalecimiento tecnológico del sector agrícola colombiano. 
© 2026 AgroIA Team.
