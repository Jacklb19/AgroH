# Flujo de Calidad de Datos (Data Quality Workflow)

El ciclo de vida del dato en AgroH se divide en etapas claras para transformar datos crudos en información confiable y estructurada, garantizando que los modelos de inteligencia artificial y los dashboards se alimenten de información validada.

## Arquitectura de Transformación: Raw -> Clean -> Fact

El procesamiento sigue un enfoque de validación explícita. No se asumen correcciones automáticas destructivas (como `.fillna(0)`); en su lugar, se tipa, se normaliza y se audita contra un contrato de datos.

### 1. Extracción (Raw)
- Los datos se obtienen de las fuentes (Socrata, DANE, NOAA) y se almacenan en su formato nativo (`str` en la mayoría de APIs) en el directorio `data/raw/`.
- No hay transformación en este paso, garantizando trazabilidad completa.

### 2. Parseo y Normalización Técnica
- **Parseo:** Conversión explícita de tipos técnicos (`pd.to_numeric`, `pd.to_datetime`). Los errores de casting no bloquean el flujo, sino que se marcan como nulos técnicos explícitos (`NaN`, `NaT`).
- **Normalización de Llaves:** Estandarización de identificadores primarios, por ejemplo, aplicar padding (`.zfill(5)`) a los códigos DIVIPOLA o unificar minúsculas/sin tildes en nombres geográficos para cruces posteriores.

### 3. Validación Semántica (Data Contracts)
- Se utiliza **Pandera** (`clean/validation_contracts.py`) para establecer el esquema que los datos *deben* cumplir.
- Las validaciones incluyen reglas lógicas de negocio (ej. `area_cosechada <= area_sembrada`) y chequeo de nulos permitidos.
- Se ejecuta la validación con `lazy=True` para capturar **todos** los errores a la vez (por registro y por columna) sin fallar silenciosamente.

### 4. Separación Valid / Invalid
- Al validar, el flujo se bifurca:
  - **`df_valid`**: Registros que cumplen el 100% de las reglas semánticas y el contrato de esquema. Pasan al paso de carga.
  - **`df_invalid`**: Registros con anomalías (físicamente imposibles, llaves malas, fechas ilógicas). Se exportan a `data/processed/invalid/` para auditoría y retroalimentación, en lugar de ser descartados invisiblemente.

### 5. Carga a Base de Datos (Fact)
- Los datos validados se insertan en el motor PostgreSQL.
- Se implementa una red de seguridad final con restricciones de base de datos (`load/data_quality_constraints.sql`) usando `CHECK`, `NOT NULL` y llaves foráneas. Estas protegen contra la inyección de datos fuera de flujo o fallos sistémicos.

## Ejecución en Producción y Auditoría

La orquestación del flujo de calidad de datos se gestiona a través del script central en la raíz del proyecto. El pipeline ejecuta la extracción, limpieza, validación por contratos y carga en base de datos.

### Comando Recomendado
Para ejecutar el pipeline end-to-end con auditoría semántica y registro en `data_quality_log`, ejecuta:
```bash
python run_pipeline.py --mode all --once --use-clean-modules
```

### Ejecución Manual de Módulos
Si deseas validar o probar un módulo de calidad de forma aislada, puedes ejecutarlo directamente desde la raíz del proyecto:
```bash
python -m clean.clean_produccion
python -m clean.clean_clima
python -m clean.clean_divipola
```

Para una explicación exhaustiva de la arquitectura del almacén de datos (Star Schema) y las reglas específicas de los contratos semánticos, consulta el [README principal](../README.md).
