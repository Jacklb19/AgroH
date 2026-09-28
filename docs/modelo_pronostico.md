# Modelo de pronóstico de rendimiento

Script: `models/train_pronostico.py` · Tablas: `model_version` (nombre `xgboost_pronostico`) y `pred_pronostico`.

## Qué predice

El rendimiento (toneladas por hectárea cosechada) de un cultivo en un municipio para cada uno de los años siguientes al último dato de producción, con un rango probable del 90 % y tres escenarios de El Niño / La Niña.

## Datos

- **Objetivo:** producción ÷ área cosechada de `fact_produccion_agricola` (2019–2024). No se usa la columna `rendimiento_t_ha`, que estaba inflada por sumar semestres (corregido en `load/load_facts.py`).
- **Limpieza:**
  - rendimientos a más de 3,5 desviaciones robustas (MAD, en escala logarítmica) del resto del cultivo y valores mayores a 500 t/ha;
  - errores de reporte de área: años en que el área cosechada cae por debajo del 40 % del máximo de esa combinación y el rendimiento supera 2,5 veces el de sus años normales (p. ej. arroz en Puerto Concordia 2022-2024: 37 t/ha).
- **Rankings en la web:** los listados de "mayor rendimiento" excluyen pronósticos por encima del percentil 95 de lo registrado para el cultivo.

## Variables (solo información conocida antes de la cosecha)

Rendimiento del año anterior y de hace dos años, promedio y desviación histórica, años con datos, tendencia, área cosechada del año anterior, rendimiento típico del cultivo en el país y el departamento, cultivo, departamento, región, coordenadas, ciclo (permanente/transitorio), aptitud del suelo (UPRA), climatología del municipio e índice ONI del año (escenario) y del año anterior.

## Formulación

- Punto de partida: rendimiento del año anterior (o el promedio histórico si falta).
- XGBoost aprende el **cambio logarítmico** frente a ese punto de partida con pérdida absoluta (mediana).
- El cambio predicho se atenúa con `ALPHA = 0.5`, lo que reduce los errores grandes sin empeorar el error típico.
- Pronóstico recursivo: el pronóstico neutral de un año alimenta los rezagos del siguiente.
- Escenarios: ONI = 0 (Neutral), +1 (El Niño), −1 (La Niña). Los años ya cerrados usan el ONI real.

## Validación

Backtest de origen móvil en 2022, 2023 y 2024. Referencias: repetir el año anterior y promedio histórico. El rango del 90 % se calcula por cultivo con los cuantiles 5 % y 95 % del error logarítmico del backtest (global si el cultivo tiene menos de 40 casos).

Se compararon cuatro configuraciones de hiperparámetros y tres funciones de pérdida (absoluta, Huber, cuadrática); la pérdida absoluta sobre el cambio fue la única que igualó a "repetir el año anterior" en error medio.

## Reentrenar

```bash
python -m models.train_pronostico            # entrena y evalúa, no escribe
python -m models.train_pronostico --write    # registra versión nueva y guarda pronósticos
```

Cada ejecución con `--write` agrega una versión nueva y desactiva la anterior; no borra datos.
