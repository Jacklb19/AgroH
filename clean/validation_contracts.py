import pandera.pandas as pa
import datetime

# Constantes de negocio
YEAR_MIN = 2007
YEAR_MAX = datetime.datetime.now().year

produccion_schema = pa.DataFrameSchema(
    {
        "id_municipio": pa.Column(
            str, 
            checks=[
                pa.Check(lambda s: s.str.len() == 5, error="El id_municipio debe tener exactamente 5 caracteres"),
                pa.Check(lambda s: s.str.isdigit(), error="El id_municipio debe ser numérico")
            ],
            nullable=False
        ),
        "anio": pa.Column(
            int, 
            pa.Check.in_range(YEAR_MIN, YEAR_MAX, error=f"El año debe estar entre {YEAR_MIN} y {YEAR_MAX}"),
            nullable=False
        ),
        "area_sembrada_ha": pa.Column(
            float, 
            pa.Check.ge(0, error="El área sembrada debe ser >= 0"),
            nullable=False # Después del parseo y si es lógicamente requerido
        ),
        "area_cosechada_ha": pa.Column(
            float, 
            pa.Check.ge(0, error="El área cosechada debe ser >= 0"),
            # validación cruzada a nivel de DF: area_cosechada <= area_sembrada
            nullable=False
        ),
        "produccion_total_ton": pa.Column(
            float, 
            pa.Check.ge(0, error="La producción debe ser >= 0"),
            nullable=False
        ),
        "rendimiento_t_ha": pa.Column(
            float, 
            pa.Check.ge(0, error="El rendimiento debe ser >= 0"),
            nullable=True # Puede ser nulo si área es cero
        ),
        "cultivo_normalizado": pa.Column(
            str, 
            pa.Check.str_length(min_value=2, error="El nombre del cultivo es inválido"),
            nullable=False
        )
    },
    # Reglas a nivel de dataframe (Business Rules cruzadas)
    checks=[
        pa.Check(
            lambda df: df["area_cosechada_ha"] <= df["area_sembrada_ha"],
            error="El área cosechada no puede ser mayor al área sembrada",
            name="business_rule_cosechada_vs_sembrada",
            ignore_na=True
        )
    ]
)
