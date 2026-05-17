import pandera.pandas as pa

# Valores categóricos esperados en BD
APTITUD_CATEGORIAS = ["alta", "moderada", "marginal", "no_apta"]

sipra_schema = pa.DataFrameSchema(
    {
        "id_municipio": pa.Column(
            str,
            checks=[
                pa.Check(lambda s: s.str.len() == 5, error="El id_municipio debe tener exactamente 5 caracteres"),
                pa.Check(lambda s: s.str.isdigit(), error="El id_municipio debe ser numérico")
            ],
            nullable=False
        ),
        "clase_aptitud": pa.Column(
            str,
            checks=[
                pa.Check.isin(APTITUD_CATEGORIAS, error=f"Aptitud debe ser uno de: {APTITUD_CATEGORIAS}")
            ],
            nullable=False
        ),
        "cultivo_origen": pa.Column(
            str,
            checks=[
                pa.Check.str_length(min_value=2, error="Cultivo origen inválido")
            ],
            nullable=False
        )
    },
    checks=[]
)
