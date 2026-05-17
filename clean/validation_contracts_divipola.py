import pandera.pandas as pa

divipola_schema = pa.DataFrameSchema(
    {
        "cod_dpto": pa.Column(
            str,
            checks=[
                pa.Check(lambda s: s.str.len() == 2, error="El cod_dpto debe tener 2 caracteres"),
                pa.Check(lambda s: s.str.isdigit(), error="El cod_dpto debe ser numérico")
            ],
            nullable=False
        ),
        "dpto": pa.Column(
            str,
            checks=[
                pa.Check.str_length(min_value=2, error="Nombre de departamento inválido")
            ],
            nullable=False
        ),
        "cod_mpio": pa.Column(
            str,
            checks=[
                pa.Check(lambda s: s.str.len() == 5, error="El cod_mpio debe tener 5 caracteres"),
                pa.Check(lambda s: s.str.isdigit(), error="El cod_mpio debe ser numérico")
            ],
            nullable=False
        ),
        "nom_mpio": pa.Column(
            str,
            checks=[
                pa.Check.str_length(min_value=2, error="Nombre de municipio inválido")
            ],
            nullable=False
        ),
        "tipo_municipio": pa.Column(
            str,
            nullable=True
        ),
        "latitud": pa.Column(
            float,
            checks=[
                pa.Check.in_range(-5.0, 15.0, error="Latitud fuera de los límites de Colombia")
            ],
            nullable=True
        ),
        "longitud": pa.Column(
            float,
            checks=[
                pa.Check.in_range(-85.0, -66.0, error="Longitud fuera de los límites de Colombia")
            ],
            nullable=True
        )
    },
    checks=[]
)
