import logging
logging.basicConfig(level=logging.INFO)
import pandas as pd
from extract.extract_sipra import extract_sipra
from clean.clean_suelo import resumir_aptitud_suelo_por_municipio
from extract.extract_divipola import extract_divipola

df_divipola = extract_divipola()
df_sipra = extract_sipra()
df_suelo = resumir_aptitud_suelo_por_municipio(df_sipra, df_divipola)

print(df_suelo.head())
print("Columns:", df_suelo.columns.tolist())
