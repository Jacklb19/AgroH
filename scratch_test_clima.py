import sys
import os
sys.path.append(os.path.abspath('.'))

import logging
logging.basicConfig(level=logging.INFO)

from extract.extract_ideam_clima import _download_month_fast
from config.settings import SOURCES

url = SOURCES["precipitacion_ideam"]
print("Test downloading January 2024...")
df = _download_month_fast(url, "sum", 2024, 1, include_sensor=False)

if not df.empty:
    print(f"Success! Downloaded {len(df)} stations for Jan 2024.")
    print(df.head())
else:
    print("Failed to download or empty.")
