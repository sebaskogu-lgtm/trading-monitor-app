# ==============================================================================
# REQUERIMIENTOS E INSTALACIÓN AUTOMÁTICA
# ==============================================================================
import os
import subprocess
import sys

requirements_content = """fastapi
uvicorn
yfinance
pydantic
pandas
pytz
psycopg2-binary
"""

if not os.path.exists("requirements.txt"):
  with open("requirements.txt", "w") as f:
    f.write(requirements_content)

try:
  import fastapi
  import pandas
  import pydantic
  import uvicorn
  import yfinance
  import pytz
  import psycopg2
except ImportError:
  subprocess.check_call(
      [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"]
  )

# ==============================================================================
# PERSISTENCIA EN SUPABASE (POSTGRESQL DIRECTO)
# ==============================================================================
import json
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "")

def init_db():
    if not DATABASE_URL:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS configuracion (
                id INT PRIMARY KEY,
                activos TEXT,
                cartera TEXT
            );
        """)
        cursor.execute("SELECT COUNT(*) FROM configuracion WHERE id=1;")
        if cursor.fetchone()[0] == 0:
            cursor.execute(
                "INSERT INTO configuracion (id, activos, cartera) VALUES (1, %s, %s);",
                (json.dumps(["QQQ", "SPY", "NVDA", "AAPL"]), json.dumps([]))
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print("Error inicializando DB en Supabase:", e)

init_db()

def db_get(campo):
    if not DATABASE_URL:
        return ["QQQ", "SPY", "NVDA", "AAPL"] if campo == "activos" else []
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute(f"SELECT {campo} FROM configuracion WHERE id=1;")
        row = cursor.fetchone()
        conn.close()
        return json.loads(row[0]) if row and row[0] else []
    except Exception as e:
        print(f"Error leyendo DB ({campo}):", e)
        return ["QQQ", "SPY", "NVDA", "AAPL"] if campo == "activos" else []

def db_set(campo, valor):
    if not DATABASE_URL:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute(f"UPDATE configuracion SET {campo} = %s WHERE id=1;", (json.dumps(valor),))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error guardando DB ({campo}):", e)
