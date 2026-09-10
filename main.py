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
# CÓDIGO PRINCIPAL
# ==============================================================================
import asyncio
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
import pandas as pd
import psycopg2
import pytz
import yfinance as yf

app = FastAPI(title="Trading Monitor Pro")

INTERVALO_SEGUNDOS = 60
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()


def get_db_connection():
  if not DATABASE_URL:
    return None
  try:
    url = (
        DATABASE_URL + "?sslmode=require"
        if "?" not in DATABASE_URL
        else DATABASE_URL
    )
    return psycopg2.connect(url)
  except Exception as e:
    print("❌ ERROR CRÍTICO DE CONEXIÓN A SUPABASE:", e)
    return None


def init_db():
  conn = get_db_connection()
  if not conn:
    print("⚠️ ADVERTENCIA: No se pudo conectar a la base de datos.")
    return
  try:
    cursor = conn.cursor()
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS configuracion (
                id INT PRIMARY KEY,
                mercado_actual TEXT,
                datos TEXT
            );
        """)
    cursor.execute("SELECT COUNT(*) FROM configuracion WHERE id=1;")
    if cursor.fetchone()[0] == 0:
      default_datos = {
          "NY": {
              "activos": [
                  "SPY",
                  "QQQ",
                  "NVDA",
                  "AAPL",
                  "MSFT",
                  "AMZN",
                  "GOOGL",
                  "META",
                  "TSLA",
                  "BTC-USD",
              ],
              "cartera": [],
          },
          "LONDRES": {
              "activos": [
                  "SHEL.L",
                  "AZN.L",
                  "ULVR.L",
                  "HSBA.L",
                  "BP.L",
                  "GSK.L",
                  "RIO.L",
                  "BARC.L",
                  "LLOY.L",
                  "VOD.L",
              ],
              "cartera": [],
          },
          "ASIA": {
              "activos": [
                  "7203.T",
                  "6758.T",
                  "7974.T",
                  "9984.T",
                  "8306.T",
                  "6861.T",
                  "6501.T",
                  "4063.T",
                  "6902.T",
                  "8035.T",
              ],
              "cartera": [],
          },
      }
      cursor.execute(
          "INSERT INTO configuracion (id, mercado_actual, datos) VALUES (1,"
          " %s, %s);",
          ("NY", json.dumps(default_datos)),
      )
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Base de datos conectada e inicializada correctamente.")
  except Exception as e:
    print("❌ Error ejecutando init_db:", e)


init_db()


def db_get_all_data():
  default_market = "NY"
  default_datos = {
      "NY": {
          "activos": [
              "SPY",
              "QQQ",
              "NVDA",
              "AAPL",
              "MSFT",
              "AMZN",
              "GOOGL",
              "META",
              "TSLA",
              "BTC-USD",
          ],
          "cartera": [],
      },
      "LONDRES": {
          "activos": [
              "SHEL.L",
              "AZN.L",
              "ULVR.L",
              "HSBA.L",
              "BP.L",
              "GSK.L",
              "RIO.L",
              "BARC.L",
              "LLOY.L",
              "VOD.L",
          ],
          "cartera": [],
      },
      "ASIA": {
          "activos": [
              "7203.T",
              "6758.T",
              "7974.T",
              "9984.T",
              "8306.T",
              "6861.T",
              "6501.T",
              "4063.T",
              "6902.T",
              "8035.T",
          ],
          "cartera": [],
      },
  }
  conn = get_db_connection()
  if not conn:
    return default_market, default_datos
  try:
    cursor = conn.cursor()
    cursor.execute("SELECT mercado_actual, datos FROM configuracion WHERE id=1;")
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row:
      m_act = row[0] or "NY"
      d_json = json.loads(row[1]) if row[1] else default_datos
      return m_act, d_json
    return default_market, default_datos
  except Exception as e:
    print("❌ Error leyendo DB:", e)
    return default_market, default_datos


def db_save_all_data(mercado_act, datos):
  conn = get_db_connection()
  if not conn:
    return
  try:
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE configuracion SET mercado_actual = %s, datos = %s WHERE id=1;",
        (mercado_act, json.dumps(datos)),
    )
    conn.commit()
    cursor.close()
    conn.close()
  except Exception as e:
    print("❌ Error guardando DB:", e)


CATALOGO_TICKERS = {
    # Wall Street (NY)
    "AAPL": {
        "nombre": "Apple Inc.",
        "desc": "Gigante tecnológico de consumo, software y servicios digitales.",
        "estrategia": "Ruptura de máximos con soporte en medias móviles (SMA 9/21).",
    },
    "MSFT": {
        "nombre": "Microsoft Corporation",
        "desc": "Liderazgo en cloud computing (Azure) y software empresarial.",
        "estrategia": "Seguimiento de tendencia alcista de largo plazo en gráfico diario.",
    },
    "AMZN": {
        "nombre": "Amazon.com Inc.",
        "desc": "E-commerce global y servicios de infraestructura web.",
        "estrategia": "Operativa de rangos intradiarios y rebotes en soportes Fibonacci.",
    },
    "NVDA": {
        "nombre": "NVIDIA Corporation",
        "desc": "Pionero absoluto en procesamiento gráfico y semiconductores IA.",
        "estrategia": "Alta volatilidad; exige confirmación estricta de volumen en rupturas.",
    },
    "GOOGL": {
        "nombre": "Alphabet Inc.",
        "desc": "Ecosistema publicitario global, búsquedas y soluciones de IA.",
        "estrategia": "Ideal para operativa institucional en swing trading (4H / 1D).",
    },
    "META": {
        "nombre": "Meta Platforms Inc.",
        "desc": "Redes sociales masivas e inversión en infraestructura tecnológica.",
        "estrategia": "Monitoreo de divergencias en RSI y quiebres de resistencia clave.",
    },
    "TSLA": {
        "nombre": "Tesla Inc.",
        "desc": "Vehículos eléctricos, almacenamiento energético y conducción autónoma.",
        "estrategia": "Atención a zonas de sobreventa extrema (RSI <= 30) para rebotes rápidos.",
    },
    "NFLX": {
        "nombre": "Netflix Inc.",
        "desc": "Plataforma líder global de streaming y entretenimiento en la nube.",
        "estrategia": "Busca impulsos de momentum alcista tras publicación de métricas.",
    },
    "AMD": {
        "nombre": "Advanced Micro Devices",
        "desc": "Procesadores de alta computación y tarjetas gráficas competitivas.",
        "estrategia": "Correlación directa con flujos de volumen institucional.",
    },
    "COIN": {
        "nombre": "Coinbase Global",
        "desc": "Plataforma centralizada líder para intercambio de criptoactivos.",
        "estrategia": "Alta correlación con el comportamiento macro de Bitcoin.",
    },
    "MSTR": {
        "nombre": "MicroStrategy Inc.",
        "desc": "Empresa de software corporativo con tesorería masiva en Bitcoin.",
        "estrategia": "Movimientos apalancados especulativos de alta volatilidad.",
    },
    "PLTR": {
        "nombre": "Palantir Technologies",
        "desc": "Software de análisis de macro datos para defensa e industrias.",
        "estrategia": "Patrones de quiebre de resistencia con fuerte inyección de volumen.",
    },
    "SPY": {
        "nombre": "S&P 500 ETF",
        "desc": "Fondo indexado que replica las 500 empresas más grandes de EE.UU.",
        "estrategia": "El termómetro principal del mercado; excelente para filtrar la tendencia base.",
    },
    "QQQ": {
        "nombre": "Nasdaq 100 ETF",
        "desc": "Fondo enfocado en las 100 mayores empresas tecnológicas del índice.",
        "estrategia": "Operativa intradiaria ágil apalancada en cruces de medias móviles (SMA 9/21).",
    },
    "BTC-USD": {
        "nombre": "Bitcoin USD",
        "desc": "Activo digital descentralizado de referencia global.",
        "estrategia": "Monitoreo 24/7 de niveles psicológicos y retrocesos de Fibonacci.",
    },
    "ETH-USD": {
        "nombre": "Ethereum USD",
        "desc": "Red principal para contratos inteligentes y finanzas descentralizadas.",
        "estrategia": "Estructuras de consolidación seguidas de rupturas con alto volumen.",
    },
    # Londres (LSE)
    "SHEL.L": {
        "nombre": "Shell plc",
        "desc": "Energía global, petróleo y gas natural.",
        "estrategia": "Seguimiento de precios del petróleo y dividendos estables.",
    },
    "AZN.L": {
        "nombre": "AstraZeneca PLC",
        "desc": "Gigante biofarmacéutico y científico global.",
        "estrategia": "Inversión defensiva en salud con rupturas de resistencia técnica.",
    },
    "ULVR.L": {
        "nombre": "Unilever PLC",
        "desc": "Bienes de consumo masivo y productos de hogar.",
        "estrategia": "Operativa de rango en mercados defensivos.",
    },
    "HSBA.L": {
        "nombre": "HSBC Holdings plc",
        "desc": "Banca internacional con fuerte presencia en Asia y Europa.",
        "estrategia": "Sensibilidad a tipos de interés globales y flujos bancarios.",
    },
    "BP.L": {
        "nombre": "BP p.l.c.",
        "desc": "Producción energética e infraestructura petrolera.",
        "estrategia": "Rebotes en soportes clave y correlación con crudo Brent.",
    },
    "GSK.L": {
        "nombre": "GSK plc",
        "desc": "Desarrollo farmacéutico, vacunas y salud global.",
        "estrategia": "Comportamiento defensivo ante alta volatilidad de mercado.",
    },
    "RIO.L": {
        "nombre": "Rio Tinto plc",
        "desc": "Minería global y extracción de metales industriales (hierro, cobre).",
        "estrategia": "Correlación directa con la demanda de materias primas en China.",
    },
    "BARC.L": {
        "nombre": "Barclays PLC",
        "desc": "Banca de inversión y servicios financieros en Reino Unido.",
        "estrategia": "Alta volatilidad intradiaria en aperturas europeas.",
    },
    "LLOY.L": {
        "nombre": "Lloyds Banking Group",
        "desc": "Líder en banca minorista e hipotecaria británica.",
        "estrategia": "Seguimiento de tendencia macroeconómica del Reino Unido.",
    },
    "VOD.L": {
        "nombre": "Vodafone Group Plc",
        "desc": "Telecomunicaciones móviles y redes de fibra óptica.",
        "estrategia": "Operativa de dividendos y estabilidad en rangos estrechos.",
    },
    # Asia (Tokio)
    "7203.T": {
        "nombre": "Toyota Motor Corp",
        "desc": "Liderazgo mundial en fabricación y tecnología automotriz.",
        "estrategia": "Seguimiento de tipo de cambio USD/JPY y flujos institucionales.",
    },
    "6758.T": {
        "nombre": "Sony Group Corp",
        "desc": "Electrónica de consumo, entretenimiento, cine y videojuegos.",
        "estrategia": "Quiebres de resistencia impulsados por lanzamientos de productos.",
    },
    "7974.T": {
        "nombre": "Nintendo Co Ltd",
        "desc": "Desarrollo de consolas y franquicias icónicas de videojuegos.",
        "estrategia": "Alta volatilidad ligada al ciclo de lanzamientos y temporada.",
    },
    "9984.T": {
        "nombre": "SoftBank Group Corp",
        "desc": "Inversión en tecnología global, IA y fondos de riesgo (Vision Fund).",
        "estrategia": "Correlación con el sector tecnológico global y valoración de startups.",
    },
    "8306.T": {
        "nombre": "Mitsubishi UFJ Financial",
        "desc": "Gigante bancario y financiero japonés.",
        "estrategia": "Sensibilidad a la política monetaria del Banco de Japón (BOJ).",
    },
    "6861.T": {
        "nombre": "Keyence Corp",
        "desc": "Automatización industrial, sensores y sistemas de medición de alta precisión.",
        "estrategia": "Crecimiento estructural ligado a la manufactura avanzada global.",
    },
    "6501.T": {
        "nombre": "Hitachi Ltd",
        "desc": "Infraestructura digital, energía, sistemas ferroviarios y tecnología industrial.",
        "estrategia": "Contratos gubernamentales e industriales de largo plazo.",
    },
    "4063.T": {
        "nombre": "Shin-Etsu Chemical",
        "desc": "Líder mundial en obleas de silicio para semiconductores y materiales de PVC.",
        "estrategia": "Correlación estrecha con el ciclo global de chips y semiconductores.",
    },
    "6902.T": {
        "nombre": "Denso Corp",
        "desc": "Componentes y sistemas avanzados para la industria automotriz.",
        "estrategia": "Innovación en vehículos eléctricos y conducción autónoma.",
    },
    "8035.T": {
        "nombre": "Tokyo Electron Ltd",
        "desc": "Fabricación de equipos avanzados para producción de semiconductores.",
        "estrategia": "Alta sensibilidad al ciclo global de chips y demanda de IA.",
    },
}

POOLS_ESCANER = {
    "NY": [
        "AAPL",
        "MSFT",
        "AMZN",
        "NVDA",
        "GOOGL",
        "META",
        "TSLA",
        "NFLX",
        "AMD",
        "COIN",
        "MSTR",
        "PLTR",
        "SPY",
        "QQQ",
        "INTC",
        "BA",
        "JPM",
        "DIS",
        "XOM",
        "BABA",
        "BTC-USD",
        "ETH-USD",
        "ARM",
        "SMCI",
        "MU",
        "QCOM",
        "AVGO",
        "MARA",
        "RIOT",
    ],
    "LONDRES": [
        "SHEL.L",
        "AZN.L",
        "ULVR.L",
        "HSBA.L",
        "BP.L",
        "GSK.L",
        "RIO.L",
        "BARC.L",
        "LLOY.L",
        "VOD.L",
        "RR.L",
        "AAL.L",
        "NG.L",
        "VRSK.L",
        "LSEG.L",
    ],
    "ASIA": [
        "7203.T",
        "6758.T",
        "7974.T",
        "9984.T",
        "8306.T",
        "6861.T",
        "6501.T",
        "4063.T",
        "6902.T",
        "8035.T",
        "6752.T",
        "7267.T",
        "4307.T",
        "6301.T",
        "9432.T",
    ],
}

timeframe_actual = "1h"
mercado_actual, db_datos = db_get_all_data()
estado_mercado = {}
historial_alertas = []
recomendaciones_escaner = []
cache_yf = {}
SSE_SUBSCRIBERS = []


class TimeframeModel(BaseModel):
  timeframe: str


class MercadoModel(BaseModel):
  mercado: str


class PosicionModel(BaseModel):
  ticker: str
  precio_compra: float
  sl_usuario: float
  tp_usuario: float
  riesgo_usd: float
  timeframe: str


class ReordenarModel(BaseModel):
  activos: list


def obtener_info_horario():
  global mercado_actual
  if mercado_actual == "LONDRES":
    tz = pytz.timezone("Europe/London")
    nombre_mercado = "Londres (LSE)"
    open_h, open_m = 8, 0
    close_h, close_m = 16, 30
    has_lunch = False
  elif mercado_actual == "ASIA":
    tz = pytz.timezone("Asia/Tokyo")
    nombre_mercado = "Asia (Tokio)"
    open_h, open_m = 9, 0
    close_h, close_m = 15, 30
    has_lunch = True
  else:
    tz = pytz.timezone("America/New_York")
    nombre_mercado = "Nueva York (NYSE)"
    open_h, open_m = 9, 30
    close_h, close_m = 16, 0
    has_lunch = False

  t = datetime.now(tz)
  hora_mercado_str = t.strftime("%H:%M:%S")

  if t.weekday() > 4:
    dias_hasta_lunes = (7 - t.weekday()) % 7
    if dias_hasta_lunes == 0:
      dias_hasta_lunes = 2
    proximo_lunes = t + timedelta(days=dias_hasta_lunes)
    proximo_lunes = proximo_lunes.replace(
        hour=open_h, minute=open_m, second=0, microsecond=0
    )
    diff = proximo_lunes - t
    horas, rem = divmod(int(diff.total_seconds()), 3600)
    minutos, segundos = divmod(rem, 60)
    return (
        f"🔴 {nombre_mercado} CERRADO (Fin de semana) | Hora Local:"
        f" {hora_mercado_str}",
        f"Abre en {horas // 24}d {horas % 24}h {minutos}m {segundos}s",
    )

  m_open = t.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
  m_close = t.replace(hour=close_h, minute=close_m, second=0, microsecond=0)

  if has_lunch:
    lunch_start = t.replace(hour=11, minute=30, second=0, microsecond=0)
    lunch_end = t.replace(hour=12, minute=30, second=0, microsecond=0)
  else:
    lunch_start = lunch_end = None

  if t < m_open:
    diff = m_open - t
    seg = int(diff.total_seconds())
    h, r = divmod(seg, 3600)
    m, s = divmod(r, 60)
    return (
        f"🔴 {nombre_mercado} CERRADO (Pre-apertura) | Hora Local:"
        f" {hora_mercado_str}",
        f"Abre en {h}h {m}m {s}s",
    )
  elif t > m_close:
    proxima = m_open + timedelta(days=1)
    diff = proxima - t
    seg = int(diff.total_seconds())
    h, s = divmod(seg, 60)
    return (
        f"🔴 {nombre_mercado} CERRADO | Hora Local: {hora_mercado_str}",
        f"Abre mañana en {h}h {s}m",
    )
  elif has_lunch and lunch_start <= t < lunch_end:
    diff = lunch_end - t
    seg = int(diff.total_seconds())
    m, s = divmod(seg, 60)
    return (
        f"☕ {nombre_mercado} RECESO (Almuerzo) | Hora Local:"
        f" {hora_mercado_str}",
        f"Vuelve en {m}m {s}s",
    )
  else:
    diff = m_close - t
    seg = int(diff.total_seconds())
    h, r = divmod(seg, 3600)
    m, s = divmod(r, 60)
    return (
        f"🟢 {nombre_mercado} ABIERTO | Hora Local: {hora_mercado_str}",
        f"Cierra en {h}h {m}m {s}s",
    )


def obtener_config_tf(tf: str):
  if tf == "4h":
    return "60d", "60m"
  elif tf == "1d":
    return "6mo", "1d"
  return "1mo", "1h"


def procesar_ticker(symbol, tf_local):
  ahora = time.time()
  if (
      symbol in cache_yf
      and cache_yf[symbol]["tf"] == tf_local
      and (ahora - cache_yf[symbol]["time"] < 35)
  ):
    return cache_yf[symbol]["data"]

  periodo, intervalo = obtener_config_tf(tf_local)
  try:
    tendencia_macro = "ALZA"
    if tf_local in ["1h", "4h"]:
      try:
        df_macro = yf.download(
            tickers=symbol, period="6mo", interval="1d", progress=False
        )
        if not df_macro.empty:
          if hasattr(df_macro.columns, "nlevels") and df_macro.columns.nlevels > 1:
            df_macro.columns = df_macro.columns.get_level_values(0)
          df_macro["SMA_9"] = df_macro["Close"].rolling(9).mean()
          df_macro["SMA_21"] = df_macro["Close"].rolling(21).mean()
          ultima_macro = df_macro.iloc[-1]
          if not pd.isna(ultima_macro["SMA_9"]) and not pd.isna(
              ultima_macro["SMA_21"]
          ):
            tendencia_macro = (
                "ALZA" if ultima_macro["SMA_9"] > ultima_macro["SMA_21"] else "BAJA"
            )
      except:
        pass

    df = yf.download(
        tickers=symbol, period=periodo, interval=intervalo, progress=False
    )
    if df.empty:
      return None
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
      df.columns = df.columns.get_level_values(0)

    if tf_local == "4h" and len(df) >= 4:
      df = (
          df.resample("4h")
          .agg({
              "Open": "first",
              "High": "max",
              "Low": "min",
              "Close": "last",
              "Volume": "sum",
          })
          .dropna()
      )

    if len(df) >= 20:
      df["SMA_9"] = df["Close"].rolling(window=9).mean()
      df["SMA_21"] = df["Close"].rolling(window=21).mean()
      df["Resistencia"] = df["High"].rolling(window=20).max().shift(1)
      df["Soporte_SL"] = df["Low"].rolling(window=10).min().shift(1)
      df["ATR"] = df["High"] - df["Low"]
      atr_medio = round(float(df["ATR"].rolling(14).mean().iloc[-1]), 2)

      delta = df["Close"].diff()
      gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
      loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
      rs = gain / loss
      df["RSI"] = 100 - (100 / (1 + rs))
      rsi_val = (
          round(float(df["RSI"].iloc[-1]), 1)
          if not pd.isna(df["RSI"].iloc[-1])
          else 50.0
      )

      vol_valido = True
      if "Volume" in df.columns:
        df["Vol_SMA_20"] = df["Volume"].rolling(window=20).mean()
        vol_actual = float(df["Volume"].iloc[-1])
        vol_promedio = float(df["Vol_SMA_20"].iloc[-1])
        if vol_promedio > 0:
          vol_valido = vol_actual >= (1.5 * vol_promedio)

      ultima = df.iloc[-1]
      anterior = df.iloc[-2]

      precio = round(float(ultima["Close"]), 2)
      resistencia = round(float(anterior["Resistencia"]), 2)
      soporte_tecnico = round(float(anterior["Soporte_SL"]), 2)
      sma9 = round(float(ultima["SMA_9"]), 2)
      sma21 = round(float(ultima["SMA_21"]), 2)

      tendencia = "ALZA" if sma9 > sma21 else "BAJA"
      hora = datetime.now().strftime("%H:%M:%S")

      max_tramo = float(df["High"].tail(20).max())
      min_tramo = float(df["Low"].tail(20).min())
      dif_tramo = max_tramo - min_tramo

      fib_500 = round(max_tramo - (dif_tramo * 0.500), 2)
      fib_618 = round(max_tramo - (dif_tramo * 0.618), 2)

      en_zona_fib = (fib_618 * 0.99) <= precio <= (fib_500 * 1.01)

      riesgo = precio - soporte_tecnico
      tp_tecnico = (
          round(precio + (riesgo * 2), 2)
          if riesgo > 0
          else round(precio * 1.02, 2)
      )

      distancia_resistencia = ((resistencia - precio) / precio) * 100
      riesgo_macro_str = " | ⚠️ Macro BAJISTA" if tendencia_macro == "BAJA" else ""

      if precio > resistencia and tendencia == "ALZA":
        if rsi_val >= 70.0:
          estado_entrada = f"⚠️ SOBRECOMPRADO (Riesgo | RSI {rsi_val})"
        elif not vol_valido:
          estado_entrada = f"⚠️ FALSO QUIEBRE (Bajo Vol | RSI {rsi_val})"
        elif en_zona_fib:
          estado_entrada = f"🟢 BUENA ENTRADA (Quiebre+Fib){riesgo_macro_str}"
        else:
          estado_entrada = f"🟢 BUENA ENTRADA (Quiebre){riesgo_macro_str}"
      elif 0 < distancia_resistencia <= 1.2 and tendencia == "ALZA":
        if en_zona_fib:
          estado_entrada = (
              f"⏳ PREPARANDO (Apoyo Fib | RSI {rsi_val}){riesgo_macro_str}"
          )
        else:
          estado_entrada = f"⏳ PREPARANDO RUPTURA (RSI {rsi_val}){riesgo_macro_str}"
      elif rsi_val <= 30.0 and en_zona_fib:
        estado_entrada = f"💥 REBOTE EN ZONA (Sobrevendido){riesgo_macro_str}"
      elif rsi_val <= 30.0:
        estado_entrada = f"📉 SOBREVENDIDO (Esperar giro | RSI {rsi_val})"
      else:
        estado_entrada = f"⏳ ESPERAR (RSI {rsi_val})"

      ultimos_precios = df["Close"].tail(15).tolist()
      min_p, max_p = min(ultimos_precios), max(ultimos_precios)
      rango_p = max_p - min_p if max_p != min_p else 1
      points = []
      for idx, val in enumerate(ultimos_precios):
        x = round((idx / 14) * 100, 1)
        y = round(35 - ((val - min_p) / rango_p) * 30, 1)
        points.append(f"{x},{y}")
      sparkline_svg = " ".join(points)

      meta_info = CATALOGO_TICKERS.get(symbol, {
          "nombre": symbol,
          "desc": "Activo financiero cotizado en mercados globales.",
          "estrategia": "Análisis técnico estándar basado en soportes, resistencias y volumen.",
      })

      resultado = {
          "symbol": symbol,
          "nombre": meta_info["nombre"],
          "descripcion": meta_info["desc"],
          "estrategia_explicacion": meta_info["estrategia"],
          "timeframe": tf_local.upper(),
          "precio": precio,
          "resistencia": resistencia,
          "soporte_tecnico": soporte_tecnico,
          "tp_tecnico": tp_tecnico,
          "sma9": sma9,
          "sma21": sma21,
          "rsi": rsi_val,
          "tendencia": tendencia,
          "tendencia_macro": tendencia_macro,
          "vol_valido": vol_valido,
          "estado_entrada": estado_entrada,
          "atr": atr_medio,
          "fib_50": fib_500,
          "fib_618": fib_618,
          "en_zona_fib": en_zona_fib,
          "hora": hora,
          "sparkline": sparkline_svg,
          "sparkline_color": "#4ade80" if tendencia == "ALZA" else "#f87171",
      }
      cache_yf[symbol] = {"tf": tf_local, "time": ahora, "data": resultado}
      return resultado
  except Exception:
    pass
  return None


def escaneo_autonomo():
  global recomendaciones_escaner, mercado_actual
  while True:
    try:
      pool = POOLS_ESCANER.get(mercado_actual, POOLS_ESCANER["NY"])
      with ThreadPoolExecutor(max_workers=5) as executor:
        resultados = list(
            executor.map(
                lambda s: procesar_ticker(s, timeframe_actual),
                pool,
            )
        )

      validos = [r for r in resultados if r is not None]
      oportunidades = []
      for r in validos:
        st = r["estado_entrada"]
        if "BUENA ENTRADA" in st or "PREPARANDO" in st or "REBOTE" in st:
          oportunidades.append({
              "ticker": r["symbol"],
              "precio": r["precio"],
              "tp": r["tp_tecnico"],
              "sl": r["soporte_tecnico"],
              "rsi": r["rsi"],
              "estado": r["estado_entrada"],
          })

      oportunidades.sort(
          key=lambda x: (
              0
              if "BUENA ENTRADA" in x["estado"]
              else (1 if "REBOTE" in x["estado"] else 2)
          )
      )
      recomendaciones_escaner = oportunidades[:5]
    except Exception as e:
      print("❌ Error en escáner dinámico:", e)
    time.sleep(120)


def analizar_mercado():
  global estado_mercado, historial_alertas, timeframe_actual, mercado_actual
  while True:
    m_act, datos = db_get_all_data()
    mercado_actual = m_act
    lista_actual = datos.get(mercado_actual, {}).get("activos", [])
    tf_local = timeframe_actual

    with ThreadPoolExecutor(max_workers=5) as executor:
      resultados = executor.map(
          lambda s: procesar_ticker(s, tf_local), lista_actual
      )

    nuevo_estado = {}
    for r in resultados:
      if r:
        sym = r["symbol"]
        nuevo_estado[sym] = r
        if (
            "BUENA ENTRADA" in r["estado_entrada"]
            or "REBOTE EN ZONA" in r["estado_entrada"]
        ):
          _registrar_alerta(
              sym,
              f"🟢 ALERTA ({r['estado_entrada']}) | TP: ${r['tp_tecnico']}",
              r["precio"],
              r["hora"],
              r["soporte_tecnico"],
              r["tp_tecnico"],
          )
        _evaluar_cartera(
            sym,
            r["precio"],
            r["sma9"],
            r["sma21"],
            r["soporte_tecnico"],
            r["atr"],
            r["hora"],
        )

    estado_mercado = nuevo_estado
    notificar_suscriptores()
    time.sleep(INTERVALO_SEGUNDOS)


def _evaluar_cartera(
    symbol, precio_actual, sma9, sma21, soporte_tecnico, atr, hora
):
  global mercado_actual
  m_act, datos = db_get_all_data()
  cartera = datos.get(mercado_actual, {}).get("cartera", [])
  modificado = False
  for pos in cartera:
    if pos["ticker"] == symbol:
      p_compra = pos["precio_compra"]
      sl_user = pos["sl_usuario"]
      p_ganancia = ((precio_actual - p_compra) / p_compra) * 100
      estado_pos = "🔵 MANTENER"

      if sma9 < sma21:
        estado_pos = "⚠️ CRUCE BAJISTA"
        _registrar_alerta(
            symbol,
            "⚠️ CARTERA: Pérdida de impulso.",
            precio_actual,
            hora,
            soporte_tecnico,
            pos["tp_usuario"],
        )
      elif p_ganancia >= 2.0:
        estado_pos = "🟢 EN GANANCIA"

      distancia_sl = precio_actual - sl_user
      mensaje_sl = "✔️ SL Correcto"

      if sl_user >= precio_actual:
        mensaje_sl = "❌ SL Inválido"
      elif p_ganancia >= 3.0 and sl_user < soporte_tecnico:
        mensaje_sl = f"📈 Trailing: Sube SL a soporte (${soporte_tecnico})"
      elif p_ganancia >= 1.5 and sl_user < p_compra:
        mensaje_sl = f"🔔 Asegura: Sube SL a BE (${p_compra})"
      elif distancia_sl < (atr * 0.5):
        mensaje_sl = "⚠️ SL MUY CORTO"
      elif sl_user < (soporte_tecnico * 0.95):
        mensaje_sl = "⚠️ SL MUY LEJOS"

      pos["precio_actual"] = precio_actual
      pos["pnl_porcentaje"] = round(p_ganancia, 2)
      pos["estado"] = estado_pos
      pos["analisis_sl"] = mensaje_sl
      modificado = True

  if modificado:
    datos[mercado_actual]["cartera"] = cartera
    db_save_all_data(mercado_actual, datos)


def _registrar_alerta(symbol, evento, precio, hora, sl=0, tp=0):
  global historial_alertas
  if (
      not historial_alertas
      or historial_alertas[0]["symbol"] != symbol
      or historial_alertas[0]["evento"] != evento
  ):
    historial_alertas.insert(
        0,
        {
            "symbol": symbol,
            "evento": evento,
            "precio": precio,
            "hora": hora,
            "sl": sl,
            "tp": tp,
        },
    )
    historial_alertas = historial_alertas[:20]


threading.Thread(target=analizar_mercado, daemon=True).start()
threading.Thread(target=escaneo_autonomo, daemon=True).start()


def notificar_suscriptores():
  for q in SSE_SUBSCRIBERS:
    try:
      q.put_nowait("update")
    except Exception:
      pass


@app.get("/api/data")
def obtener_datos():
  global mercado_actual
  m_act, datos = db_get_all_data()
  mercado_actual = m_act
  mercado_info = datos.get(mercado_actual, {"activos": [], "cartera": []})
  estado, cuenta_reg = obtener_info_horario()
  return {
      "mercado": estado_mercado,
      "alertas": historial_alertas,
      "cartera": mercado_info.get("cartera", []),
      "timeframe": timeframe_actual,
      "mercado_actual": mercado_actual,
      "horario": estado,
      "cuenta_regresiva": cuenta_reg,
      "sugerencias": recomendaciones_escaner,
      "catalogo": CATALOGO_TICKERS,
      "activos_orden": mercado_info.get("activos", []),
  }


@app.get("/api/stream")
async def stream_endpoint(request: Request):
  q = asyncio.Queue()
  SSE_SUBSCRIBERS.append(q)

  async def event_generator():
    try:
      while True:
        if await request.is_disconnected():
          break
        try:
          await asyncio.wait_for(q.get(), timeout=15.0)
          yield "data: update\n\n"
        except asyncio.TimeoutError:
          yield ":ping\n\n"
    except Exception:
      pass
    finally:
      if q in SSE_SUBSCRIBERS:
        SSE_SUBSCRIBERS.remove(q)

  return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/add")
async def agregar_activo(request: Request):
  global mercado_actual
  try:
    data = await request.json()
    symbol = data.get("ticker", "").strip().upper()
  except Exception:
    symbol = ""

  if symbol:
    m_act, datos = db_get_all_data()
    mercado_actual = m_act
    activos = datos[mercado_actual]["activos"]
    if symbol not in activos:
      activos.append(symbol)
      db_save_all_data(mercado_actual, datos)

      def _fetch_and_update():
        res = procesar_ticker(symbol, timeframe_actual)
        if res:
          estado_mercado[symbol] = res
          notificar_suscriptores()

      threading.Thread(target=_fetch_and_update, daemon=True).start()
  return {"status": "ok"}


@app.post("/api/remove")
async def eliminar_activo(request: Request):
  global mercado_actual
  try:
    data = await request.json()
    symbol = data.get("ticker", "").strip().upper()
  except Exception:
    symbol = ""

  if symbol:
    m_act, datos = db_get_all_data()
    mercado_actual = m_act
    activos = datos[mercado_actual]["activos"]
    if symbol in activos:
      activos.remove(symbol)
      db_save_all_data(mercado_actual, datos)
      if symbol in estado_mercado:
        del estado_mercado[symbol]
  return {"status": "ok"}


@app.post("/api/reorder")
async def reordenar_activos(item: ReordenarModel):
  global mercado_actual
  if item.activos:
    m_act, datos = db_get_all_data()
    mercado_actual = m_act
    datos[mercado_actual]["activos"] = [
        s.strip().upper() for s in item.activos if s.strip()
    ]
    db_save_all_data(mercado_actual, datos)
  return {"status": "ok"}


@app.post("/api/cartera/add")
def agregar_cartera(item: PosicionModel):
  global mercado_actual
  m_act, datos = db_get_all_data()
  mercado_actual = m_act
  cartera = datos[mercado_actual]["cartera"]
  ticker = item.ticker.strip().upper()
  distancia_sl = abs(item.precio_compra - item.sl_usuario)
  acciones = (
      round(item.riesgo_usd / distancia_sl, 2) if distancia_sl > 0 else 0
  )
  inversion = round(acciones * item.precio_compra, 2)
  cartera = [p for p in cartera if p["ticker"] != ticker]
  cartera.append({
      "ticker": ticker,
      "precio_compra": item.precio_compra,
      "sl_usuario": item.sl_usuario,
      "tp_usuario": item.tp_usuario,
      "riesgo_usd": item.riesgo_usd,
      "acciones": acciones,
      "inversion_total": inversion,
      "timeframe": item.timeframe,
      "precio_actual": item.precio_compra,
      "pnl_porcentaje": 0.0,
      "estado": "🔵 MANTENER",
      "analisis_sl": "Analizando...",
  })
  datos[mercado_actual]["cartera"] = cartera
  db_save_all_data(mercado_actual, datos)
  return {"status": "ok"}


@app.post("/api/cartera/remove")
async def eliminar_cartera(request: Request):
  global mercado_actual
  try:
    data = await request.json()
    ticker = data.get("ticker", "").strip().upper()
  except Exception:
    ticker = ""
  if ticker:
    m_act, datos = db_get_all_data()
    mercado_actual = m_act
    cartera = datos[mercado_actual]["cartera"]
    cartera = [p for p in cartera if p["ticker"] != ticker]
    datos[mercado_actual]["cartera"] = cartera
    db_save_all_data(mercado_actual, datos)
  return {"status": "ok"}


@app.post("/api/timeframe")
def cambiar_timeframe(item: TimeframeModel):
  global timeframe_actual, estado_mercado
  if item.timeframe in ["1h", "4h", "1d"]:
    timeframe_actual = item.timeframe
    estado_mercado = {}
  return {"status": "ok"}


@app.post("/api/mercado")
def cambiar_mercado(item: MercadoModel):
  global mercado_actual, estado_mercado
  if item.mercado in ["NY", "LONDRES", "ASIA"]:
    m_act, datos = db_get_all_data()
    mercado_actual = item.mercado
    db_save_all_data(mercado_actual, datos)
    estado_mercado = {}
  return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def dashboard():
  return """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Trading Monitor Pro</title>
        <style>
            * { box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0b132b; color: #f8fafc; margin: 0; padding: 12px; }
            h1 { text-align: center; color: #38bdf8; font-size: 1.6rem; margin: 5px 0; }
            .reloj-box { text-align: center; margin-bottom: 16px; position: relative; }
            .reloj { font-weight: bold; font-size: 1rem; }
            .reloj-sub { font-size: 0.8rem; color: #94a3b8; margin-top: 2px; }
            .live-indicator { display: inline-block; width: 8px; height: 8px; background: #22c55e; border-radius: 50%; margin-left: 6px; box-shadow: 0 0 8px #22c55e; animation: pulse 1.5s infinite; }
            @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.3; } 100% { opacity: 1; } }
            
            .control-panel { max-width: 1200px; margin: 0 auto 16px auto; background: #1c2541; padding: 12px; border-radius: 10px; display: flex; gap: 8px; align-items: center; justify-content: center; flex-wrap: wrap; border: 1px solid #3a506b; }
            input[type="text"], input[type="number"], select { background: #0b132b; border: 1px solid #3a506b; color: #fff; padding: 8px; border-radius: 6px; font-size: 0.9rem; }
            input[type="text"] { width: 160px; text-transform: uppercase; }
            button { background: #38bdf8; color: #0b132b; border: none; padding: 8px 12px; font-weight: bold; border-radius: 6px; cursor: pointer; }
            button:hover { background: #7dd3fc; }
            
            .btn-mercado { background: #3a506b; color: #cbd5e1; border: 1px solid #3a506b; }
            .btn-mercado.active { background: #38bdf8; color: #0b132b; border-color: #7dd3fc; font-weight: 800; }

            .container { max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1.2fr; gap: 16px; }
            @media (max-width: 900px) { 
                .container { grid-template-columns: 1fr; } 
                .sidebar-prioritario { order: -1; } 
            }
            
            .grid-activos { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 12px; }
            .grid-activos.list-view { grid-template-columns: 1fr; }
            
            .card { background: #1c2541; border-radius: 10px; padding: 14px; border: 1px solid #3a506b; position: relative; }
            .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
            .ticker { font-weight: bold; font-size: 1.1rem; display: flex; align-items: center; gap: 6px; }
            .price { font-size: 1.4rem; font-weight: 800; margin-bottom: 6px; }
            
            .card-top-toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; border-bottom: 1px solid #3a506b; padding-bottom: 6px; }
            
            .grid-activos.list-view .card { display: flex; flex-direction: row; align-items: center; justify-content: space-between; padding: 10px 14px; gap: 10px; flex-wrap: wrap; }
            .grid-activos.list-view .card-top-toolbar { display: none; }
            .grid-activos.list-view .card-header { margin-bottom: 0; width: 120px; }
            .grid-activos.list-view .price { font-size: 1.1rem; margin-bottom: 0; width: 75px; }
            .grid-activos.list-view .entrada-ok, .grid-activos.list-view .entrada-prep, .grid-activos.list-view .entrada-wait, .grid-activos.list-view .entrada-warn, .grid-activos.list-view .entrada-rebote { margin-bottom: 0; width: 160px; text-align: center; font-size: 0.72rem; cursor: pointer; }
            .grid-activos.list-view .sparkline-container { width: 80px; height: 25px; margin-top: 0; }
            .grid-activos.list-view .levels-box { display: none; }
            .grid-activos.list-view .list-actions-bar { display: flex; gap: 6px; align-items: center; }

            .badge { padding: 3px 6px; border-radius: 10px; font-size: 0.68rem; font-weight: bold; }
            .tf-badge { background: #3a506b; color: #cbd5e1; padding: 2px 5px; border-radius: 4px; font-size: 0.65rem; }
            .bullish { background: rgba(34, 197, 94, 0.2); color: #4ade80; border: 1px solid #22c55e; }
            .bearish { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #ef4444; }
            
            .entrada-ok { background: rgba(34, 197, 94, 0.3); color: #4ade80; font-weight: bold; padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; display: inline-block; margin-bottom: 8px; cursor: pointer; }
            .entrada-prep { background: rgba(234, 179, 8, 0.2); color: #facc15; border: 1px solid #eab308; font-weight: bold; padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; display: inline-block; margin-bottom: 8px; cursor: pointer; }
            .entrada-wait { background: rgba(148, 163, 184, 0.1); color: #94a3b8; padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; display: inline-block; margin-bottom: 8px; cursor: pointer; }
            .entrada-warn { background: rgba(245, 158, 11, 0.2); color: #f59e0b; border: 1px solid #f59e0b; font-weight: bold; padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; display: inline-block; margin-bottom: 8px; cursor: pointer; }
            .entrada-rebote { background: rgba(168, 85, 247, 0.25); color: #c084fc; border: 1px solid #a855f7; font-weight: bold; padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; display: inline-block; margin-bottom: 8px; cursor: pointer; }
            
            .stat { display: flex; justify-content: space-between; margin-top: 5px; font-size: 0.82rem; color: #cbd5e1; }
            .levels-box { background: #0b132b; padding: 8px; border-radius: 6px; margin-top: 6px; border: 1px solid #3a506b; }
            .sl-text { color: #f87171; font-weight: bold; }
            .tp-text { color: #4ade80; font-weight: bold; }
            .btn-remove { background: rgba(239, 68, 68, 0.2); color: #ef4444; border: 1px solid #ef4444; padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; cursor: pointer; font-weight: bold; }
            
            .sparkline-container { margin-top: 8px; background: #0b132b; padding: 4px; border-radius: 6px; border: 1px solid #3a506b; text-align: center; }
            
            .feed-panel, .cartera-panel, .edu-panel { background: #1c2541; border-radius: 10px; padding: 14px; border: 1px solid #3a506b; margin-bottom: 16px; }
            .feed-title { font-size: 1rem; color: #38bdf8; margin-bottom: 10px; border-bottom: 1px solid #3a506b; padding-bottom: 6px; display: flex; justify-content: space-between; align-items: center; }
            .alerta-item { background: #0b132b; border-left: 4px solid #38bdf8; padding: 8px; margin-bottom: 6px; border-radius: 4px; }
            
            .metrics-bar { background: #0b132b; padding: 8px; border-radius: 6px; margin-bottom: 10px; display: flex; justify-content: space-around; font-size: 0.85rem; border: 1px solid #3a506b; }
            
            #loading-banner { display: none; position: fixed; top: 15px; right: 15px; background: #f59e0b; color: #0b132b; padding: 8px 14px; border-radius: 8px; font-weight: bold; font-size: 0.85rem; z-index: 1000; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }

            .manual-box { background: #0b132b; border: 1px solid #38bdf8; padding: 12px; border-radius: 8px; margin-top: 10px; font-size: 0.8rem; color: #cbd5e1; }
            .manual-tag { font-weight: bold; display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 0.72rem; margin-right: 4px; }
            
            .input-group { display: flex; flex-direction: column; flex: 1; min-width: 80px; }
            .input-group label { font-size: 0.72rem; color: #38bdf8; margin-bottom: 3px; font-weight: bold; }

            /* Modal General */
            #info-modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7); z-index: 2000; justify-content: center; align-items: center; }
            .modal-content { background: #1c2541; padding: 20px; border-radius: 10px; border: 1px solid #38bdf8; width: 90%; max-width: 500px; color: #f8fafc; position: relative; }
            .modal-close { position: absolute; top: 10px; right: 15px; background: none; border: none; color: #ef4444; font-size: 1.2rem; cursor: pointer; }
        </style>
        <script type="text/javascript">
            function googleTranslateElementInit() {
                new google.translate.TranslateElement({pageLanguage: 'es', includedLanguages: 'es,en', layout: google.translate.TranslateElement.InlineLayout.SIMPLE}, 'google_translate_element');
            }
        </script>
        <script type="text/javascript" src="//translate.google.com/translate_a/element.js?cb=googleTranslateElementInit"></script>
    </head>
    <body>
        <div id="loading-banner">🔄 <span>Actualizando datos...</span></div>

        <div style="display: flex; justify-content: space-between; align-items: center; max-width: 1200px; margin: 0 auto;">
            <div></div>
            <h1>📊 Trading Monitor Pro</h1>
            <div id="google_translate_element"></div>
        </div>

        <div class="reloj-box">
            <div id="reloj-mercado" class="reloj">... <span class="live-indicator" title="Sincronización en vivo activa"></span></div>
            <div id="reloj-cuenta" class="reloj-sub">...</div>
        </div>
        
        <div class="control-panel">
            <div style="display:flex; gap:4px; align-items:center;">
                <span style="font-size:0.75rem; color:#38bdf8; font-weight:bold;">MERCADO:</span>
                <button onclick="cambiarMercado('NY')" id="btn-mercado-NY" class="btn-mercado active">🇺🇸 NY</button>
                <button onclick="cambiarMercado('LONDRES')" id="btn-mercado-LONDRES" class="btn-mercado">🇬🇧 Londres</button>
                <button onclick="cambiarMercado('ASIA')" id="btn-mercado-ASIA" class="btn-mercado">🇯🇵 Asia</button>
            </div>

            <div style="position: relative;">
                <input type="text" id="new-ticker" placeholder="Buscar Ticker..." list="datalist-tickers" onkeydown="if(event.key==='Enter') agregarActivo()" autocomplete="off" />
                <datalist id="datalist-tickers"></datalist>
            </div>
            <button onclick="agregarActivo()">+ Seguir</button>
            <select id="select-tf" onchange="cambiarTimeframe(this.value)">
                <option value="1h">1H (Intradiario)</option>
                <option value="4h">4H (Swing)</option>
                <option value="1d">1D (Diario)</option>
            </select>
            <button onclick="toggleVista()" id="btn-vista" style="background:#3a506b; color:#fff;">📋 Lista</button>
            <button onclick="solicitarPermisoNotificaciones()" style="background:#f59e0b; color:#0b132b;" title="Recibe alertas nativas">🔔 Alertas</button>
        </div>

        <div class="container">
            <div>
                <h3>Activos bajo Monitoreo (Orden Automático por Urgencia)</h3>
                <div class="grid-activos" id="grid-mercado"><p style="color:#94a3b8;">⏳ Sincronizando con servidores...</p></div>

                <div class="cartera-panel" style="margin-top: 16px;">
                    <div class="feed-title">
                        <span>💼 Mi Cartera y Gestión de Riesgo</span>
                    </div>
                    
                    <div class="metrics-bar" id="resumen-cartera">
                        <span>Capital: <b>$0.00</b></span>
                        <span>Rendimiento: <b>0.00%</b></span>
                    </div>

                    <div style="font-size:0.8rem; font-weight:bold; color:#38bdf8; margin-bottom:6px;">🧮 CALCULADORA DE TAMAÑO DE POSICIÓN Y RIESGO</div>
                    <div style="display:flex; flex-wrap:wrap; gap:6px; margin-bottom:12px; background:#0b132b; padding:10px; border-radius:6px; border:1px solid #3a506b;">
                        <div class="input-group">
                            <label>ACTIVO</label>
                            <input type="text" id="c-ticker" placeholder="Ej: NVDA" style="width:100%; text-transform:uppercase;" />
                        </div>
                        <div class="input-group">
                            <label>ENTRADA ($)</label>
                            <input type="number" id="c-precio" placeholder="0.00" style="width:100%;" step="any" />
                        </div>
                        <div class="input-group">
                            <label>STOP LOSS ($)</label>
                            <input type="number" id="c-sl" placeholder="0.00" style="width:100%;" step="any" />
                        </div>
                        <div class="input-group">
                            <label>TAKE PROFIT ($)</label>
                            <input type="number" id="c-tp" placeholder="0.00" style="width:100%;" step="any" />
                        </div>
                        <div class="input-group">
                            <label>RIESGO USD ($)</label>
                            <input type="number" id="c-riesgo" placeholder="50" style="width:100%;" value="50" step="any" title="Dinero máximo a arriesgar" />
                        </div>
                        <div style="display:flex; align-items:flex-end;">
                            <button onclick="registrarPosicion()" style="height:38px; background:#10b981; color:#fff;">Guardar</button>
                        </div>
                    </div>
                    <div id="lista-cartera">Sin posiciones guardadas.</div>
                </div>
            </div>
            
            <div class="sidebar-prioritario">
                <div class="feed-panel">
                    <div class="feed-title">🚨 Feed de Alertas en Vivo (Acción Urgente)</div>
                    <div id="lista-alertas">Sin señales recientes.</div>
                </div>

                <div class="feed-panel">
                    <div class="feed-title">🤖 Escáner Dinámico de Oportunidades</div>
                    <div id="lista-sugerencias" style="font-size:0.85rem; color:#cbd5e1;">Buscando Momentum y Rebotes...</div>
                </div>

                <div class="edu-panel">
                    <div class="feed-title">📖 Manual PRO & Horarios</div>
                    <button onclick="toggleManual()" style="width:100%; font-size:0.78rem; background:#3a506b; color:#fff; margin-bottom:8px; border:none; padding:6px; border-radius:4px; cursor:pointer;">📘 Ver / Ocultar Guía de Mercados</button>
                    
                    <div id="box-manual" class="manual-box" style="display:none;">
                        <b>⏰ Horarios de Sesiones Globales:</b><br>
                        • 🇺🇸 <b>Nueva York (NYSE):</b> 09:30 a 16:00 hora NY.<br>
                        • 🇬🇧 <b>Londres (LSE):</b> 08:00 a 16:30 hora Londres.<br>
                        • 🇯🇵 <b>Asia (Tokio):</b> 09:00 a 15:30 hora Tokio (con receso de almuerzo 11:30-12:30).<br><br>
                        💡 <b>Nota:</b> Al cambiar de mercado en el botón superior, los 10 activos por defecto y tu cartera se adaptan automáticamente a esa bolsa. El reloj muestra la hora exacta del mercado seleccionado.
                    </div>
                </div>
            </div>
        </div>

        <!-- MODAL DE INFORMACIÓN Y ESTADOS -->
        <div id="info-modal">
            <div class="modal-content">
                <button class="modal-close" onclick="cerrarModal()">✕</button>
                <h3 id="modal-titulo" style="color:#38bdf8; margin-top:0;">Información</h3>
                <div id="modal-body-content">
                    <p><b>Descripción y Contexto:</b></p>
                    <p id="modal-desc" style="color:#cbd5e1; font-size:0.85rem;"></p>
                    <p><b>Estrategia e Indicadores:</b></p>
                    <p id="modal-estrategia" style="color:#cbd5e1; font-size:0.85rem;"></p>
                </div>
            </div>
        </div>

        <script>
            let eventoSource = null;
            let ordenActivosGlobal = [];
            let modoLista = false;
            let mercadoGlobalData = {};
            let catalogoGlobal = {};
            let ultimaAlertaVistaId = null;

            const explicacionesEstados = {
                "BUENA ENTRADA": {
                    titulo: "🟢 Buena Entrada Detectada",
                    porque: "El precio superó su resistencia clave y hay suficiente dinero real (volumen) respaldando el movimiento alcista.",
                    resultado: "El análisis técnico confirma que los compradores tienen el control absoluto a corto plazo.",
                    queHacer: "Es un escenario favorable. Puedes operar con confianza usando el botón 'Operar' para calcular tu tamaño de posición."
                },
                "PREPARANDO": {
                    titulo: "⏳ Preparando Movimiento",
                    porque: "El activo está retrocediendo de manera saludable hacia una zona de imán o soporte (como los niveles de Fibonacci) antes de seguir subiendo.",
                    resultado: "El precio está descansando de su última subida, lo cual es normal y sano.",
                    queHacer: "Monitorea de cerca. No compres de golpe todavía; espera a que toque el soporte y muestre intenciones de rebotar."
                },
                "REBOTE EN ZONA": {
                    titulo: "💥 Rebote en Zona de Sobreventa",
                    porque: "El activo cayó con fuerza y tocó un piso técnico importante (RSI menor o igual a 30) desde donde los compradores suelen despertar.",
                    resultado: "El mercado castigó demasiado al precio y ahora está rebotando desde el suelo.",
                    queHacer: "Oportunidad ideal para buscar un rebote rápido al alza. Entra con precaución y mantén tu Stop Loss ajustado por seguridad."
                },
                "FALSO QUIEBRE": {
                    titulo: "⚠️ Falso Quiebre (Trampa)",
                    porque: "El precio intentó romper una resistencia hacia arriba, pero no hay dinero real (volumen) detrás de la subida.",
                    resultado: "Es una trampa típica del mercado para atrapar compradores desprevenidos antes de una caída.",
                    queHacer: "¡No entres! Ignora la subida. Lo más probable es que el precio vuelva a caer al rango anterior en poco tiempo."
                },
                "SOBRECOMPRADO": {
                    titulo: "⚠️ Zona de Sobrecompra",
                    porque: "El activo subió demasiado rápido en muy poco tiempo y los inversores están empezando a tomar ganancias.",
                    resultado: "El precio está 'caro' a corto plazo y la fuerza compradora se está agotando.",
                    queHacer: "Evita comprar aquí. El riesgo de una corrección o caída repentina es alto; espera a que el precio descanse."
                },
                "SOBREVENDIDO": {
                    titulo: "📉 Zona de Sobreventa",
                    porque: "El precio ha tenido una caída muy fuerte y prolongada, pero el mercado aún no muestra señales claras de haber encontrado un suelo definitivo.",
                    resultado: "El activo está cayendo con fuerza sin frenos aparentes.",
                    queHacer: "No compres por desesperación. Espera pacientemente a que el gráfico dibuje una vela de giro o cambie de dirección."
                },
                "ESPERAR": {
                    titulo: "⏳ Esperar y Observar",
                    porque: "El activo está en una zona neutral, sin fuerza clara, sin volumen relevante ni dirección definida.",
                    resultado: "El mercado está indeciso y lateral.",
                    queHacer: "No hagas nada. Guarda tu capital y pasa a revisar otro activo que sí tenga una oportunidad clara."
                },
                "CRUCE BAJISTA": {
                    titulo: "⚠️ Cruce Bajista en Cartera",
                    porque: "La media móvil rápida cruzó por debajo de la lenta, indicando pérdida de impulso y posible cambio de tendencia a la baja.",
                    resultado: "La fuerza alcista se rompió.",
                    queHacer: "Si tienes este activo en tu cartera, evalúa asegurar tus ganancias o recortar pérdidas antes de que la caída sea mayor."
                },
                "SL Correcto": {
                    titulo: "✔️ Stop Loss Correcto",
                    porque: "Tu nivel de protección está ubicado de forma lógica por debajo del último soporte técnico.",
                    resultado: "Tu riesgo está bien delimitado frente a fluctuaciones normales del mercado.",
                    queHacer: "No necesitas hacer nada. Mantén la posición tranquila y deja que trabaje."
                },
                "TRAILING": {
                    titulo: "📈 Trailing Stop Recomendado",
                    porque: "Tu operación ya ganó buen porcentaje y el precio ha subido considerablemente.",
                    resultado: "El mercado te está regalando margen de ganancia.",
                    queHacer: "Sube tu Stop Loss al último soporte técnico para asegurar tus ganancias sin arriesgar tu capital inicial."
                },
                "ASEGURA": {
                    titulo: "🔔 Asegurar a Break Even",
                    porque: "Tu operación ya superó el 1.5% o más de ganancia.",
                    resultado: "Estás en zona ganadora con holgura.",
                    queHacer: "Sube tu Stop Loss exactamente al precio en que compraste (Break Even). Así, si el mercado se da vuelta de repente, sales con cero pérdidas."
                },
                "SL MUY CORTO": {
                    titulo: "⚠️ Stop Loss Muy Corto",
                    porque: "Tu protección está demasiado cerca del precio actual.",
                    resultado: "Cualquier pequeña fluctuación normal del mercado te sacará de la operación por error.",
                    queHacer: "Amplía un poco más abajo tu Stop Loss, colocándolo debajo del soporte técnico real."
                },
                "SL MUY LEJOS": {
                    titulo: "⚠️ Stop Loss Muy Lejos",
                    porque: "Tu protección está demasiado separada del precio de entrada.",
                    resultado: "Estás arriesgando mucho más dinero del necesario si la operación sale mal.",
                    queHacer: "Acerca tu Stop Loss al soporte técnico para mantener tu gestión de riesgo estricta."
                },
                "ANALIZANDO": {
                    titulo: "🔄 Analizando Posición",
                    porque: "El sistema está procesando los precios en tiempo real para auditar el estado de tu Stop Loss.",
                    resultado: "Sincronizando datos de mercado.",
                    queHacer: "Espera unos segundos a que reciba la primera actualización en vivo."
                }
            };

            function solicitarPermisoNotificaciones() {
                if (!("Notification" in window)) {
                    alert("Este navegador no soporta notificaciones de escritorio.");
                    return;
                }
                Notification.requestPermission().then(permission => {
                    if (permission === "granted") {
                        new Notification("Trading Monitor Pro", {
                            body: "¡Notificaciones de navegador activadas con éxito!",
                            icon: ""
                        });
                    } else {
                        alert("Permiso de notificaciones denegado.");
                    }
                });
            }

            function dispararNotificacionEscritorio(titulo, cuerpo) {
                if ("Notification" in window && Notification.permission === "granted") {
                    new Notification(titulo, { body: cuerpo, icon: "" });
                }
            }

            function toggleManual() {
                const el = document.getElementById('box-manual');
                el.style.display = el.style.display === 'none' ? 'block' : 'none';
            }

            function toggleVista() {
                modoLista = !modoLista;
                const grid = document.getElementById('grid-mercado');
                if(modoLista) {
                    grid.classList.add('list-view');
                    document.getElementById('btn-vista').innerText = "🔲 Cuadrícula";
                } else {
                    grid.classList.remove('list-view');
                    document.getElementById('btn-vista').innerText = "📋 Lista";
                }
                renderizarGridMercado();
            }

            async function cambiarMercado(mercado) {
                mostrarBannerCarga(true);
                await fetch('/api/mercado', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ mercado: mercado })
                });
                await actualizarApp(true);
                mostrarBannerCarga(false);
            }

            function mostrarModal(ticker) {
                const info = mercadoGlobalData[ticker] || (catalogoGlobal[ticker] ? {
                    symbol: ticker,
                    nombre: catalogoGlobal[ticker].nombre,
                    descripcion: catalogoGlobal[ticker].desc,
                    estrategia_explicacion: catalogoGlobal[ticker].estrategia
                } : {
                    symbol: ticker,
                    nombre: ticker,
                    descripcion: "Activo detectado por escáner de alta volatilidad y momentum.",
                    estrategia_explicacion: "Monitoreo técnico de soportes, resistencias y confirmación de volumen institucional."
                });

                document.getElementById('modal-titulo').innerText = `${ticker} - ${info.nombre || ''}`;
                document.getElementById('modal-body-content').innerHTML = `
                    <p><b>Descripción y Contexto:</b></p>
                    <p style="color:#cbd5e1; font-size:0.85rem;">${info.descripcion || "Sin descripción disponible."}</p>
                    <p><b>Estrategia e Indicadores:</b></p>
                    <p style="color:#cbd5e1; font-size:0.85rem;">${info.estrategia_explicacion || "Estrategia estándar de monitoreo técnico."}</p>
                `;
                document.getElementById('info-modal').style.display = 'flex';
            }

            function mostrarExplicacionEstado(claveEstado) {
                let encontrado = null;
                for (const [key, val] of Object.entries(explicacionesEstados)) {
                    if (claveEstado.toUpperCase().includes(key.toUpperCase())) {
                        encontrado = val;
                        break;
                    }
                }

                if (!encontrado) {
                    encontrado = {
                        titulo: "ℹ️ Estado del Sistema",
                        porque: "Este estado indica la condición actual del activo basada en cruces de medias móviles, RSI y volumen.",
                        resultado: "Lectura técnica en proceso.",
                        queHacer: "Vigila el comportamiento del precio en los niveles de soporte y resistencia cercanos."
                    };
                }

                document.getElementById('modal-titulo').innerText = encontrado.titulo;
                document.getElementById('modal-body-content').innerHTML = `
                    <p><b>¿Por qué ocurre esto? (El por qué):</b></p>
                    <p style="color:#cbd5e1; font-size:0.85rem; background:#0b132b; padding:8px; border-radius:6px; border:1px solid #3a506b;">${encontrado.porque}</p>
                    
                    <p><b>Resultado del análisis:</b></p>
                    <p style="color:#cbd5e1; font-size:0.85rem; background:#0b132b; padding:8px; border-radius:6px; border:1px solid #3a506b;">${encontrado.resultado}</p>
                    
                    <p><b>¿Qué deberías hacer?:</b></p>
                    <p style="color:#4ade80; font-weight:bold; font-size:0.88rem; background:#0b132b; padding:8px; border-radius:6px; border:1px solid #3a506b;">👉 ${encontrado.queHacer}</p>
                `;
                document.getElementById('info-modal').style.display = 'flex';
            }

            function cerrarModal() {
                document.getElementById('info-modal').style.display = 'none';
            }

            function mostrarBannerCarga(mostrar) {
                document.getElementById('loading-banner').style.display = mostrar ? 'block' : 'none';
            }

            function iniciarSSE() {
                if (!!window.EventSource) {
                    eventoSource = new EventSource('/api/stream');
                    eventoSource.onmessage = function(e) {
                        if(e.data === 'update') { actualizarApp(false); }
                    };
                }
            }

            async function cambiarTimeframe(tf) {
                mostrarBannerCarga(true);
                await fetch('/api/timeframe', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ timeframe: tf })
                });
                await actualizarApp(true);
                mostrarBannerCarga(false);
            }

            async function agregarActivo(tickerParam = null) {
                const input = document.getElementById('new-ticker');
                const ticker = tickerParam || input.value.trim();
                if (!ticker) return;
                
                mostrarBannerCarga(true);
                await fetch('/api/add', {
                    method: 'POST', 
                    headers: {'Content-Type': 'application/json'}, 
                    body: JSON.stringify({ ticker: ticker })
                });
                if(!tickerParam) input.value = '';
                await actualizarApp(true);
                mostrarBannerCarga(false);
            }

            function usarParaOperar(ticker, precio, sl, tp) {
                document.getElementById('c-ticker').value = ticker;
                document.getElementById('c-precio').value = precio;
                document.getElementById('c-sl').value = sl;
                document.getElementById('c-tp').value = tp;
                window.scrollTo({ top: 0, behavior: 'smooth' });
            }

            async function eliminarActivo(ticker) {
                mostrarBannerCarga(true);
                await fetch('/api/remove', {
                    method: 'POST', 
                    headers: {'Content-Type': 'application/json'}, 
                    body: JSON.stringify({ ticker: ticker })
                });
                await actualizarApp(true);
                mostrarBannerCarga(false);
            }

            async function registrarPosicion() {
                const ticker = document.getElementById('c-ticker').value.trim();
                const precio = parseFloat(document.getElementById('c-precio').value);
                const sl = parseFloat(document.getElementById('c-sl').value);
                const tp = parseFloat(document.getElementById('c-tp').value);
                const riesgo = parseFloat(document.getElementById('c-riesgo').value) || 50;
                const tf = document.getElementById('select-tf').value;
                if (!ticker || isNaN(precio) || isNaN(sl) || isNaN(tp)) return;

                await fetch('/api/cartera/add', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ ticker: ticker, precio_compra: precio, sl_usuario: sl, tp_usuario: tp, riesgo_usd: riesgo, timeframe: tf })
                });
                document.getElementById('c-ticker').value = '';
                document.getElementById('c-precio').value = '';
                document.getElementById('c-sl').value = '';
                document.getElementById('c-tp').value = '';
                actualizarApp(true);
            }

            async function eliminarPosicion(ticker) {
                await fetch('/api/cartera/remove', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ ticker: ticker })
                });
                actualizarApp(true);
            }

            function obtenerPuntajeUrgencia(ticker) {
                const info = mercadoGlobalData[ticker];
                if (!info) return 3;
                const st = info.estado_entrada || "";
                if (st.includes("BUENA ENTRADA") || st.includes("REBOTE")) return 1;
                if (st.includes("PREPARANDO") || st.includes("SOBRE") || st.includes("FALSO")) return 2;
                return 3;
            }

            async function actualizarApp(forzarRender = false) {
                try {
                    const res = await fetch('/api/data');
                    const { mercado, alertas, cartera, timeframe, mercado_actual, horario, cuenta_regresiva, sugerencias, catalogo, activos_orden } = await res.json();
                    
                    document.getElementById('select-tf').value = timeframe;
                    if(activos_orden) ordenActivosGlobal = activos_orden;
                    if(mercado) mercadoGlobalData = mercado;
                    if(catalogo) catalogoGlobal = catalogo;

                    ['NY', 'LONDRES', 'ASIA'].forEach(m => {
                        const btn = document.getElementById(`btn-mercado-${m}`);
                        if(btn) {
                            if(m === mercado_actual) btn.classList.add('active');
                            else btn.classList.remove('active');
                        }
                    });

                    if(ordenActivosGlobal.length > 0 && Object.keys(mercadoGlobalData).length > 0) {
                        ordenActivosGlobal.sort((a, b) => obtenerPuntajeUrgencia(a) - obtenerPuntajeUrgencia(b));
                    }
                    
                    const datalist = document.getElementById('datalist-tickers');
                    if(datalist.children.length === 0 && catalogo) {
                        for(const [t, desc] of Object.entries(catalogo)) {
                            const opt = document.createElement('option');
                            opt.value = t;
                            opt.textContent = desc.nombre;
                            datalist.appendChild(opt);
                        }
                    }

                    document.getElementById('reloj-mercado').innerHTML = `${horario} <span class="live-indicator" title="Sincronización en vivo activa"></span>`;
                    document.getElementById('reloj-cuenta').innerHTML = `${cuenta_regresiva}`;

                    if (alertas && alertas.length > 0) {
                        const ultima = alertas[0];
                        const idUnico = ultima.symbol + "_" + ultima.hora + "_" + ultima.precio;
                        if (ultimaAlertaVistaId !== null && ultimaAlertaVistaId !== idUnico) {
                            dispararNotificacionEscritorio(`🚨 Alerta Trading: ${ultima.symbol}`, `${ultima.evento} - Precio: $${ultima.precio}`);
                        }
                        ultimaAlertaVistaId = idUnico;
                    }

                    let capitalTotal = 0;
                    let pnlSuma = 0;

                    const divCartera = document.getElementById('lista-cartera');
                    if (cartera.length > 0) {
                        divCartera.innerHTML = '';
                        cartera.forEach(p => {
                            capitalTotal += p.inversion_total || 0;
                            pnlSuma += p.pnl_porcentaje || 0;
                            const pnlColor = p.pnl_porcentaje >= 0 ? '#4ade80' : '#f87171';
                            let slColor = p.analisis_sl.includes("Correcto") ? '#4ade80' : '#f87171';
                            if (p.analisis_sl.includes("Sube SL") || p.analisis_sl.includes("Trailing") || p.analisis_sl.includes("Asegura")) {
                                slColor = '#facc15';
                            }
                            divCartera.innerHTML += `
                                <div class="alerta-item" style="border-left-color: ${pnlColor};">
                                    <div style="display:flex; justify-content:space-between; font-weight:bold; font-size:0.9rem;">
                                        <span>${p.ticker} (${p.timeframe.toUpperCase()})</span>
                                        <span style="color:${pnlColor};">${p.pnl_porcentaje >= 0 ? '+' : ''}${p.pnl_porcentaje}%</span>
                                        <button onclick="eliminarPosicion('${p.ticker}')" style="background:none;color:#ef4444;border:none;cursor:pointer;">✕</button>
                                    </div>
                                    <div style="font-size:0.8rem; margin-top:4px;">Entrada: $${p.precio_compra} | Actual: $${p.precio_actual} | TP: $${p.tp_usuario}</div>
                                    <div style="font-size:0.8rem; color:#38bdf8; margin-top:2px; font-weight:bold;">Acciones: ${p.acciones || 0} ($${p.inversion_total || 0})</div>
                                    <div style="font-size:0.8rem; margin-top:2px; font-weight:bold; color:${slColor}; cursor:pointer;" onclick="mostrarExplicacionEstado('${p.analisis_sl}')" title="Haz clic para entender este estado">SL Audit: <span style="text-decoration:underline;">${p.analisis_sl}</span> 🔍</div>
                                </div>
                            `;
                        });
                        const pnlPromedio = (pnlSuma / cartera.length).toFixed(2);
                        document.getElementById('resumen-cartera').innerHTML = `
                            <span>Capital: <b>$${capitalTotal.toFixed(2)}</b></span>
                            <span>Rendimiento: <b style="color:${pnlPromedio >= 0 ? '#4ade80' : '#f87171'}">${pnlPromedio >= 0 ? '+' : ''}${pnlPromedio}%</b></span>
                        `;
                    } else { 
                        divCartera.innerHTML = `<span style="font-size:0.8rem; color:#94a3b8;">Sin posiciones guardadas.</span>`;
                        document.getElementById('resumen-cartera').innerHTML = `<span>Capital: <b>$0.00</b></span><span>Rendimiento: <b>0.00%</b></span>`;
                    }

                    const divSug = document.getElementById('lista-sugerencias');
                    if(sugerencias && sugerencias.length > 0) {
                        divSug.innerHTML = '';
                        sugerencias.forEach(s => {
                            let badgeStyle = "color:#4ade80;";
                            if(s.estado.includes("REBOTE")) badgeStyle = "color:#c084fc;";
                            divSug.innerHTML += `
                                <div style="background:#0b132b; padding:8px; border-radius:6px; margin-bottom:6px; border:1px solid #3a506b;">
                                    <div style="font-weight:bold; ${badgeStyle} font-size:0.85rem;">⭐ ${s.ticker} a $${s.precio} (RSI: ${s.rsi})</div>
                                    <div style="font-size:0.75rem; color:#facc15; margin: 2px 0; cursor:pointer;" onclick="mostrarExplicacionEstado('${s.estado}')" title="Haz clic para ver explicación">📌 <span style="text-decoration:underline;">${s.estado}</span> 🔍</div>
                                    <div style="display:flex; gap:6px; margin-top:6px; flex-wrap:wrap;">
                                        <button onclick="agregarActivo('${s.ticker}')" style="font-size:0.68rem; padding:4px 6px;">+ Seguir</button>
                                        <button onclick="usarParaOperar('${s.ticker}', ${s.precio}, ${s.sl}, ${s.tp})" style="font-size:0.68rem; padding:4px 6px; background:#10b981; color:#fff;">💼 Operar</button>
                                        <a href="https://www.tradingview.com/chart/?symbol=${s.ticker}" target="_blank" style="background:#0b132b; color:#38bdf8; border:1px solid #3a506b; padding:3px 6px; border-radius:4px; text-decoration:none; font-weight:bold; font-size:0.68rem; text-align:center;">📈 TradingView</a>
                                        <button onclick="mostrarModal('${s.ticker}')" style="background:#3a506b; color:#fff; font-size:0.68rem; padding:4px 6px;">ℹ️ Info</button>
                                    </div>
                                </div>
                            `;
                        });
                    } else {
                        divSug.innerHTML = `<span style="font-size:0.8rem; color:#94a3b8;">Buscando configuraciones óptimas...</span>`;
                    }

                    renderizarGridMercado();

                    const lista = document.getElementById('lista-alertas');
                    if (alertas.length > 0) {
                        lista.innerHTML = alertas.map(a => `
                            <div class="alerta-item">
                                <div style="display:flex; justify-content:space-between; font-weight:bold; font-size:0.85rem;">
                                    <span>${a.symbol} - $${a.precio}</span><span style="font-size:0.70rem; color:#64748b;">${a.hora}</span>
                                </div>
                                <div style="font-size:0.78rem; margin-top:3px; cursor:pointer;" onclick="mostrarExplicacionEstado('${a.evento}')" title="Haz clic para ver explicación">🔔 <span style="text-decoration:underline;">${a.evento}</span> 🔍</div>
                                <div style="display:flex; gap:6px; margin-top:6px; flex-wrap:wrap;">
                                    <button onclick="agregarActivo('${a.symbol}')" style="font-size:0.68rem; padding:3px 6px;">+ Seguir Activo</button>
                                    <button onclick="usarParaOperar('${a.symbol}', ${a.precio}, ${a.sl || 0}, ${a.tp || 0})" style="font-size:0.68rem; padding:3px 6px; background:#10b981; color:#fff;">💼 Operar</button>
                                    <a href="https://www.tradingview.com/chart/?symbol=${a.symbol}" target="_blank" style="background:#0b132b; color:#38bdf8; border:1px solid #3a506b; padding:3px 6px; border-radius:4px; text-decoration:none; font-weight:bold; font-size:0.68rem; text-align:center;">📈 TradingView</a>
                                    <button onclick="mostrarModal('${a.symbol}')" style="background:#3a506b; color:#fff; font-size:0.68rem; padding:4px 6px;">ℹ️ Info</button>
                                </div>
                            </div>
                        `).join('');
                    } else {
                        lista.innerHTML = `<span style="font-size:0.8rem; color:#94a3b8;">Sin señales recientes.</span>`;
                    }
                } catch (e) { console.error(e); }
            }

            function renderizarGridMercado() {
                const grid = document.getElementById('grid-mercado');
                if (ordenActivosGlobal.length > 0 && Object.keys(mercadoGlobalData).length > 0) {
                    grid.innerHTML = '';
                    ordenActivosGlobal.forEach((ticker, idx) => {
                        const info = mercadoGlobalData[ticker];
                        if(!info) return;
                        const isBull = info.tendencia === 'ALZA';
                        let claseEntrada = 'entrada-wait';
                        if (info.estado_entrada.includes("FALSO QUIEBRE")) claseEntrada = 'entrada-warn';
                        else if (info.estado_entrada.includes("BUENA ENTRADA")) claseEntrada = 'entrada-ok';
                        else if (info.estado_entrada.includes("PREPARANDO")) claseEntrada = 'entrada-prep';
                        else if (info.estado_entrada.includes("REBOTE")) claseEntrada = 'entrada-rebote';

                        if(modoLista) {
                            grid.innerHTML += `
                                <div class="card">
                                    <div class="card-header" style="margin-bottom:0; width:120px;">
                                        <span class="ticker">${ticker} <span class="tf-badge">${info.timeframe}</span></span>
                                    </div>
                                    <div class="price" style="width:75px;">$${info.precio}</div>
                                    <div class="${claseEntrada}" style="width:160px; text-align:center; margin-bottom:0;" onclick="mostrarExplicacionEstado('${info.estado_entrada}')" title="Haz clic para explicación">${info.estado_entrada} 🔍</div>
                                    
                                    <div class="sparkline-container" style="width:80px; height:25px; margin-top:0;">
                                        <svg width="100%" height="25" viewBox="0 0 100 35" preserveAspectRatio="none">
                                            <polyline fill="none" stroke="${info.sparkline_color}" stroke-width="2" points="${info.sparkline}" />
                                        </svg>
                                    </div>

                                    <div class="list-actions-bar">
                                        <button onclick="usarParaOperar('${ticker}', ${info.precio}, ${info.soporte_tecnico}, ${info.tp_tecnico})" style="font-size:0.68rem; background:#10b981; color:#fff; padding:4px 6px;">💼 Operar</button>
                                        <a href="https://www.tradingview.com/chart/?symbol=${ticker}" target="_blank" style="background:#0b132b; color:#38bdf8; border:1px solid #3a506b; padding:3px 6px; border-radius:4px; text-decoration:none; font-weight:bold; font-size:0.68rem;">📈 TradingView</a>
                                        <button onclick="mostrarModal('${ticker}')" style="background:#3a506b; color:#fff; font-size:0.68rem; padding:4px 6px;">ℹ️ Info</button>
                                        <button class="btn-remove" onclick="eliminarActivo('${ticker}')" title="Eliminar">✕</button>
                                    </div>
                                </div>
                            `;
                        } else {
                            grid.innerHTML += `
                                <div class="card">
                                    <div class="card-top-toolbar">
                                        <span style="font-size:0.7rem; color:#38bdf8; font-weight:bold;">Prioridad #${idx + 1}</span>
                                        <button class="btn-remove" onclick="eliminarActivo('${ticker}')" title="Eliminar">✕ Eliminar</button>
                                    </div>

                                    <div class="card-header">
                                        <span class="ticker">${ticker} <span class="tf-badge">${info.timeframe}</span></span>
                                        <span class="badge ${isBull ? 'bullish' : 'bearish'}">${info.tendencia}</span>
                                    </div>
                                    <div class="price">$${info.precio}</div>
                                    <div class="${claseEntrada}" onclick="mostrarExplicacionEstado('${info.estado_entrada}')" title="Haz clic para entender este estado">${info.estado_entrada} 🔍</div>
                                    
                                    <div class="sparkline-container">
                                        <svg width="100%" height="35" viewBox="0 0 100 35" preserveAspectRatio="none">
                                            <polyline fill="none" stroke="${info.sparkline_color}" stroke-width="2" points="${info.sparkline}" />
                                        </svg>
                                    </div>

                                    <div class="levels-box">
                                        <div class="stat"><span>🛡️ Soporte (SL):</span> <span class="sl-text">$${info.soporte_tecnico}</span></div>
                                        <div class="stat"><span>🎯 TP Técnico:</span> <span class="tp-text">$${info.tp_tecnico}</span></div>
                                        <div class="stat" style="margin-top:6px;"><span>📊 RSI (14):</span> <span style="font-weight:bold; color:${info.rsi >= 70 ? '#ef4444' : (info.rsi <= 30 ? '#c084fc' : '#38bdf8')}">${info.rsi}</span></div>
                                        <div class="stat"><span>🌊 Macro (1D):</span> <span style="font-weight:bold; color:${info.tendencia_macro === 'ALZA' ? '#4ade80' : '#f87171'}">${info.tendencia_macro}</span></div>
                                        <div class="stat"><span>📈 Volumen:</span> <span style="font-weight:bold; color:${info.vol_valido ? '#4ade80' : '#f87171'}">${info.vol_valido ? 'ÓPTIMO' : 'BAJO'}</span></div>
                                    </div>

                                    <div style="display:flex; gap:6px; margin-top:8px; flex-wrap:wrap;">
                                        <button onclick="usarParaOperar('${ticker}', ${info.precio}, ${info.soporte_tecnico}, ${info.tp_tecnico})" style="flex:1; font-size:0.72rem; background:#10b981; color:#fff; padding:5px;">💼 Operar</button>
                                        <a href="https://www.tradingview.com/chart/?symbol=${ticker}" target="_blank" style="flex:1; background:#0b132b; color:#38bdf8; border:1px solid #3a506b; padding:4px; border-radius:4px; text-decoration:none; font-weight:bold; font-size:0.72rem; text-align:center; display:inline-block;">📈 TradingView</a>
                                        <button onclick="mostrarModal('${ticker}')" style="background:#3a506b; color:#fff; font-size:0.72rem; padding:4px 6px;">ℹ️ Info</button>
                                    </div>
                                </div>
                            `;
                        }
                    });
                } else {
                    grid.innerHTML = `<p style="color:#94a3b8;">Sin activos bajo monitoreo.</p>`;
                }
            }

            actualizarApp(true);
            iniciarSSE();
        </script>
    </body>
    </html>
    """


if __name__ == "__main__":
  port = int(os.environ.get("PORT", 8000))
  uvicorn.run(app, host="0.0.0.0", port=port)
