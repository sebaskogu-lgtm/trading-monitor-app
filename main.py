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

# CACHÉ EN MEMORIA CENTRALIZADA
APP_CONFIG = {
    "mercado_actual": "NY",
    "datos": {
        "NY": {
            "activos": ["SPY", "QQQ", "NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "BTC-USD"],
            "cartera": []
        },
        "LONDRES": {
            "activos": ["SHEL.L", "AZN.L", "ULVR.L", "HSBA.L", "BP.L", "GSK.L", "RIO.L", "BARC.L", "LLOY.L", "VOD.L"],
            "cartera": []
        },
        "ASIA": {
            "activos": ["7203.T", "6758.T", "7974.T", "9984.T", "8306.T", "6861.T", "6501.T", "4063.T", "6902.T", "8035.T"],
            "cartera": []
        }
    }
}

# Historial aislado por mercado
historial_alertas = {"NY": [], "LONDRES": [], "ASIA": []}

def get_db_connection():
  if not DATABASE_URL:
    return None
  try:
    url = DATABASE_URL + "?sslmode=require" if "?" not in DATABASE_URL else DATABASE_URL
    return psycopg2.connect(url)
  except Exception as e:
    print("❌ ERROR DE CONEXIÓN A SUPABASE:", e)
    return None

def init_db():
  global APP_CONFIG
  conn = get_db_connection()
  if not conn:
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
    cursor.execute("SELECT mercado_actual, datos FROM configuracion WHERE id=1;")
    row = cursor.fetchone()
    if not row:
      cursor.execute(
          "INSERT INTO configuracion (id, mercado_actual, datos) VALUES (1, %s, %s);",
          (APP_CONFIG["mercado_actual"], json.dumps(APP_CONFIG["datos"]))
      )
    else:
      APP_CONFIG["mercado_actual"] = row[0] or "NY"
      if row[1]:
        APP_CONFIG["datos"] = json.loads(row[1])
    conn.commit()
    cursor.close()
    conn.close()
  except Exception as e:
    print("❌ Error ejecutando init_db:", e)

init_db()

def db_save_all_data():
  conn = get_db_connection()
  if not conn:
    return
  try:
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE configuracion SET mercado_actual = %s, datos = %s WHERE id=1;",
        (APP_CONFIG["mercado_actual"], json.dumps(APP_CONFIG["datos"]))
    )
    conn.commit()
    cursor.close()
    conn.close()
  except Exception as e:
    pass

CATALOGO_TICKERS = {
    # Wall Street (NY)
    "AAPL": {"nombre": "Apple Inc.", "desc": "Tecnología de consumo.", "estrategia": "Ruptura de máximos SMA 9/21."},
    "MSFT": {"nombre": "Microsoft Corp", "desc": "Cloud y software.", "estrategia": "Tendencia alcista diaria."},
    "AMZN": {"nombre": "Amazon", "desc": "E-commerce y web services.", "estrategia": "Rebotes en soportes."},
    "NVDA": {"nombre": "NVIDIA", "desc": "Semiconductores IA.", "estrategia": "Alta volatilidad, buscar volumen."},
    "TSLA": {"nombre": "Tesla Inc.", "desc": "Vehículos eléctricos.", "estrategia": "Atención a sobreventa RSI."},
    "SPY": {"nombre": "S&P 500 ETF", "desc": "Índice principal EE.UU.", "estrategia": "Termómetro del mercado."},
    "QQQ": {"nombre": "Nasdaq 100", "desc": "Tecnológicas EE.UU.", "estrategia": "Cruce de medias móviles."},
    "BTC-USD": {"nombre": "Bitcoin USD", "desc": "Criptomoneda.", "estrategia": "Niveles psicológicos 24/7."},
    # Londres (LSE)
    "SHEL.L": {"nombre": "Shell plc", "desc": "Energía global.", "estrategia": "Precios de petróleo."},
    "AZN.L": {"nombre": "AstraZeneca", "desc": "Biofarmacéutico.", "estrategia": "Inversión defensiva."},
    "ULVR.L": {"nombre": "Unilever", "desc": "Bienes de consumo.", "estrategia": "Operativa de rango."},
    "HSBA.L": {"nombre": "HSBC", "desc": "Banca internacional.", "estrategia": "Tipos de interés."},
    "BP.L": {"nombre": "BP p.l.c.", "desc": "Producción energética.", "estrategia": "Correlación crudo Brent."},
    "RIO.L": {"nombre": "Rio Tinto", "desc": "Minería global.", "estrategia": "Demanda metales industriales."},
    # Asia (Tokio)
    "7203.T": {"nombre": "Toyota Motor", "desc": "Automotriz.", "estrategia": "Flujos institucionales."},
    "6758.T": {"nombre": "Sony Group", "desc": "Electrónica y cine.", "estrategia": "Quiebres de resistencia."},
    "7974.T": {"nombre": "Nintendo", "desc": "Videojuegos.", "estrategia": "Alta volatilidad estacional."},
    "9984.T": {"nombre": "SoftBank Group", "desc": "Inversión tecnológica.", "estrategia": "Correlación tech global."},
    "8306.T": {"nombre": "Mitsubishi UFJ", "desc": "Banca japonesa.", "estrategia": "Política Banco de Japón."},
    "6501.T": {"nombre": "Hitachi", "desc": "Infraestructura digital.", "estrategia": "Contratos de largo plazo."}
}

POOLS_ESCANER = {
    "NY": ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "NFLX", "AMD", "SPY", "QQQ"],
    "LONDRES": ["SHEL.L", "AZN.L", "ULVR.L", "HSBA.L", "BP.L", "GSK.L", "RIO.L", "BARC.L", "LLOY.L", "VOD.L"],
    "ASIA": ["7203.T", "6758.T", "7974.T", "9984.T", "8306.T", "6861.T", "6501.T", "4063.T", "6902.T", "8035.T"]
}

timeframe_actual = "1h"
estado_mercado = {}
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
  mercado_actual = APP_CONFIG["mercado_actual"]
  if mercado_actual == "LONDRES":
    tz_str = "Europe/London"
    nombre_mercado = "Londres (LSE)"
    open_h, open_m, close_h, close_m = 8, 0, 16, 30
    has_lunch = False
  elif mercado_actual == "ASIA":
    tz_str = "Asia/Tokyo"
    nombre_mercado = "Asia (Tokio)"
    open_h, open_m, close_h, close_m = 9, 0, 15, 30
    has_lunch = True
  else:
    tz_str = "America/New_York"
    nombre_mercado = "Nueva York (NYSE)"
    open_h, open_m, close_h, close_m = 9, 30, 16, 0
    has_lunch = False

  tz = pytz.timezone(tz_str)
  now = datetime.now(tz)
  
  # Base datetimes for today
  m_open = now.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
  m_close = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
  
  # Fin de semana
  if now.weekday() >= 5:
    days_ahead = 7 - now.weekday()
    target = m_open + timedelta(days=days_ahead)
    return "🔴 CERRADO (Fin de semana)", target.timestamp(), tz_str, "Abre"

  if now < m_open:
    return "🔴 CERRADO (Pre-apertura)", m_open.timestamp(), tz_str, "Abre"
  elif now >= m_close:
    days_ahead = 3 if now.weekday() == 4 else 1
    target = m_open + timedelta(days=days_ahead)
    txt_prefix = "Abre el lunes" if now.weekday() == 4 else "Abre mañana"
    return "🔴 CERRADO", target.timestamp(), tz_str, txt_prefix
  elif has_lunch:
    lunch_start = now.replace(hour=11, minute=30, second=0, microsecond=0)
    lunch_end = now.replace(hour=12, minute=30, second=0, microsecond=0)
    if lunch_start <= now < lunch_end:
      return "☕ RECESO (Almuerzo)", lunch_end.timestamp(), tz_str, "Vuelve"
  
  return "🟢 ABIERTO", m_close.timestamp(), tz_str, "Cierra"

def obtener_config_tf(tf: str):
  if tf == "4h": return "60d", "60m"
  if tf == "1d": return "6mo", "1d"
  return "1mo", "1h"

def get_bandera(mercado):
    if mercado == "NY": return "🇺🇸 NY"
    if mercado == "LONDRES": return "🇬🇧 LSE"
    return "🇯🇵 TSE"

def procesar_ticker(symbol, tf_local, mercado):
  ahora = time.time()
  if symbol in cache_yf and cache_yf[symbol]["tf"] == tf_local and (ahora - cache_yf[symbol]["time"] < 35):
    return cache_yf[symbol]["data"]

  periodo, intervalo = obtener_config_tf(tf_local)
  try:
    tendencia_macro = "ALZA"
    if tf_local in ["1h", "4h"]:
      try:
        df_macro = yf.download(tickers=symbol, period="6mo", interval="1d", progress=False)
        if not df_macro.empty:
          if hasattr(df_macro.columns, "nlevels") and df_macro.columns.nlevels > 1: df_macro.columns = df_macro.columns.get_level_values(0)
          u = df_macro.iloc[-1]
          sma9_m = df_macro["Close"].rolling(9).mean().iloc[-1]
          sma21_m = df_macro["Close"].rolling(21).mean().iloc[-1]
          if not pd.isna(sma9_m) and not pd.isna(sma21_m):
            tendencia_macro = "ALZA" if sma9_m > sma21_m else "BAJA"
      except: pass

    df = yf.download(tickers=symbol, period=periodo, interval=intervalo, progress=False)
    if df.empty: return None
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1: df.columns = df.columns.get_level_values(0)

    if tf_local == "4h" and len(df) >= 4:
      df = df.resample("4h").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna()

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
      df["RSI"] = 100 - (100 / (1 + (gain / loss)))
      rsi_val = round(float(df["RSI"].iloc[-1]), 1) if not pd.isna(df["RSI"].iloc[-1]) else 50.0

      vol_valido = True
      if "Volume" in df.columns:
        df["Vol_SMA_20"] = df["Volume"].rolling(window=20).mean()
        vp = float(df["Vol_SMA_20"].iloc[-1])
        if vp > 0: vol_valido = float(df["Volume"].iloc[-1]) >= (1.5 * vp)

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
      dif = max_tramo - min_tramo
      fib_500 = round(max_tramo - (dif * 0.500), 2)
      fib_618 = round(max_tramo - (dif * 0.618), 2)
      en_zona_fib = (fib_618 * 0.99) <= precio <= (fib_500 * 1.01)

      riesgo = precio - soporte_tecnico
      tp_tecnico = round(precio + (riesgo * 2), 2) if riesgo > 0 else round(precio * 1.02, 2)
      riesgo_macro_str = " | ⚠️ Macro BAJA" if tendencia_macro == "BAJA" else ""

      if precio > resistencia and tendencia == "ALZA":
        if rsi_val >= 70.0: estado_entrada = f"⚠️ SOBRECOMPRADO (Riesgo | RSI {rsi_val})"
        elif not vol_valido: estado_entrada = f"⚠️ FALSO QUIEBRE (Bajo Vol | RSI {rsi_val})"
        elif en_zona_fib: estado_entrada = f"🟢 BUENA ENTRADA (Quiebre+Fib){riesgo_macro_str}"
        else: estado_entrada = f"🟢 BUENA ENTRADA (Quiebre){riesgo_macro_str}"
      elif 0 < ((resistencia - precio) / precio * 100) <= 1.2 and tendencia == "ALZA":
        estado_entrada = f"⏳ PREPARANDO {'(Apoyo Fib)' if en_zona_fib else 'RUPTURA'} (RSI {rsi_val}){riesgo_macro_str}"
      elif rsi_val <= 30.0 and en_zona_fib:
        estado_entrada = f"💥 REBOTE EN ZONA (Sobrevendido){riesgo_macro_str}"
      elif rsi_val <= 30.0:
        estado_entrada = f"📉 SOBREVENDIDO (Esperar giro | RSI {rsi_val})"
      else:
        estado_entrada = f"⏳ ESPERAR (RSI {rsi_val})"

      upre = df["Close"].tail(15).tolist()
      mp, Map = min(upre), max(upre)
      rp = Map - mp if Map != mp else 1
      sparkline = " ".join([f"{round((i/14)*100,1)},{round(35-((v-mp)/rp)*30,1)}" for i, v in enumerate(upre)])

      meta_info = CATALOGO_TICKERS.get(symbol, {"nombre": symbol, "desc": "Activo global.", "estrategia": "Análisis técnico estándar."})

      resultado = {
          "symbol": symbol, "nombre": meta_info["nombre"], "descripcion": meta_info["desc"],
          "estrategia_explicacion": meta_info["estrategia"], "timeframe": tf_local.upper(),
          "precio": precio, "resistencia": resistencia, "soporte_tecnico": soporte_tecnico,
          "tp_tecnico": tp_tecnico, "sma9": sma9, "sma21": sma21, "rsi": rsi_val,
          "tendencia": tendencia, "tendencia_macro": tendencia_macro, "vol_valido": vol_valido,
          "estado_entrada": estado_entrada, "atr": atr_medio, "fib_50": fib_500, "fib_618": fib_618,
          "en_zona_fib": en_zona_fib, "hora": hora, "sparkline": sparkline,
          "sparkline_color": "#4ade80" if tendencia == "ALZA" else "#f87171",
          "bandera": get_bandera(mercado)
      }
      cache_yf[symbol] = {"tf": tf_local, "time": ahora, "data": resultado}
      return resultado
  except Exception: pass
  return None

def escaneo_autonomo():
  global recomendaciones_escaner
  while True:
    try:
      m_act = APP_CONFIG["mercado_actual"]
      pool = POOLS_ESCANER.get(m_act, POOLS_ESCANER["NY"])
      with ThreadPoolExecutor(max_workers=5) as executor:
        resultados = [r for r in executor.map(lambda s: procesar_ticker(s, timeframe_actual, m_act), pool) if r]
      
      ops = [r for r in resultados if "BUENA ENTRADA" in r["estado_entrada"] or "PREPARANDO" in r["estado_entrada"] or "REBOTE" in r["estado_entrada"]]
      ops.sort(key=lambda x: 0 if "BUENA ENTRADA" in x["estado_entrada"] else (1 if "REBOTE" in x["estado_entrada"] else 2))
      
      recomendaciones_escaner = [{"ticker": r["symbol"], "precio": r["precio"], "tp": r["tp_tecnico"], "sl": r["soporte_tecnico"], "rsi": r["rsi"], "estado": r["estado_entrada"], "bandera": r["bandera"]} for r in ops[:5]]
    except Exception as e:
      pass
    time.sleep(120)

def procesar_lote_mercado():
  global estado_mercado
  m_act = APP_CONFIG["mercado_actual"]
  lista = APP_CONFIG["datos"][m_act]["activos"]
  
  with ThreadPoolExecutor(max_workers=5) as executor:
    resultados = list(executor.map(lambda s: procesar_ticker(s, timeframe_actual, m_act), lista))

  nuevo_estado = {}
  for r in resultados:
    if r:
      sym = r["symbol"]
      nuevo_estado[sym] = r
      if "BUENA ENTRADA" in r["estado_entrada"] or "REBOTE EN ZONA" in r["estado_entrada"]:
        _registrar_alerta(sym, f"🟢 ALERTA ({r['estado_entrada']}) | TP: ${r['tp_tecnico']}", r["precio"], r["hora"], r["soporte_tecnico"], r["tp_tecnico"], m_act)
      _evaluar_cartera(sym, r["precio"], r["sma9"], r["sma21"], r["soporte_tecnico"], r["atr"], r["hora"], m_act)
  
  estado_mercado = nuevo_estado
  notificar_suscriptores()

def analizar_mercado():
  while True:
    procesar_lote_mercado()
    time.sleep(INTERVALO_SEGUNDOS)

def _forzar_actualizacion():
  procesar_lote_mercado()

def _evaluar_cartera(symbol, precio_actual, sma9, sma21, soporte_tecnico, atr, hora, mercado):
  cartera = APP_CONFIG["datos"][mercado]["cartera"]
  modificado = False
  for pos in cartera:
    if pos["ticker"] == symbol:
      p_compra = pos["precio_compra"]
      sl_user = pos["sl_usuario"]
      p_ganancia = ((precio_actual - p_compra) / p_compra) * 100
      estado_pos = "🔵 MANTENER"

      if sma9 < sma21:
        estado_pos = "⚠️ CRUCE BAJISTA"
        _registrar_alerta(symbol, "⚠️ CARTERA: Pérdida de impulso.", precio_actual, hora, soporte_tecnico, pos["tp_usuario"], mercado)
      elif p_ganancia >= 2.0:
        estado_pos = "🟢 EN GANANCIA"

      distancia_sl = precio_actual - sl_user
      mensaje_sl = "✔️ SL Correcto"

      if sl_user >= precio_actual: mensaje_sl = "❌ SL Inválido"
      elif p_ganancia >= 3.0 and sl_user < soporte_tecnico: mensaje_sl = f"📈 Trailing: Sube SL a soporte (${soporte_tecnico})"
      elif p_ganancia >= 1.5 and sl_user < p_compra: mensaje_sl = f"🔔 Asegura: Sube SL a BE (${p_compra})"
      elif distancia_sl < (atr * 0.5): mensaje_sl = "⚠️ SL MUY CORTO"
      elif sl_user < (soporte_tecnico * 0.95): mensaje_sl = "⚠️ SL MUY LEJOS"

      pos.update({"precio_actual": precio_actual, "pnl_porcentaje": round(p_ganancia, 2), "estado": estado_pos, "analisis_sl": mensaje_sl, "bandera": get_bandera(mercado)})
      modificado = True

  if modificado: db_save_all_data()

def _registrar_alerta(symbol, evento, precio, hora, sl, tp, mercado):
  global historial_alertas
  lista = historial_alertas[mercado]
  if not lista or lista[0]["symbol"] != symbol or lista[0]["evento"] != evento:
    lista.insert(0, {"symbol": symbol, "evento": evento, "precio": precio, "hora": hora, "sl": sl, "tp": tp, "bandera": get_bandera(mercado)})
    historial_alertas[mercado] = lista[:20]

threading.Thread(target=analizar_mercado, daemon=True).start()
threading.Thread(target=escaneo_autonomo, daemon=True).start()

def notificar_suscriptores():
  for q in SSE_SUBSCRIBERS:
    try: q.put_nowait("update")
    except Exception: pass

@app.get("/api/data")
def obtener_datos():
  m_act = APP_CONFIG["mercado_actual"]
  info = APP_CONFIG["datos"].get(m_act, {"activos": [], "cartera": []})
  estado, target_ts, tz_name, txt_prefix = obtener_info_horario()
  return {
      "mercado": estado_mercado,
      "alertas": historial_alertas[m_act],
      "cartera": info.get("cartera", []),
      "timeframe": timeframe_actual,
      "mercado_actual": m_act,
      "estado_horario": estado,
      "target_ts": target_ts,
      "prefix_cuenta": txt_prefix,
      "timezone": tz_name,
      "sugerencias": recomendaciones_escaner,
      "catalogo": CATALOGO_TICKERS,
      "activos_orden": info.get("activos", [])
  }

@app.get("/api/stream")
async def stream_endpoint(request: Request):
  q = asyncio.Queue()
  SSE_SUBSCRIBERS.append(q)
  async def event_generator():
    try:
      while True:
        if await request.is_disconnected(): break
        try:
          await asyncio.wait_for(q.get(), timeout=15.0)
          yield "data: update\n\n"
        except asyncio.TimeoutError:
          yield ":ping\n\n"
    except Exception: pass
    finally:
      if q in SSE_SUBSCRIBERS: SSE_SUBSCRIBERS.remove(q)
  return StreamingResponse(event_generator(), media_type="text/event-stream")

def auto_sufijo(symbol, mercado):
  if "-" in symbol or symbol in ["SPY", "QQQ"]: return symbol
  if mercado == "LONDRES" and not symbol.endswith(".L"): return f"{symbol}.L"
  if mercado == "ASIA" and not symbol.endswith(".T"): return f"{symbol}.T"
  return symbol

@app.post("/api/add")
async def agregar_activo(request: Request):
  data = await request.json()
  raw_symbol = data.get("ticker", "").strip().upper()
  if raw_symbol:
    m_act = APP_CONFIG["mercado_actual"]
    symbol = auto_sufijo(raw_symbol, m_act)
    activos = APP_CONFIG["datos"][m_act]["activos"]
    if symbol not in activos:
      activos.append(symbol)
      db_save_all_data()
      threading.Thread(target=_forzar_actualizacion, daemon=True).start()
  return {"status": "ok"}

@app.post("/api/remove")
async def eliminar_activo(request: Request):
  data = await request.json()
  symbol = data.get("ticker", "").strip().upper()
  if symbol:
    m_act = APP_CONFIG["mercado_actual"]
    activos = APP_CONFIG["datos"][m_act]["activos"]
    if symbol in activos:
      activos.remove(symbol)
      db_save_all_data()
      if symbol in estado_mercado: del estado_mercado[symbol]
      notificar_suscriptores()
  return {"status": "ok"}

@app.post("/api/reorder")
async def reordenar_activos(item: ReordenarModel):
  if item.activos:
    m_act = APP_CONFIG["mercado_actual"]
    APP_CONFIG["datos"][m_act]["activos"] = [s.strip().upper() for s in item.activos if s.strip()]
    db_save_all_data()
  return {"status": "ok"}

@app.post("/api/cartera/add")
def agregar_cartera(item: PosicionModel):
  m_act = APP_CONFIG["mercado_actual"]
  cartera = APP_CONFIG["datos"][m_act]["cartera"]
  ticker = auto_sufijo(item.ticker.strip().upper(), m_act)
  distancia_sl = abs(item.precio_compra - item.sl_usuario)
  acciones = round(item.riesgo_usd / distancia_sl, 2) if distancia_sl > 0 else 0
  
  cartera = [p for p in cartera if p["ticker"] != ticker]
  cartera.append({
      "ticker": ticker, "precio_compra": item.precio_compra, "sl_usuario": item.sl_usuario,
      "tp_usuario": item.tp_usuario, "riesgo_usd": item.riesgo_usd, "acciones": acciones,
      "inversion_total": round(acciones * item.precio_compra, 2), "timeframe": item.timeframe,
      "precio_actual": item.precio_compra, "pnl_porcentaje": 0.0, "estado": "🔵 MANTENER", "analisis_sl": "Analizando...",
      "bandera": get_bandera(m_act)
  })
  APP_CONFIG["datos"][m_act]["cartera"] = cartera
  db_save_all_data()
  threading.Thread(target=_forzar_actualizacion, daemon=True).start()
  return {"status": "ok"}

@app.post("/api/cartera/remove")
async def eliminar_cartera(request: Request):
  data = await request.json()
  ticker = data.get("ticker", "").strip().upper()
  if ticker:
    m_act = APP_CONFIG["mercado_actual"]
    APP_CONFIG["datos"][m_act]["cartera"] = [p for p in APP_CONFIG["datos"][m_act]["cartera"] if p["ticker"] != ticker]
    db_save_all_data()
    notificar_suscriptores()
  return {"status": "ok"}

@app.post("/api/timeframe")
def cambiar_timeframe(item: TimeframeModel):
  global timeframe_actual, estado_mercado
  if item.timeframe in ["1h", "4h", "1d"]:
    timeframe_actual = item.timeframe
    estado_mercado = {}
    threading.Thread(target=_forzar_actualizacion, daemon=True).start()
  return {"status": "ok"}

@app.post("/api/mercado")
def cambiar_mercado(item: MercadoModel):
  global estado_mercado
  if item.mercado in ["NY", "LONDRES", "ASIA"]:
    APP_CONFIG["mercado_actual"] = item.mercado
    db_save_all_data()
    estado_mercado = {}
    # Retorna rápido, el update asíncrono se lanza de fondo
    threading.Thread(target=_forzar_actualizacion, daemon=True).start()
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
            .reloj-sub { font-size: 0.85rem; color: #facc15; margin-top: 2px; font-weight:bold; }
            .live-indicator { display: inline-block; width: 8px; height: 8px; background: #22c55e; border-radius: 50%; margin-left: 6px; box-shadow: 0 0 8px #22c55e; animation: pulse 1.5s infinite; }
            @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.3; } 100% { opacity: 1; } }
            
            /* LOADER CENTRADO ESTILO PROFESIONAL */
            #pantalla-carga { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(11, 19, 43, 0.85); backdrop-filter: blur(4px); z-index: 9999; justify-content: center; align-items: center; flex-direction: column; }
            .spinner { border: 4px solid rgba(255,255,255,0.1); width: 50px; height: 50px; border-radius: 50%; border-left-color: #38bdf8; animation: spin 1s linear infinite; margin-bottom:15px; }
            @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
            
            .control-panel { max-width: 1200px; margin: 0 auto 16px auto; background: #1c2541; padding: 12px; border-radius: 10px; display: flex; gap: 8px; align-items: center; justify-content: center; flex-wrap: wrap; border: 1px solid #3a506b; }
            input[type="text"], input[type="number"], select { background: #0b132b; border: 1px solid #3a506b; color: #fff; padding: 8px; border-radius: 6px; font-size: 0.9rem; }
            input[type="text"] { width: 160px; text-transform: uppercase; }
            button { background: #38bdf8; color: #0b132b; border: none; padding: 8px 12px; font-weight: bold; border-radius: 6px; cursor: pointer; }
            button:hover { background: #7dd3fc; }
            
            .btn-mercado { background: #3a506b; color: #cbd5e1; border: 1px solid #3a506b; transition: all 0.2s; }
            .btn-mercado.active { background: #38bdf8; color: #0b132b; border-color: #7dd3fc; font-weight: 800; transform: scale(1.05); }

            .container { max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1.2fr; gap: 16px; }
            @media (max-width: 900px) { .container { grid-template-columns: 1fr; } .sidebar-prioritario { order: -1; } }
            
            .grid-activos { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 12px; }
            .grid-activos.list-view { grid-template-columns: 1fr; }
            
            .card { background: #1c2541; border-radius: 10px; padding: 14px; border: 1px solid #3a506b; position: relative; }
            .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
            .ticker { font-weight: bold; font-size: 1.1rem; display: flex; align-items: center; gap: 6px; }
            .price { font-size: 1.4rem; font-weight: 800; margin-bottom: 6px; }
            
            .card-top-toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; border-bottom: 1px solid #3a506b; padding-bottom: 6px; }
            
            .grid-activos.list-view .card { display: flex; flex-direction: row; align-items: center; justify-content: space-between; padding: 10px 14px; gap: 10px; flex-wrap: wrap; }
            .grid-activos.list-view .card-top-toolbar { display: none; }
            .grid-activos.list-view .card-header { margin-bottom: 0; width: 150px; }
            .grid-activos.list-view .price { font-size: 1.1rem; margin-bottom: 0; width: 75px; }
            .grid-activos.list-view .entrada-ok, .grid-activos.list-view .entrada-prep, .grid-activos.list-view .entrada-wait, .grid-activos.list-view .entrada-warn, .grid-activos.list-view .entrada-rebote { margin-bottom: 0; width: 160px; text-align: center; font-size: 0.72rem; cursor: pointer; }
            .grid-activos.list-view .sparkline-container { width: 80px; height: 25px; margin-top: 0; }
            .grid-activos.list-view .levels-box { display: none; }
            .grid-activos.list-view .list-actions-bar { display: flex; gap: 6px; align-items: center; }

            .badge { padding: 3px 6px; border-radius: 10px; font-size: 0.68rem; font-weight: bold; }
            .tf-badge { background: #3a506b; color: #cbd5e1; padding: 2px 5px; border-radius: 4px; font-size: 0.65rem; }
            .flag-badge { font-size: 0.75rem; background: #0b132b; padding: 2px 5px; border-radius: 4px; border: 1px solid #3a506b; margin-right: 4px; }
            
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
            .tv-btn { background:#2962ff; color:#fff; padding:4px 6px; border-radius:4px; text-decoration:none; font-weight:bold; font-size:0.68rem; text-align:center; display:inline-block; border:1px solid #1e40af; }
            
            .sparkline-container { margin-top: 8px; background: #0b132b; padding: 4px; border-radius: 6px; border: 1px solid #3a506b; text-align: center; }
            
            .feed-panel, .cartera-panel, .edu-panel { background: #1c2541; border-radius: 10px; padding: 14px; border: 1px solid #3a506b; margin-bottom: 16px; }
            .feed-title { font-size: 1rem; color: #38bdf8; margin-bottom: 10px; border-bottom: 1px solid #3a506b; padding-bottom: 6px; display: flex; justify-content: space-between; align-items: center; }
            .alerta-item { background: #0b132b; border-left: 4px solid #38bdf8; padding: 8px; margin-bottom: 6px; border-radius: 4px; }
            
            .metrics-bar { background: #0b132b; padding: 8px; border-radius: 6px; margin-bottom: 10px; display: flex; justify-content: space-around; font-size: 0.85rem; border: 1px solid #3a506b; }
            
            .manual-box { background: #0b132b; border: 1px solid #38bdf8; padding: 12px; border-radius: 8px; margin-top: 10px; font-size: 0.85rem; color: #cbd5e1; }
            .manual-box details { margin-bottom: 8px; background: #1c2541; border: 1px solid #3a506b; border-radius: 6px; padding: 6px; }
            .manual-box summary { font-weight: bold; color: #38bdf8; cursor: pointer; padding: 4px; }
            .manual-box p, .manual-box ul { margin-top: 8px; font-size: 0.8rem; line-height: 1.4; color: #f8fafc; }
            
            .input-group { display: flex; flex-direction: column; flex: 1; min-width: 80px; }
            .input-group label { font-size: 0.72rem; color: #38bdf8; margin-bottom: 3px; font-weight: bold; }

            #info-modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.8); z-index: 2000; justify-content: center; align-items: center; }
            .modal-content { background: #1c2541; padding: 20px; border-radius: 10px; border: 1px solid #38bdf8; width: 90%; max-width: 500px; color: #f8fafc; position: relative; }
            .modal-close { position: absolute; top: 10px; right: 15px; background: none; border: none; color: #ef4444; font-size: 1.2rem; cursor: pointer; }
        </style>
    </head>
    <body>
        <div id="pantalla-carga">
            <div class="spinner"></div>
            <h3 style="color:#38bdf8; margin:0;">Cambiando Bolsa...</h3>
            <p style="color:#cbd5e1; font-size:0.9rem;">Sincronizando activos, historial y relojes locales.</p>
        </div>

        <div style="display: flex; justify-content: space-between; align-items: center; max-width: 1200px; margin: 0 auto;">
            <div></div>
            <h1>📊 Trading Monitor Pro</h1>
            <div></div>
        </div>

        <div class="reloj-box">
            <div id="reloj-mercado" class="reloj">Cargando Horarios... <span class="live-indicator"></span></div>
            <div id="reloj-cuenta" class="reloj-sub">--:--:--</div>
            <div style="font-size: 0.9rem; margin-top: 5px; color: #4ade80;">Hora en Bolsa Local: <b id="reloj-hora-mercado">--:--:--</b></div>
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
                <div class="grid-activos" id="grid-mercado"><p style="color:#94a3b8;">⏳ Descargando base de datos...</p></div>

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
                        <div class="input-group"><label>ACTIVO</label><input type="text" id="c-ticker" style="width:100%; text-transform:uppercase;" /></div>
                        <div class="input-group"><label>ENTRADA ($)</label><input type="number" id="c-precio" style="width:100%;" step="any" /></div>
                        <div class="input-group"><label>STOP LOSS ($)</label><input type="number" id="c-sl" style="width:100%;" step="any" /></div>
                        <div class="input-group"><label>TAKE PROFIT ($)</label><input type="number" id="c-tp" style="width:100%;" step="any" /></div>
                        <div class="input-group"><label>RIESGO USD ($)</label><input type="number" id="c-riesgo" value="50" style="width:100%;" step="any" /></div>
                        <div style="display:flex; align-items:flex-end;"><button onclick="registrarPosicion()" style="height:38px; background:#10b981; color:#fff;">Guardar</button></div>
                    </div>
                    <div id="lista-cartera">Sin posiciones guardadas en esta bolsa.</div>
                </div>
            </div>
            
            <div class="sidebar-prioritario">
                <div class="feed-panel">
                    <div class="feed-title">🚨 Feed de Alertas en Vivo</div>
                    <div id="lista-alertas">Sin señales recientes.</div>
                </div>

                <div class="feed-panel">
                    <div class="feed-title">🤖 Escáner Dinámico</div>
                    <div id="lista-sugerencias" style="font-size:0.85rem; color:#cbd5e1;">Buscando Momentum y Rebotes...</div>
                </div>

                <div class="edu-panel">
                    <div class="feed-title">📖 Manual PRO Integral</div>
                    <button onclick="toggleManual()" style="width:100%; font-size:0.78rem; background:#3a506b; color:#fff; margin-bottom:8px; border:none; padding:8px; border-radius:4px; cursor:pointer; font-weight:bold;">📚 Desplegar / Ocultar Guía de Uso</button>
                    
                    <div id="box-manual" class="manual-box" style="display:none;">
                        <details>
                            <summary>🎯 Estrategias y Estados</summary>
                            <ul>
                                <li><b>🟢 Buena Entrada:</b> El precio ha roto una resistencia clave y el volumen (dinero real) acompaña la subida. Es el momento ideal para entrar.</li>
                                <li><b>⏳ Preparando:</b> El activo está retrocediendo saludablemente hacia un nivel de soporte (Fibonacci). No compres aún, espera el rebote.</li>
                                <li><b>💥 Rebote en Zona:</b> Caída fuerte (sobreventa, RSI < 30) que toca un piso técnico histórico. Oportunidad de entrada rápida con SL corto.</li>
                                <li><b>⚠️ Falso Quiebre:</b> Ruptura de precio pero sin volumen institucional. Trampa caza-bobos, evitar.</li>
                            </ul>
                        </details>
                        <details>
                            <summary>📊 Indicadores Utilizados</summary>
                            <ul>
                                <li><b>SMA 9 / 21:</b> Cruces de Medias Móviles para detectar impulsos. 9 > 21 es alcista.</li>
                                <li><b>RSI (14):</b> Mide el agotamiento. >70 Sobrecomprado (peligro), <30 Sobrevendido (oportunidad).</li>
                                <li><b>Macro (1D):</b> Filtro de seguridad que lee el gráfico diario para asegurar que no operes contra la tendencia principal.</li>
                            </ul>
                        </details>
                        <details>
                            <summary>💼 Gestión de Cartera (SL Audit)</summary>
                            <p>La calculadora de riesgo te dice cuántas acciones comprar en base a tu Riesgo en $. Luego, el sistema audita tu SL en vivo:</p>
                            <ul>
                                <li><b>SL Muy Corto / Lejos:</b> Te avisa si estás arriesgando de más o de menos según la volatilidad (ATR).</li>
                                <li><b>Sube SL a Soporte (Trailing):</b> Si vas ganando +3%, te sugiere subir el SL para asegurar la operación.</li>
                            </ul>
                        </details>
                        <details>
                            <summary>⏰ Horarios por Mercado</summary>
                            <ul>
                                <li>🇺🇸 <b>Nueva York (NYSE):</b> 09:30 a 16:00 hora NY.</li>
                                <li>🇬🇧 <b>Londres (LSE):</b> 08:00 a 16:30 hora Londres.</li>
                                <li>🇯🇵 <b>Asia (Tokio):</b> 09:00 a 15:30 hora Tokio (Receso: 11:30-12:30).</li>
                            </ul>
                        </details>
                    </div>
                </div>
            </div>
        </div>

        <div id="info-modal">
            <div class="modal-content">
                <button class="modal-close" onclick="cerrarModal()">✕</button>
                <h3 id="modal-titulo" style="color:#38bdf8; margin-top:0;">Información</h3>
                <div id="modal-body-content"></div>
            </div>
        </div>

        <script>
            let eventoSource = null;
            let ordenActivosGlobal = [];
            let modoLista = false;
            let mercadoGlobalData = {};
            let catalogoGlobal = {};
            let currentTz = "America/New_York";
            let targetTimestampGlobal = 0;
            let prefixCuentaGlobal = "";
            let ultimaAlertaVistaId = null;
            let mercadoEnUso = "NY";

            // Motor de Reloj de Alta Precisión (Javascript Puro)
            setInterval(() => {
                if(!currentTz) return;
                
                // Reloj Local del Mercado (Hora en la Bolsa)
                const options = { timeZone: currentTz, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false };
                try { document.getElementById('reloj-hora-mercado').innerText = new Intl.DateTimeFormat('es-ES', options).format(new Date()); } catch(e) {}
                
                // Cuenta regresiva ultra-precisa
                if(targetTimestampGlobal > 0) {
                    const ahoraMs = Date.now();
                    let diffMs = (targetTimestampGlobal * 1000) - ahoraMs;
                    if(diffMs < 0) diffMs = 0;
                    
                    const totalSeg = Math.floor(diffMs / 1000);
                    const dias = Math.floor(totalSeg / 86400);
                    const horas = Math.floor((totalSeg % 86400) / 3600);
                    const minutos = Math.floor((totalSeg % 3600) / 60);
                    const segundos = totalSeg % 60;
                    
                    let cuentaStr = `${prefixCuentaGlobal} en `;
                    if(dias > 0) cuentaStr += `${dias}d `;
                    cuentaStr += `${horas}h ${minutos}m ${segundos}s`;
                    
                    document.getElementById('reloj-cuenta').innerText = cuentaStr;
                }
            }, 1000);

            const explicacionesEstados = {
                "BUENA ENTRADA": { titulo: "🟢 Buena Entrada", porque: "Superó resistencia con volumen real.", resultado: "Compradores en control.", queHacer: "Operar usando la calculadora." },
                "PREPARANDO": { titulo: "⏳ Preparando", porque: "Retrocediendo a zona de soporte sano (Fibonacci).", resultado: "Descanso del precio.", queHacer: "Vigilar soporte para entrar en el rebote." },
                "REBOTE EN ZONA": { titulo: "💥 Rebote en Zona", porque: "Tocó piso técnico extremo en sobreventa.", resultado: "Posible giro alcista rápido.", queHacer: "Oportunidad agresiva. Usar SL ajustado." },
                "FALSO QUIEBRE": { titulo: "⚠️ Falso Quiebre", porque: "Rompió alza sin volumen de respaldo.", resultado: "Trampa de mercado.", queHacer: "No operar. Ignorar." },
                "SOBRECOMPRADO": { titulo: "⚠️ Sobrecomprado", porque: "Subió muy rápido verticalmente.", resultado: "Riesgo extremo de toma de ganancias.", queHacer: "Evitar compras nuevas." },
                "SOBREVENDIDO": { titulo: "📉 Sobrevendido", porque: "Caída vertical severa.", resultado: "Aún sin frenos ni suelo confirmado.", queHacer: "Esperar vela de giro técnico." },
                "ESPERAR": { titulo: "⏳ Esperar", porque: "Zona neutral media.", resultado: "Lateralidad y ruido.", queHacer: "Observar otros activos." }
            };

            function solicitarPermisoNotificaciones() {
                if (!("Notification" in window)) return;
                Notification.requestPermission().then(p => {
                    if(p === "granted") new Notification("Trading Monitor Pro", {body: "¡Notificaciones activadas con éxito!"});
                });
            }

            function dispararNotificacionEscritorio(titulo, cuerpo) {
                if ("Notification" in window && Notification.permission === "granted") new Notification(titulo, { body: cuerpo });
            }

            function toggleManual() {
                const el = document.getElementById('box-manual');
                el.style.display = el.style.display === 'none' ? 'block' : 'none';
            }

            function toggleVista() {
                modoLista = !modoLista;
                const grid = document.getElementById('grid-mercado');
                modoLista ? grid.classList.add('list-view') : grid.classList.remove('list-view');
                document.getElementById('btn-vista').innerText = modoLista ? "🔲 Cuadrícula" : "📋 Lista";
                renderizarGridMercado();
            }

            async function cambiarMercado(mercado) {
                // UI: Cambio inmediato y pantalla de carga
                document.getElementById('pantalla-carga').style.display = 'flex';
                document.getElementById('grid-mercado').innerHTML = '';
                document.getElementById('lista-alertas').innerHTML = '';
                document.getElementById('lista-sugerencias').innerHTML = '';
                
                // Petición no bloqueante
                await fetch('/api/mercado', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ mercado: mercado }) });
                
                // Pequeño delay artificial para asegurar que el backend arrancó el hilo
                setTimeout(async () => {
                    await actualizarApp();
                    document.getElementById('pantalla-carga').style.display = 'none';
                }, 1000);
            }

            function formatearLinkTV(ticker, mercado) {
                let sym = ticker;
                if(mercado === 'LONDRES') sym = 'LSE:' + ticker.replace('.L', '');
                else if(mercado === 'ASIA') sym = 'TSE:' + ticker.replace('.T', '');
                return `https://www.tradingview.com/chart/?symbol=${sym}`;
            }

            function mostrarModal(ticker) {
                const info = mercadoGlobalData[ticker] || (catalogoGlobal[ticker] ? { nombre: catalogoGlobal[ticker].nombre, descripcion: catalogoGlobal[ticker].desc, estrategia_explicacion: catalogoGlobal[ticker].estrategia } : { nombre: ticker, descripcion: "Activo detectado por volatilidad extrema.", estrategia_explicacion: "Monitoreo técnico base."});
                document.getElementById('modal-titulo').innerText = `${ticker} - ${info.nombre}`;
                document.getElementById('modal-body-content').innerHTML = `<p><b>Contexto:</b></p><p style="color:#cbd5e1; font-size:0.85rem;">${info.descripcion}</p><p><b>Estrategia de Operación:</b></p><p style="color:#cbd5e1; font-size:0.85rem;">${info.estrategia_explicacion}</p>`;
                document.getElementById('info-modal').style.display = 'flex';
            }

            function mostrarExplicacionEstado(claveEstado) {
                let enc = Object.entries(explicacionesEstados).find(([k, v]) => claveEstado.toUpperCase().includes(k))?.[1] || {titulo: "ℹ️ Evaluación Técnica", porque: "Sistema procesando velas.", resultado: "Lectura activa.", queHacer: "Vigilar niveles."};
                document.getElementById('modal-titulo').innerText = enc.titulo;
                document.getElementById('modal-body-content').innerHTML = `<p><b>Diagnóstico:</b></p><p style="color:#cbd5e1; font-size:0.85rem;">${enc.porque}</p><p><b>Resultado:</b></p><p style="color:#cbd5e1; font-size:0.85rem;">${enc.resultado}</p><p><b>Acción Sugerida:</b></p><p style="color:#4ade80; font-weight:bold;">👉 ${enc.queHacer}</p>`;
                document.getElementById('info-modal').style.display = 'flex';
            }

            function cerrarModal() { document.getElementById('info-modal').style.display = 'none'; }

            function iniciarSSE() {
                if (!!window.EventSource) {
                    eventoSource = new EventSource('/api/stream');
                    eventoSource.onmessage = e => { if(e.data === 'update') actualizarApp(); };
                }
            }

            async function cambiarTimeframe(tf) {
                document.getElementById('pantalla-carga').style.display = 'flex';
                await fetch('/api/timeframe', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ timeframe: tf }) });
                setTimeout(async () => {
                    await actualizarApp();
                    document.getElementById('pantalla-carga').style.display = 'none';
                }, 800);
            }

            async function agregarActivo(tickerParam = null) {
                const input = document.getElementById('new-ticker');
                const ticker = tickerParam || input.value.trim();
                if (!ticker) return;
                document.getElementById('pantalla-carga').style.display = 'flex';
                await fetch('/api/add', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker: ticker }) });
                if(!tickerParam) input.value = '';
                setTimeout(async () => { await actualizarApp(); document.getElementById('pantalla-carga').style.display = 'none'; }, 800);
            }

            function usarParaOperar(ticker, precio, sl, tp) {
                document.getElementById('c-ticker').value = ticker;
                document.getElementById('c-precio').value = precio;
                document.getElementById('c-sl').value = sl;
                document.getElementById('c-tp').value = tp;
                window.scrollTo({ top: 0, behavior: 'smooth' });
            }

            async function eliminarActivo(ticker) {
                document.getElementById('pantalla-carga').style.display = 'flex';
                await fetch('/api/remove', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker: ticker }) });
                setTimeout(async () => { await actualizarApp(); document.getElementById('pantalla-carga').style.display = 'none'; }, 500);
            }

            async function registrarPosicion() {
                const ticker = document.getElementById('c-ticker').value.trim();
                const precio = parseFloat(document.getElementById('c-precio').value);
                const sl = parseFloat(document.getElementById('c-sl').value);
                const tp = parseFloat(document.getElementById('c-tp').value);
                const riesgo = parseFloat(document.getElementById('c-riesgo').value) || 50;
                const tf = document.getElementById('select-tf').value;
                if (!ticker || isNaN(precio) || isNaN(sl) || isNaN(tp)) return;

                document.getElementById('pantalla-carga').style.display = 'flex';
                await fetch('/api/cartera/add', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ ticker: ticker, precio_compra: precio, sl_usuario: sl, tp_usuario: tp, riesgo_usd: riesgo, timeframe: tf })
                });
                document.getElementById('c-ticker').value = ''; document.getElementById('c-precio').value = '';
                document.getElementById('c-sl').value = ''; document.getElementById('c-tp').value = '';
                setTimeout(async () => { await actualizarApp(); document.getElementById('pantalla-carga').style.display = 'none'; }, 800);
            }

            async function eliminarPosicion(ticker) {
                document.getElementById('pantalla-carga').style.display = 'flex';
                await fetch('/api/cartera/remove', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker: ticker }) });
                setTimeout(async () => { await actualizarApp(); document.getElementById('pantalla-carga').style.display = 'none'; }, 500);
            }

            function obtenerPuntajeUrgencia(ticker) {
                const st = mercadoGlobalData[ticker]?.estado_entrada || "";
                if (st.includes("BUENA ENTRADA") || st.includes("REBOTE")) return 1;
                if (st.includes("PREPARANDO") || st.includes("SOBRE") || st.includes("FALSO")) return 2;
                return 3;
            }

            async function actualizarApp() {
                try {
                    const res = await fetch('/api/data');
                    const data = await res.json();
                    
                    mercadoEnUso = data.mercado_actual;
                    document.getElementById('select-tf').value = data.timeframe;
                    ordenActivosGlobal = data.activos_orden || [];
                    mercadoGlobalData = data.mercado || {};
                    catalogoGlobal = data.catalogo || {};
                    
                    // Reloj Engine Params
                    currentTz = data.timezone;
                    targetTimestampGlobal = data.target_ts;
                    prefixCuentaGlobal = data.prefix_cuenta;

                    ['NY', 'LONDRES', 'ASIA'].forEach(m => {
                        const btn = document.getElementById(`btn-mercado-${m}`);
                        if(btn) btn.classList.toggle('active', m === mercadoEnUso);
                    });

                    if(ordenActivosGlobal.length > 0 && Object.keys(mercadoGlobalData).length > 0) {
                        ordenActivosGlobal.sort((a, b) => obtenerPuntajeUrgencia(a) - obtenerPuntajeUrgencia(b));
                    }
                    
                    const datalist = document.getElementById('datalist-tickers');
                    if(datalist.children.length === 0 && data.catalogo) {
                        for(const [t, desc] of Object.entries(data.catalogo)) {
                            const opt = document.createElement('option');
                            opt.value = t; opt.textContent = desc.nombre;
                            datalist.appendChild(opt);
                        }
                    }

                    document.getElementById('reloj-mercado').innerHTML = `${data.estado_horario} <span class="live-indicator"></span>`;

                    // Alertas
                    const alertas = data.alertas || [];
                    if (alertas.length > 0) {
                        const ultima = alertas[0];
                        const idUnico = ultima.symbol + "_" + ultima.hora + "_" + ultima.precio;
                        if (ultimaAlertaVistaId !== null && ultimaAlertaVistaId !== idUnico) {
                            dispararNotificacionEscritorio(`🚨 Alerta: ${ultima.symbol}`, `${ultima.evento} - Precio: $${ultima.precio}`);
                        }
                        ultimaAlertaVistaId = idUnico;
                    }

                    // Cartera
                    let capitalTotal = 0, pnlSuma = 0;
                    const divCartera = document.getElementById('lista-cartera');
                    const cartera = data.cartera || [];
                    if (cartera.length > 0) {
                        divCartera.innerHTML = '';
                        cartera.forEach(p => {
                            capitalTotal += p.inversion_total || 0;
                            pnlSuma += p.pnl_porcentaje || 0;
                            const pnlColor = p.pnl_porcentaje >= 0 ? '#4ade80' : '#f87171';
                            let slColor = p.analisis_sl.includes("Correcto") ? '#4ade80' : '#f87171';
                            if (/Sube SL|Trailing|Asegura/.test(p.analisis_sl)) slColor = '#facc15';
                            divCartera.innerHTML += `
                                <div class="alerta-item" style="border-left-color: ${pnlColor};">
                                    <div style="display:flex; justify-content:space-between; font-weight:bold; font-size:0.9rem;">
                                        <span><span class="flag-badge">${p.bandera}</span> ${p.ticker} (${p.timeframe.toUpperCase()})</span>
                                        <span style="color:${pnlColor};">${p.pnl_porcentaje >= 0 ? '+' : ''}${p.pnl_porcentaje}%</span>
                                        <button onclick="eliminarPosicion('${p.ticker}')" style="background:none;color:#ef4444;border:none;cursor:pointer;">✕</button>
                                    </div>
                                    <div style="font-size:0.8rem; margin-top:4px;">Entrada: $${p.precio_compra} | Actual: $${p.precio_actual} | TP: $${p.tp_usuario}</div>
                                    <div style="font-size:0.8rem; color:#38bdf8; font-weight:bold;">Acciones: ${p.acciones || 0} ($${p.inversion_total || 0})</div>
                                    <div style="font-size:0.8rem; font-weight:bold; color:${slColor}; cursor:pointer;" onclick="mostrarExplicacionEstado('${p.analisis_sl}')">SL Audit: <span style="text-decoration:underline;">${p.analisis_sl}</span> 🔍</div>
                                </div>`;
                        });
                        const pnlPromedio = (pnlSuma / cartera.length).toFixed(2);
                        document.getElementById('resumen-cartera').innerHTML = `<span>Capital: <b>$${capitalTotal.toFixed(2)}</b></span><span>Rendimiento: <b style="color:${pnlPromedio >= 0 ? '#4ade80' : '#f87171'}">${pnlPromedio >= 0 ? '+' : ''}${pnlPromedio}%</b></span>`;
                    } else { 
                        divCartera.innerHTML = `<span style="font-size:0.8rem; color:#94a3b8;">Sin posiciones guardadas.</span>`;
                        document.getElementById('resumen-cartera').innerHTML = `<span>Capital: <b>$0.00</b></span><span>Rendimiento: <b>0.00%</b></span>`;
                    }

                    // Escaner
                    const divSug = document.getElementById('lista-sugerencias');
                    const sugerencias = data.sugerencias || [];
                    if(sugerencias.length > 0) {
                        divSug.innerHTML = sugerencias.map(s => {
                            const badgeStyle = s.estado.includes("REBOTE") ? "color:#c084fc;" : "color:#4ade80;";
                            const tvLink = formatearLinkTV(s.ticker, mercadoEnUso);
                            return `
                                <div style="background:#0b132b; padding:8px; border-radius:6px; margin-bottom:6px; border:1px solid #3a506b;">
                                    <div style="font-weight:bold; ${badgeStyle} font-size:0.85rem;"><span class="flag-badge">${s.bandera}</span> ${s.ticker} a $${s.precio}</div>
                                    <div style="font-size:0.75rem; color:#facc15; margin: 2px 0; cursor:pointer;" onclick="mostrarExplicacionEstado('${s.estado}')">📌 <span style="text-decoration:underline;">${s.estado}</span> 🔍</div>
                                    <div style="display:flex; gap:6px; margin-top:6px; flex-wrap:wrap;">
                                        <button onclick="agregarActivo('${s.ticker}')" style="font-size:0.68rem; padding:4px 6px;">+ Seguir</button>
                                        <button onclick="usarParaOperar('${s.ticker}', ${s.precio}, ${s.sl}, ${s.tp})" style="font-size:0.68rem; padding:4px 6px; background:#10b981; color:#fff;">💼 Operar</button>
                                        <a href="${tvLink}" target="_blank" class="tv-btn">📈 TV</a>
                                    </div>
                                </div>`;
                        }).join('');
                    } else { divSug.innerHTML = `<span style="font-size:0.8rem; color:#94a3b8;">Buscando oportunidades en ${mercadoEnUso}...</span>`; }

                    renderizarGridMercado();

                    // Lista de Alertas
                    const lista = document.getElementById('lista-alertas');
                    if (alertas.length > 0) {
                        lista.innerHTML = alertas.map(a => {
                            const tvLink = formatearLinkTV(a.symbol, mercadoEnUso);
                            return `
                            <div class="alerta-item">
                                <div style="display:flex; justify-content:space-between; font-weight:bold; font-size:0.85rem;">
                                    <span><span class="flag-badge">${a.bandera}</span> ${a.symbol} - $${a.precio}</span><span style="font-size:0.70rem; color:#64748b;">${a.hora}</span>
                                </div>
                                <div style="font-size:0.78rem; margin-top:3px; cursor:pointer;" onclick="mostrarExplicacionEstado('${a.evento}')">🔔 <span style="text-decoration:underline;">${a.evento}</span> 🔍</div>
                                <div style="display:flex; gap:6px; margin-top:6px;">
                                    <button onclick="agregarActivo('${a.symbol}')" style="font-size:0.68rem; padding:3px 6px;">+ Seguir</button>
                                    <button onclick="usarParaOperar('${a.symbol}', ${a.precio}, ${a.sl || 0}, ${a.tp || 0})" style="font-size:0.68rem; padding:3px 6px; background:#10b981; color:#fff;">💼 Op</button>
                                    <a href="${tvLink}" target="_blank" class="tv-btn" style="padding: 3px 6px;">📈 TV</a>
                                </div>
                            </div>`;
                        }).join('');
                    } else { lista.innerHTML = `<span style="font-size:0.8rem; color:#94a3b8;">Sin alertas en ${mercadoEnUso}.</span>`; }
                } catch (e) {}
            }

            function renderizarGridMercado() {
                const grid = document.getElementById('grid-mercado');
                if (ordenActivosGlobal.length > 0 && Object.keys(mercadoGlobalData).length > 0) {
                    grid.innerHTML = ordenActivosGlobal.map((ticker, idx) => {
                        const info = mercadoGlobalData[ticker];
                        if(!info) return '';
                        let claseEntrada = 'entrada-wait';
                        if (info.estado_entrada.includes("FALSO")) claseEntrada = 'entrada-warn';
                        else if (info.estado_entrada.includes("BUENA ENTRADA")) claseEntrada = 'entrada-ok';
                        else if (info.estado_entrada.includes("PREPARANDO")) claseEntrada = 'entrada-prep';
                        else if (info.estado_entrada.includes("REBOTE")) claseEntrada = 'entrada-rebote';

                        const tvLink = formatearLinkTV(ticker, mercadoEnUso);

                        if(modoLista) {
                            return `
                                <div class="card">
                                    <div class="card-header" style="margin-bottom:0; width:170px;">
                                        <span class="ticker"><span class="flag-badge">${info.bandera}</span> ${ticker} <span class="tf-badge">${info.timeframe}</span></span>
                                    </div>
                                    <div class="price" style="width:75px;">$${info.precio}</div>
                                    <div class="${claseEntrada}" style="width:160px;" onclick="mostrarExplicacionEstado('${info.estado_entrada}')">${info.estado_entrada} 🔍</div>
                                    <div class="sparkline-container" style="width:80px; height:25px; margin-top:0;">
                                        <svg width="100%" height="25" viewBox="0 0 100 35" preserveAspectRatio="none">
                                            <polyline fill="none" stroke="${info.sparkline_color}" stroke-width="2" points="${info.sparkline}" />
                                        </svg>
                                    </div>
                                    <div class="list-actions-bar">
                                        <button onclick="usarParaOperar('${ticker}', ${info.precio}, ${info.soporte_tecnico}, ${info.tp_tecnico})" style="font-size:0.68rem; background:#10b981; color:#fff; padding:4px 6px;">💼 Op</button>
                                        <a href="${tvLink}" target="_blank" class="tv-btn">📈 TV</a>
                                        <button onclick="mostrarModal('${ticker}')" style="background:#3a506b; color:#fff; font-size:0.68rem; padding:4px 6px;">ℹ️</button>
                                        <button class="btn-remove" onclick="eliminarActivo('${ticker}')">✕</button>
                                    </div>
                                </div>`;
                        } else {
                            return `
                                <div class="card">
                                    <div class="card-top-toolbar">
                                        <span style="font-size:0.7rem; color:#38bdf8; font-weight:bold;">Prioridad #${idx + 1}</span>
                                        <button class="btn-remove" onclick="eliminarActivo('${ticker}')">✕ Eliminar</button>
                                    </div>
                                    <div class="card-header">
                                        <span class="ticker"><span class="flag-badge">${info.bandera}</span> ${ticker} <span class="tf-badge">${info.timeframe}</span></span>
                                        <span class="badge ${info.tendencia === 'ALZA' ? 'bullish' : 'bearish'}">${info.tendencia}</span>
                                    </div>
                                    <div class="price">$${info.precio}</div>
                                    <div class="${claseEntrada}" onclick="mostrarExplicacionEstado('${info.estado_entrada}')">${info.estado_entrada} 🔍</div>
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
                                    </div>
                                    <div style="display:flex; gap:6px; margin-top:8px;">
                                        <button onclick="usarParaOperar('${ticker}', ${info.precio}, ${info.soporte_tecnico}, ${info.tp_tecnico})" style="flex:1; font-size:0.72rem; background:#10b981; color:#fff; padding:5px;">💼 Operar</button>
                                        <a href="${tvLink}" target="_blank" class="tv-btn" style="flex:1;">📈 TV</a>
                                        <button onclick="mostrarModal('${ticker}')" style="background:#3a506b; color:#fff; font-size:0.72rem; padding:4px 6px;">ℹ️ Info</button>
                                    </div>
                                </div>`;
                        }
                    }).join('');
                } else {
                    grid.innerHTML = `<p style="color:#94a3b8;">Aún no se han procesado los precios.</p>`;
                }
            }

            actualizarApp();
            iniciarSSE();
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
  port = int(os.environ.get("PORT", 8000))
  uvicorn.run(app, host="0.0.0.0", port=port)
