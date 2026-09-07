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

# Sobrescribir siempre para asegurar dependencias
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

# --- PERSISTENCIA SUPABASE (CON SSL REQUERIDO) ---
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()


def get_db_connection():
  if not DATABASE_URL:
    return None
  try:
    if "?" not in DATABASE_URL:
      url_con_ssl = DATABASE_URL + "?sslmode=require"
    elif "sslmode" not in DATABASE_URL:
      url_con_ssl = DATABASE_URL + "&sslmode=require"
    else:
      url_con_ssl = DATABASE_URL
    return psycopg2.connect(url_con_ssl)
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
                activos TEXT,
                cartera TEXT
            );
        """)
    cursor.execute("SELECT COUNT(*) FROM configuracion WHERE id=1;")
    if cursor.fetchone()[0] == 0:
      cursor.execute(
          "INSERT INTO configuracion (id, activos, cartera) VALUES (1, %s,"
          " %s);",
          (json.dumps(["QQQ", "SPY", "NVDA", "AAPL"]), json.dumps([])),
      )
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Base de datos conectada e inicializada correctamente.")
  except Exception as e:
    print("❌ Error ejecutando init_db:", e)


init_db()


def db_get(campo):
  conn = get_db_connection()
  if not conn:
    return ["QQQ", "SPY", "NVDA", "AAPL"] if campo == "activos" else []
  try:
    cursor = conn.cursor()
    cursor.execute(f"SELECT {campo} FROM configuracion WHERE id=1;")
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return json.loads(row[0]) if row and row[0] else []
  except Exception as e:
    print(f"❌ Error leyendo DB ({campo}):", e)
    return ["QQQ", "SPY", "NVDA", "AAPL"] if campo == "activos" else []


def db_set(campo, valor):
  conn = get_db_connection()
  if not conn:
    print("❌ No se pudo guardar: Sin conexión a base de datos.")
    return
  try:
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE configuracion SET {campo} = %s WHERE id=1;",
        (json.dumps(valor),),
    )
    conn.commit()
    cursor.close()
    conn.close()
  except Exception as e:
    print(f"❌ Error guardando DB ({campo}):", e)


# CATÁLOGO AMPLIADO (Acciones, Cripto y Divisas)
CATALOGO_TICKERS = {
    "AAPL": "Apple Inc. (Tecnología / Consumo)",
    "MSFT": "Microsoft Corporation (Software / Cloud)",
    "AMZN": "Amazon.com Inc. (E-Commerce / Cloud)",
    "NVDA": "NVIDIA Corporation (Semiconductores / IA)",
    "GOOGL": "Alphabet Inc. - Google (Buscador / Tech)",
    "META": "Meta Platforms Inc. (Redes Sociales / IA)",
    "TSLA": "Tesla Inc. (Vehículos Eléctricos / Energía)",
    "NFLX": "Netflix Inc. (Streaming / Entretenimiento)",
    "AMD": "Advanced Micro Devices (Semiconductores)",
    "COIN": "Coinbase Global Inc. (Cripto / Exchange)",
    "MSTR": "MicroStrategy Inc. (Bitcoin Treasury / Tech)",
    "PLTR": "Palantir Technologies (Software / IA Gobierno)",
    "SPY": "SPDR S&P 500 ETF Trust (Índice General)",
    "QQQ": "Invesco QQQ Trust (Índice Tecnológico Nasdaq)",
    "INTC": "Intel Corporation (Semiconductores)",
    "BA": "Boeing Company (Aeroespacial / Defensa)",
    "JPM": "JPMorgan Chase & Co. (Banca / Finanzas)",
    "DIS": "The Walt Disney Company (Entretenimiento)",
    "XOM": "Exxon Mobil Corporation (Energía / Petróleo)",
    "BABA": "Alibaba Group Holding (E-Commerce China)",
    "BTC-USD": "Bitcoin / Dólar Estadounidense (Criptomoneda)",
    "ETH-USD": "Ethereum / Dólar Estadounidense (Criptomoneda)",
    "EURUSD=X": "Euro / Dólar Estadounidense (Divisa Forex)",
    "GBPUSD=X": "Libra Esterlina / Dólar Estadounidense (Divisa Forex)",
}

TICKERS_ESCANER = [
    "AAPL",
    "MSFT",
    "AMZN",
    "NVDA",
    "GOOGL",
    "META",
    "TSLA",
    "NFLX",
    "AMD",
    "SPY",
    "QQQ",
]

# --- ESTADO EN MEMORIA Y CACHÉ TTL ---
timeframe_actual = "1h"
estado_mercado = {}
historial_alertas = []
recomendaciones_escaner = []
cache_yf = {}
SSE_SUBSCRIBERS = []


# --- MODELOS ---
class TimeframeModel(BaseModel):
  timeframe: str


class PosicionModel(BaseModel):
  ticker: str
  precio_compra: float
  sl_usuario: float
  tp_usuario: float
  riesgo_usd: float
  timeframe: str


# --- HORARIO NYSE ---
def obtener_info_horario():
  ny_tz = pytz.timezone("America/New_York")
  ny_time = datetime.now(ny_tz)

  if ny_time.weekday() > 4:
    dias_hasta_lunes = (7 - ny_time.weekday()) % 7
    if dias_hasta_lunes == 0:
      dias_hasta_lunes = 2
    proximo_lunes = ny_time + timedelta(days=dias_hasta_lunes)
    proximo_lunes = proximo_lunes.replace(
        hour=9, minute=30, second=0, microsecond=0
    )
    diff = proximo_lunes - ny_time
    horas, rem = divmod(int(diff.total_seconds()), 3600)
    minutos, _ = divmod(rem, 60)
    return (
        "🔴 CERRADO (Fin de semana)",
        f"Abre en {horas // 24} días y {horas % 24}h {minutos}m",
    )

  m_open = ny_time.replace(hour=9, minute=30, second=0, microsecond=0)
  m_close = ny_time.replace(hour=16, minute=0, second=0, microsecond=0)
  pre_close = ny_time.replace(hour=15, minute=30, second=0, microsecond=0)

  if ny_time < m_open:
    diff = m_open - ny_time
    seg = int(diff.total_seconds())
    h, r = divmod(seg, 3600)
    m, _ = divmod(r, 60)
    return "🔴 CERRADO (Pre-apertura)", f"Abre en {h}h {m}m"
  elif ny_time > m_close:
    proxima = m_open + timedelta(days=1)
    diff = proxima - ny_time
    seg = int(diff.total_seconds())
    h, r = divmod(seg, 3600)
    m, _ = divmod(r, 60)
    return "🔴 CERRADO", f"Abre mañana en {h}h {m}m"
  elif ny_time >= pre_close:
    diff = m_close - ny_time
    seg = int(diff.total_seconds())
    m, s = divmod(seg, 60)
    return "⚠️ PRE-CIERRE", f"Cierra en {m}m {s}s"
  else:
    diff = m_close - ny_time
    seg = int(diff.total_seconds())
    h, r = divmod(seg, 3600)
    m, _ = divmod(r, 60)
    return "🟢 ABIERTO (NYSE)", f"Cierra en {h}h {m}m"


# --- MOTOR DE ANÁLISIS ---
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

      ultima = df.iloc[-1]
      anterior = df.iloc[-2]

      precio = round(float(ultima["Close"]), 2)
      resistencia = round(float(anterior["Resistencia"]), 2)
      soporte_tecnico = round(float(anterior["Soporte_SL"]), 2)
      sma9 = round(float(ultima["SMA_9"]), 2)
      sma21 = round(float(ultima["SMA_21"]), 2)

      tendencia = "ALZA" if sma9 > sma21 else "BAJA"
      hora = datetime.now().strftime("%H:%M:%S")

      riesgo = precio - soporte_tecnico
      tp_tecnico = (
          round(precio + (riesgo * 2), 2)
          if riesgo > 0
          else round(precio * 1.02, 2)
      )

      distancia_resistencia = ((resistencia - precio) / precio) * 100
      if precio > resistencia and tendencia == "ALZA":
        estado_entrada = "🟢 BUENA ENTRADA (Quiebre)"
      elif 0 < distancia_resistencia <= 1.2 and tendencia == "ALZA":
        estado_entrada = "⏳ PREPARANDO RUPTURA"
      else:
        estado_entrada = "⏳ ESPERAR"

      ultimos_precios = df["Close"].tail(15).tolist()
      min_p, max_p = min(ultimos_precios), max(ultimos_precios)
      rango_p = max_p - min_p if max_p != min_p else 1
      points = []
      for idx, val in enumerate(ultimos_precios):
        x = round((idx / 14) * 100, 1)
        y = round(35 - ((val - min_p) / rango_p) * 30, 1)
        points.append(f"{x},{y}")
      sparkline_svg = " ".join(points)

      resultado = {
          "symbol": symbol,
          "timeframe": tf_local.upper(),
          "precio": precio,
          "resistencia": resistencia,
          "soporte_tecnico": soporte_tecnico,
          "tp_tecnico": tp_tecnico,
          "sma9": sma9,
          "sma21": sma21,
          "tendencia": tendencia,
          "estado_entrada": estado_entrada,
          "atr": atr_medio,
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
  global recomendaciones_escaner
  while True:
    buenas = []
    with ThreadPoolExecutor(max_workers=5) as executor:
      resultados = executor.map(
          lambda s: procesar_ticker(s, "1h"), TICKERS_ESCANER
      )

    for r in resultados:
      if r and (
          "BUENA ENTRADA" in r["estado_entrada"]
          or "PREPARANDO RUPTURA" in r["estado_entrada"]
      ):
        buenas.append({
            "ticker": r["symbol"],
            "precio": r["precio"],
            "tp": r["tp_tecnico"],
            "sl": r["soporte_tecnico"],
            "estado": r["estado_entrada"],
        })

    recomendaciones_escaner = buenas[:4]
    time.sleep(120)


def analizar_mercado():
  global estado_mercado, historial_alertas, timeframe_actual
  while True:
    lista_actual = db_get("activos")
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
        if "BUENA ENTRADA" in r["estado_entrada"]:
          _registrar_alerta(
              sym,
              f"🟢 SEÑAL DE COMPRA | Objetivo: ${r['tp_tecnico']}",
              r["precio"],
              r["hora"],
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
  cartera = db_get("cartera")
  modificado = False
  for pos in cartera:
    if pos["ticker"] == symbol:
      p_compra = pos["precio_compra"]
      sl_user = pos["sl_usuario"]

      p_ganancia = ((precio_actual - p_compra) / p_compra) * 100
      estado_pos = "🔵 MANTENER"

      if sma9 < sma21:
        estado_pos = "⚠️ GIRO A LA BAJA (Evaluar salida)"
        _registrar_alerta(
            symbol, f"⚠️ ALERTA CARTERA: Pérdida de impulso.", precio_actual, hora
        )
      elif p_ganancia >= 2.0:
        estado_pos = "🟢 EN GANANCIA"

      distancia_sl = precio_actual - sl_user
      mensaje_sl = "✔️ SL Correcto"
      if sl_user >= precio_actual:
        mensaje_sl = "❌ SL inválido (mayor al precio)"
      elif distancia_sl < (atr * 0.5):
        mensaje_sl = "⚠️ SL MUY CORTO (Riesgo de mecha)"
      elif sl_user < (soporte_tecnico * 0.95):
        mensaje_sl = "⚠️ SL MUY LEJOS (Riesgo alto)"

      pos["precio_actual"] = precio_actual
      pos["pnl_porcentaje"] = round(p_ganancia, 2)
      pos["estado"] = estado_pos
      pos["analisis_sl"] = mensaje_sl
      modificado = True

  if modificado:
    db_set("cartera", cartera)


def _registrar_alerta(symbol, evento, precio, hora):
  global historial_alertas
  if (
      not historial_alertas
      or historial_alertas[0]["symbol"] != symbol
      or historial_alertas[0]["evento"] != evento
  ):
    historial_alertas.insert(
        0, {"symbol": symbol, "evento": evento, "precio": precio, "hora": hora}
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


# --- API ---
@app.get("/api/data")
def obtener_datos():
  estado, cuenta_reg = obtener_info_horario()
  return {
      "mercado": estado_mercado,
      "alertas": historial_alertas,
      "cartera": db_get("cartera"),
      "timeframe": timeframe_actual,
      "horario": estado,
      "cuenta_regresiva": cuenta_reg,
      "sugerencias": recomendaciones_escaner,
      "catalogo": CATALOGO_TICKERS,
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
  try:
    data = await request.json()
    symbol = data.get("ticker", "").strip().upper()
  except Exception:
    symbol = ""

  if symbol:
    activos = db_get("activos")
    if symbol not in activos:
      activos.append(symbol)
      db_set("activos", activos)

      def _fetch_and_update():
        res = procesar_ticker(symbol, timeframe_actual)
        if res:
          estado_mercado[symbol] = res
          notificar_suscriptores()

      threading.Thread(target=_fetch_and_update, daemon=True).start()
  return {"status": "ok"}


@app.post("/api/remove")
async def eliminar_activo(request: Request):
  try:
    data = await request.json()
    symbol = data.get("ticker", "").strip().upper()
  except Exception:
    symbol = ""

  if symbol:
    activos = db_get("activos")
    if symbol in activos:
      activos.remove(symbol)
      db_set("activos", activos)
      if symbol in estado_mercado:
        del estado_mercado[symbol]
  return {"status": "ok"}


@app.post("/api/cartera/add")
def agregar_cartera(item: PosicionModel):
  cartera = db_get("cartera")
  ticker = item.ticker.strip().upper()

  distancia_sl = abs(item.precio_compra - item.sl_usuario)
  acciones_sugeridas = (
      round(item.riesgo_usd / distancia_sl, 2) if distancia_sl > 0 else 0
  )
  inversion_total = round(acciones_sugeridas * item.precio_compra, 2)

  cartera = [p for p in cartera if p["ticker"] != ticker]
  cartera.append({
      "ticker": ticker,
      "precio_compra": item.precio_compra,
      "sl_usuario": item.sl_usuario,
      "tp_usuario": item.tp_usuario,
      "riesgo_usd": item.riesgo_usd,
      "acciones": acciones_sugeridas,
      "inversion_total": inversion_total,
      "timeframe": item.timeframe,
      "precio_actual": item.precio_compra,
      "pnl_porcentaje": 0.0,
      "estado": "🔵 MANTENER",
      "analisis_sl": "Analizando...",
  })
  db_set("cartera", cartera)
  return {"status": "ok"}


@app.post("/api/cartera/remove")
async def eliminar_cartera(request: Request):
  try:
    data = await request.json()
    ticker = data.get("ticker", "").strip().upper()
  except Exception:
    ticker = ""

  if ticker:
    cartera = db_get("cartera")
    cartera = [p for p in cartera if p["ticker"] != ticker]
    db_set("cartera", cartera)
  return {"status": "ok"}


@app.post("/api/timeframe")
def cambiar_timeframe(item: TimeframeModel):
  global timeframe_actual, estado_mercado
  if item.timeframe in ["1h", "4h", "1d"]:
    timeframe_actual = item.timeframe
    estado_mercado = {}
  return {"status": "ok"}


# --- FRONTEND HTML ---
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
            .reloj-box { text-align: center; margin-bottom: 20px; }
            .reloj { font-weight: bold; font-size: 1rem; }
            .reloj-sub { font-size: 0.8rem; color: #94a3b8; margin-top: 2px; }
            
            .control-panel { max-width: 1200px; margin: 0 auto 16px auto; background: #1c2541; padding: 12px; border-radius: 10px; display: flex; gap: 8px; align-items: center; justify-content: center; flex-wrap: wrap; border: 1px solid #3a506b; }
            input[type="text"], input[type="number"], select { background: #0b132b; border: 1px solid #3a506b; color: #fff; padding: 8px; border-radius: 6px; font-size: 0.9rem; }
            input[type="text"] { width: 180px; text-transform: uppercase; }
            button { background: #38bdf8; color: #0b132b; border: none; padding: 8px 12px; font-weight: bold; border-radius: 6px; cursor: pointer; }
            button:hover { background: #7dd3fc; }
            
            .container { max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1fr; gap: 16px; }
            @media (max-width: 850px) { .container { grid-template-columns: 1fr; } }
            
            .grid-activos { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; }
            .card { background: #1c2541; border-radius: 10px; padding: 14px; border: 1px solid #3a506b; position: relative; }
            .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
            .ticker { font-weight: bold; font-size: 1.1rem; display: flex; align-items: center; gap: 6px; }
            .price { font-size: 1.4rem; font-weight: 800; margin-bottom: 6px; }
            
            .badge { padding: 3px 6px; border-radius: 10px; font-size: 0.68rem; font-weight: bold; }
            .tf-badge { background: #3a506b; color: #cbd5e1; padding: 2px 5px; border-radius: 4px; font-size: 0.65rem; }
            .bullish { background: rgba(34, 197, 94, 0.2); color: #4ade80; border: 1px solid #22c55e; }
            .bearish { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #ef4444; }
            .entrada-ok { background: rgba(34, 197, 94, 0.3); color: #4ade80; font-weight: bold; padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; display: inline-block; margin-bottom: 8px; }
            .entrada-prep { background: rgba(234, 179, 8, 0.2); color: #facc15; border: 1px solid #eab308; font-weight: bold; padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; display: inline-block; margin-bottom: 8px; }
            .entrada-wait { background: rgba(148, 163, 184, 0.1); color: #94a3b8; padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; display: inline-block; margin-bottom: 8px; }
            
            .stat { display: flex; justify-content: space-between; margin-top: 5px; font-size: 0.82rem; color: #cbd5e1; }
            .levels-box { background: #0b132b; padding: 8px; border-radius: 6px; margin-top: 6px; border: 1px solid #3a506b; }
            .sl-text { color: #f87171; font-weight: bold; }
            .tp-text { color: #4ade80; font-weight: bold; }
            .btn-remove { position: absolute; top: 10px; right: 10px; background: transparent; color: #ef4444; border: none; font-size: 1.1rem; cursor: pointer; }
            
            .sparkline-container { margin-top: 8px; background: #0b132b; padding: 4px; border-radius: 6px; border: 1px solid #3a506b; text-align: center; }
            
            .feed-panel, .cartera-panel, .edu-panel { background: #1c2541; border-radius: 10px; padding: 14px; border: 1px solid #3a506b; margin-bottom: 16px; }
            .feed-title { font-size: 1rem; color: #38bdf8; margin-bottom: 10px; border-bottom: 1px solid #3a506b; padding-bottom: 6px; display: flex; justify-content: space-between; align-items: center; }
            .alerta-item { background: #0b132b; border-left: 4px solid #38bdf8; padding: 8px; margin-bottom: 6px; border-radius: 4px; }
            
            .links-externos { display: flex; gap: 6px; margin-top: 8px; justify-content: center; font-size: 0.75rem; }
            .links-externos a { background: #0b132b; color: #38bdf8; border: 1px solid #3a506b; padding: 3px 6px; border-radius: 4px; text-decoration: none; font-weight: bold; }
            .links-externos a:hover { background: #38bdf8; color: #0b132b; }

            .edu-text { font-size: 0.82rem; color: #cbd5e1; line-height: 1.4; }
            .edu-text ul { padding-left: 16px; margin: 6px 0; }
            .metrics-bar { background: #0b132b; padding: 8px; border-radius: 6px; margin-bottom: 10px; display: flex; justify-content: space-around; font-size: 0.85rem; border: 1px solid #3a506b; }
        </style>
    </head>
    <body>
        <h1>📊 Trading Monitor Pro</h1>
        <div class="reloj-box">
            <div id="reloj-mercado" class="reloj">Cargando estado del mercado...</div>
            <div id="reloj-cuenta" class="reloj-sub">Sincronizando cronograma...</div>
        </div>
        
        <div class="control-panel">
            <div style="position: relative;">
                <input type="text" id="new-ticker" placeholder="Buscar Ticker (ej: BTC-USD)..." list="datalist-tickers" onkeydown="if(event.key==='Enter') agregarActivo()" autocomplete="off" />
                <datalist id="datalist-tickers"></datalist>
            </div>
            <button onclick="agregarActivo()">+ Seguir Activo</button>
            <select id="select-tf" onchange="cambiarTimeframe(this.value)">
                <option value="1h">1H (Hora - Intradiario)</option>
                <option value="4h">4H (Swing)</option>
                <option value="1d">1D (Diario)</option>
            </select>
        </div>

        <div class="container">
            <div>
                <div class="cartera-panel">
                    <div class="feed-title">
                        <span>💼 Mi Cartera y Gestión de Riesgo</span>
                    </div>
                    
                    <div class="metrics-bar" id="resumen-cartera">
                        <span>Capital Expuesto: <b>$0.00</b></span>
                        <span>Rendimiento Global: <b>0.00%</b></span>
                    </div>

                    <div style="display:flex; flex-wrap:wrap; gap:6px; margin-bottom:10px;">
                        <input type="text" id="c-ticker" placeholder="Activo" style="width:70px;" />
                        <input type="number" id="c-precio" placeholder="Entrada $" style="width:85px;" step="any" />
                        <input type="number" id="c-sl" placeholder="Tu SL $" style="width:85px;" step="any" />
                        <input type="number" id="c-tp" placeholder="Tu TP $" style="width:85px;" step="any" />
                        <input type="number" id="c-riesgo" placeholder="Riesgo $" style="width:80px;" value="50" step="any" title="Dinero máximo a perder en la operación" />
                        <button onclick="registrarPosicion()">Guardar Posición</button>
                    </div>
                    <div id="lista-cartera">Sin posiciones guardadas.</div>
                </div>

                <h3>Activos bajo Monitoreo Activo</h3>
                <div class="grid-activos" id="grid-mercado"><p style="color:#94a3b8;">⏳ Sincronizando con los servidores en tiempo real...</p></div>
            </div>
            
            <div>
                <div class="edu-panel">
                    <div class="feed-title">📖 Guía de Tiempos y Supervisión</div>
                    <div class="edu-text">
                        <b>Diferencias de Temporalidad:</b>
                        <ul>
                            <li><b>1H (Hora):</b> Para operar rápido (horas a pocos días). Requiere supervisión activa.</li>
                            <li><b>4H (Swing):</b> Para mantener días o semanas. Menos ruido diario.</li>
                            <li><b>1D (Diario):</b> Tendencia macro para semanas o meses.</li>
                        </ul>
                        <b>¿Cómo y cuándo supervisar?</b>
                        <ul>
                            <li><b>1H:</b> Revisa cada 30-60 min durante la sesión de Wall Street (9:30 - 16:00 EST).</li>
                            <li><b>4H / 1D:</b> Revisa 2 veces al día (apertura y cierre).</li>
                            <li><b>Vigila:</b> Que la SMA 9 no cruce por debajo de la SMA 21 y que el precio no rompa tu Stop Loss.</li>
                        </ul>
                    </div>
                </div>

                <div class="feed-panel">
                    <div class="feed-title">🤖 Escáner de Oportunidades & Rupturas</div>
                    <div id="lista-sugerencias" style="font-size:0.85rem; color:#cbd5e1;">Buscando rupturas y valores listos para entrar...</div>
                </div>

                <div class="feed-panel">
                    <div class="feed-title">🚨 Feed de Alertas en Tiempo Real</div>
                    <div id="lista-alertas">Sin señales recientes.</div>
                </div>
            </div>
        </div>

        <script>
            let eventoSource = null;

            function iniciarSSE() {
                if (!!window.EventSource) {
                    eventoSource = new EventSource('/api/stream');
                    eventoSource.onmessage = function(e) {
                        if(e.data === 'update') {
                            actualizarApp(false);
                        }
                    };
                }
            }

            async function cambiarTimeframe(tf) {
                document.getElementById('grid-mercado').innerHTML = '<p style="color:#38bdf8;">⏳ Cambiando temporalidad...</p>';
                await fetch('/api/timeframe', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ timeframe: tf })
                });
                actualizarApp(true);
            }

            async function agregarActivo(tickerParam = null) {
                const input = document.getElementById('new-ticker');
                const ticker = tickerParam || input.value.trim();
                if (!ticker) return;
                
                document.getElementById('grid-mercado').innerHTML += `<p style="color:#38bdf8; font-size:0.85rem;">⏳ Analizando ${ticker}...</p>`;
                
                await fetch('/api/add', {
                    method: 'POST', 
                    headers: {'Content-Type': 'application/json'}, 
                    body: JSON.stringify({ ticker: ticker })
                });
                if(!tickerParam) input.value = '';
                actualizarApp(true);
            }

            function usarSugerencia(ticker, precio, sl, tp) {
                document.getElementById('c-ticker').value = ticker;
                document.getElementById('c-precio').value = precio;
                document.getElementById('c-sl').value = sl;
                document.getElementById('c-tp').value = tp;
                window.scrollTo({ top: 0, behavior: 'smooth' });
            }

            async function eliminarActivo(ticker) {
                await fetch('/api/remove', {
                    method: 'POST', 
                    headers: {'Content-Type': 'application/json'}, 
                    body: JSON.stringify({ ticker: ticker })
                });
                actualizarApp(true);
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

            async function actualizarApp(forzarRender = false) {
                try {
                    const res = await fetch('/api/data');
                    const { mercado, alertas, cartera, timeframe, horario, cuenta_regresiva, sugerencias, catalogo } = await res.json();
                    
                    document.getElementById('select-tf').value = timeframe;
                    
                    const datalist = document.getElementById('datalist-tickers');
                    if(datalist.children.length === 0 && catalogo) {
                        for(const [t, desc] of Object.entries(catalogo)) {
                            const opt = document.createElement('option');
                            opt.value = t;
                            opt.textContent = desc;
                            datalist.appendChild(opt);
                        }
                    }

                    const relojEl = document.getElementById('reloj-mercado');
                    const cuentaEl = document.getElementById('reloj-cuenta');
                    relojEl.innerHTML = horario;
                    cuentaEl.innerHTML = cuenta_regresiva;
                    relojEl.style.color = (horario.includes("CERRADO") || horario.includes("PRE-CIERRE")) ? '#f87171' : '#4ade80';

                    let capitalTotal = 0;
                    let pnlSuma = 0;

                    const divCartera = document.getElementById('lista-cartera');
                    if (cartera.length > 0) {
                        divCartera.innerHTML = '';
                        cartera.forEach(p => {
                            capitalTotal += p.inversion_total || 0;
                            pnlSuma += p.pnl_porcentaje || 0;

                            const pnlColor = p.pnl_porcentaje >= 0 ? '#4ade80' : '#f87171';
                            const slColor = p.analisis_sl.includes("Correcto") ? '#4ade80' : '#f87171';
                            divCartera.innerHTML += `
                                <div class="alerta-item" style="border-left-color: ${pnlColor};">
                                    <div style="display:flex; justify-content:space-between; font-weight:bold; font-size:0.9rem;">
                                        <span>${p.ticker} (${p.timeframe.toUpperCase()})</span>
                                        <span style="color:${pnlColor};">${p.pnl_porcentaje >= 0 ? '+' : ''}${p.pnl_porcentaje}%</span>
                                        <button onclick="eliminarPosicion('${p.ticker}')" style="background:none;color:#ef4444;border:none;cursor:pointer;">✕</button>
                                    </div>
                                    <div style="font-size:0.8rem; margin-top:4px;">Entrada: $${p.precio_compra} | Actual: $${p.precio_actual} | TP: $${p.tp_usuario}</div>
                                    <div style="font-size:0.8rem; color:#38bdf8; margin-top:2px; font-weight:bold;">Comprar: ${p.acciones || 0} acciones ($${p.inversion_total || 0} expuestos)</div>
                                    <div style="font-size:0.8rem; margin-top:2px;">Estado: ${p.estado}</div>
                                    <div style="font-size:0.8rem; margin-top:2px; font-weight:bold; color:${slColor};">Gestión SL: ${p.analisis_sl} (Tu SL: $${p.sl_usuario})</div>
                                </div>
                            `;
                        });

                        const pnlPromedio = (pnlSuma / cartera.length).toFixed(2);
                        document.getElementById('resumen-cartera').innerHTML = `
                            <span>Capital Expuesto: <b>$${capitalTotal.toFixed(2)}</b></span>
                            <span>Rendimiento Global: <b style="color:${pnlPromedio >= 0 ? '#4ade80' : '#f87171'}">${pnlPromedio >= 0 ? '+' : ''}${pnlPromedio}%</b></span>
                        `;
                    } else { 
                        divCartera.innerHTML = '<span style="font-size:0.8rem; color:#94a3b8;">Sin posiciones guardadas.</span>';
                        document.getElementById('resumen-cartera').innerHTML = `
                            <span>Capital Expuesto: <b>$0.00</b></span>
                            <span>Rendimiento Global: <b>0.00%</b></span>
                        `;
                    }

                    const divSug = document.getElementById('lista-sugerencias');
                    if(sugerencias && sugerencias.length > 0) {
                        divSug.innerHTML = '';
                        sugerencias.forEach(s => {
                            divSug.innerHTML += `
                                <div style="background:#0b132b; padding:8px; border-radius:6px; margin-bottom:6px; border:1px solid #3a506b;">
                                    <div style="font-weight:bold; color:#4ade80; font-size:0.85rem;">⭐ ${s.ticker} a $${s.precio}</div>
                                    <div style="font-size:0.75rem; color:#facc15; margin: 2px 0;">Estado: ${s.estado}</div>
                                    <div style="font-size:0.75rem; color:#cbd5e1; margin: 2px 0;">SL: $${s.sl} | TP: $${s.tp}</div>
                                    <div style="display:flex; gap:6px; margin-top:6px;">
                                        <button onclick="agregarActivo('${s.ticker}')" style="font-size:0.7rem; padding:4px 8px;">+ Seguir</button>
                                        <button onclick="usarSugerencia('${s.ticker}', ${s.precio}, ${s.sl}, ${s.tp})" style="font-size:0.7rem; padding:4px 8px; background:#10b981; color:#fff;">💼 Operar</button>
                                    </div>
                                </div>
                            `;
                        });
                    } else {
                        divSug.innerHTML = '<span style="font-size:0.8rem; color:#94a3b8;">Buscando rupturas alcistas...</span>';
                    }

                    const grid = document.getElementById('grid-mercado');
                    if (Object.keys(mercado).length > 0) {
                        grid.innerHTML = '';
                        for (const [ticker, info] of Object.entries(mercado)) {
                            const isBull = info.tendencia === 'ALZA';
                            let claseEntrada = 'entrada-wait';
                            if (info.estado_entrada.includes("BUENA ENTRADA")) claseEntrada = 'entrada-ok';
                            else if (info.estado_entrada.includes("PREPARANDO")) claseEntrada = 'entrada-prep';

                            grid.innerHTML += `
                                <div class="card">
                                    <button class="btn-remove" onclick="eliminarActivo('${ticker}')" title="Dejar de seguir">✕</button>
                                    <div class="card-header" style="padding-right: 20px;">
                                        <span class="ticker">${ticker} <span class="tf-badge">${info.timeframe}</span></span>
                                        <span class="badge ${isBull ? 'bullish' : 'bearish'}">${info.tendencia}</span>
                                    </div>
                                    <div class="price">$${info.precio}</div>
                                    <div class="${claseEntrada}">${info.estado_entrada}</div>
                                    
                                    <div class="sparkline-container">
                                        <svg width="100%" height="35" viewBox="0 0 100 35" preserveAspectRatio="none">
                                            <polyline fill="none" stroke="${info.sparkline_color}" stroke-width="2" points="${info.sparkline}" />
                                        </svg>
                                    </div>

                                    <div class="levels-box">
                                        <div class="stat"><span>🛡️ Soporte (SL):</span> <span class="sl-text">$${info.soporte_tecnico}</span></div>
                                        <div class="stat"><span>🎯 TP Técnico (1:2):</span> <span class="tp-text">$${info.tp_tecnico}</span></div>
                                    </div>
                                    <div class="stat" style="margin-top:6px;"><span>SMA 9 / 21:</span> <span>$${info.sma9} / $${info.sma21}</span></div>
                                    <div class="stat" style="margin-top:2px;"><span>Volatilidad ATR:</span> <span>$${info.atr}</span></div>

                                    <div class="links-externos">
                                        <a href="https://es.finance.yahoo.com/quote/${ticker}" target="_blank" title="Ver detalle en Español">📊 Yahoo (ES)</a>
                                        <a href="https://finance.yahoo.com/quote/${ticker}" target="_blank" title="View detail in English">📈 Yahoo (EN)</a>
                                    </div>
                                </div>
                            `;
                        }
                    } else {
                        grid.innerHTML = '<p style="color:#94a3b8;">Sin activos bajo monitoreo.</p>';
                    }

                    const lista = document.getElementById('lista-alertas');
                    if (alertas.length > 0) {
                        lista.innerHTML = alertas.map(a => `
                            <div class="alerta-item">
                                <div style="display:flex; justify-content:space-between; font-weight:bold; font-size:0.85rem;">
                                    <span>${a.symbol} - $${a.precio}</span><span style="font-size:0.7rem; color:#64748b;">${a.hora}</span>
                                </div>
                                <div style="font-size:0.78rem; margin-top:3px;">${a.evento}</div>
                            </div>
                        `).join('');
                    }
                } catch (e) { console.error(e); }
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
