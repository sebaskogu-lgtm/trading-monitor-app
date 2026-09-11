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

historial_alertas = {"NY": [], "LONDRES": [], "ASIA": []}

def get_db_connection():
  if not DATABASE_URL: return None
  try:
    url = DATABASE_URL + "?sslmode=require" if "?" not in DATABASE_URL else DATABASE_URL
    return psycopg2.connect(url)
  except: return None

def init_db():
  global APP_CONFIG
  conn = get_db_connection()
  if not conn: return
  try:
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS configuracion (id INT PRIMARY KEY, mercado_actual TEXT, datos TEXT);")
    cursor.execute("SELECT mercado_actual, datos FROM configuracion WHERE id=1;")
    row = cursor.fetchone()
    if not row:
      cursor.execute("INSERT INTO configuracion (id, mercado_actual, datos) VALUES (1, %s, %s);", (APP_CONFIG["mercado_actual"], json.dumps(APP_CONFIG["datos"])))
    else:
      APP_CONFIG["mercado_actual"] = row[0] or "NY"
      if row[1]: APP_CONFIG["datos"] = json.loads(row[1])
    conn.commit()
    cursor.close()
    conn.close()
  except: pass

init_db()

def db_save_all_data():
  conn = get_db_connection()
  if not conn: return
  try:
    cursor = conn.cursor()
    cursor.execute("UPDATE configuracion SET mercado_actual = %s, datos = %s WHERE id=1;", (APP_CONFIG["mercado_actual"], json.dumps(APP_CONFIG["datos"])))
    conn.commit()
    cursor.close()
    conn.close()
  except: pass

CATALOGO_TICKERS = {
    # NY
    "AAPL": {"nombre": "Apple Inc.", "desc": "Tecnología de consumo.", "estrategia": "Ruptura SMA 9/21."},
    "MSFT": {"nombre": "Microsoft Corp", "desc": "Cloud y software.", "estrategia": "Tendencia alcista."},
    "AMZN": {"nombre": "Amazon", "desc": "E-commerce.", "estrategia": "Rebotes en soportes."},
    "NVDA": {"nombre": "NVIDIA", "desc": "Semiconductores.", "estrategia": "Alta volatilidad."},
    "TSLA": {"nombre": "Tesla Inc.", "desc": "Vehículos eléctricos.", "estrategia": "Sobreventa RSI."},
    "SPY": {"nombre": "S&P 500 ETF", "desc": "Índice principal.", "estrategia": "Filtro tendencial."},
    "QQQ": {"nombre": "Nasdaq 100", "desc": "Tecnológicas.", "estrategia": "Cruce de medias."},
    "BTC-USD": {"nombre": "Bitcoin USD", "desc": "Cripto.", "estrategia": "Niveles 24/7."},
    # Londres
    "SHEL.L": {"nombre": "Shell plc", "desc": "Petróleo/Gas.", "estrategia": "Precios de crudo."},
    "AZN.L": {"nombre": "AstraZeneca", "desc": "Farmacéutica.", "estrategia": "Defensiva."},
    "ULVR.L": {"nombre": "Unilever", "desc": "Consumo.", "estrategia": "Rango defensivo."},
    "HSBA.L": {"nombre": "HSBC", "desc": "Banca.", "estrategia": "Tipos de interés."},
    "BP.L": {"nombre": "BP p.l.c.", "desc": "Energía.", "estrategia": "Correlación crudo."},
    "RIO.L": {"nombre": "Rio Tinto", "desc": "Minería.", "estrategia": "Metales industriales."},
    # Asia
    "7203.T": {"nombre": "Toyota", "desc": "Automotriz.", "estrategia": "Flujos USD/JPY."},
    "6758.T": {"nombre": "Sony", "desc": "Electrónica.", "estrategia": "Quiebres de resistencia."},
    "7974.T": {"nombre": "Nintendo", "desc": "Videojuegos.", "estrategia": "Ciclos estacionales."},
    "9984.T": {"nombre": "SoftBank", "desc": "Inversión.", "estrategia": "Tech global."},
    "8306.T": {"nombre": "Mitsubishi UFJ", "desc": "Banca.", "estrategia": "Política BOJ."},
    "6501.T": {"nombre": "Hitachi", "desc": "Infraestructura.", "estrategia": "Contratos industriales."}
}

POOLS_ESCANER = {
    "NY": ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "NFLX", "AMD", "SPY", "QQQ"],
    "LONDRES": ["SHEL.L", "AZN.L", "ULVR.L", "HSBA.L", "BP.L", "GSK.L", "RIO.L", "BARC.L", "LLOY.L", "VOD.L"],
    "ASIA": ["7203.T", "6758.T", "7974.T", "9984.T", "8306.T", "6861.T", "6501.T", "4063.T", "6902.T", "8035.T"]
}

BLACKLIST_MATERIAS = ["SHEL.L", "BP.L", "RIO.L", "AAL.L", "XOM", "CVX", "GLD", "SLV"]

timeframe_actual = "1h"
estado_mercado = {}
recomendaciones_escaner = []
cache_yf = {}
SSE_SUBSCRIBERS = []

class TimeframeModel(BaseModel): timeframe: str
class MercadoModel(BaseModel): mercado: str
class PosicionModel(BaseModel):
  ticker: str; precio_compra: float; sl_usuario: float; tp_usuario: float; riesgo_usd: float; timeframe: str
class ReordenarModel(BaseModel): activos: list
class SimuladorModel(BaseModel): capital: float; fracciones: bool; sin_materias: bool

def obtener_info_horario():
  m_act = APP_CONFIG["mercado_actual"]
  if m_act == "LONDRES": tz_str, n_m, h_o, m_o, h_c, m_c, lunch = "Europe/London", "Londres", 8, 0, 16, 30, False
  elif m_act == "ASIA": tz_str, n_m, h_o, m_o, h_c, m_c, lunch = "Asia/Tokyo", "Asia", 9, 0, 15, 30, True
  else: tz_str, n_m, h_o, m_o, h_c, m_c, lunch = "America/New_York", "NY", 9, 30, 16, 0, False

  tz = pytz.timezone(tz_str)
  now = datetime.now(tz)
  m_open = now.replace(hour=h_o, minute=m_o, second=0, microsecond=0)
  m_close = now.replace(hour=h_c, minute=m_c, second=0, microsecond=0)
  
  if now.weekday() >= 5: return "🔴 CERRADO (Fin semana)", (m_open + timedelta(days=7-now.weekday())).timestamp(), tz_str, "Abre"
  if now < m_open: return "🔴 CERRADO (Pre)", m_open.timestamp(), tz_str, "Abre"
  if now >= m_close: return "🔴 CERRADO", (m_open + timedelta(days=3 if now.weekday()==4 else 1)).timestamp(), tz_str, "Abre"
  if lunch:
    ls = now.replace(hour=11, minute=30, second=0, microsecond=0)
    le = now.replace(hour=12, minute=30, second=0, microsecond=0)
    if ls <= now < le: return "☕ RECESO", le.timestamp(), tz_str, "Vuelve"
  return "🟢 ABIERTO", m_close.timestamp(), tz_str, "Cierra"

def obtener_config_tf(tf: str):
  if tf == "4h": return "60d", "60m"
  if tf == "1d": return "6mo", "1d"
  return "1mo", "1h"

def get_bandera(mercado):
    if mercado == "NY": return "🇺🇸"
    if mercado == "LONDRES": return "🇬🇧"
    return "🇯🇵"

def procesar_ticker(symbol, tf_local, mercado):
  ahora = time.time()
  if symbol in cache_yf and cache_yf[symbol]["tf"] == tf_local and (ahora - cache_yf[symbol]["time"] < 35):
    return cache_yf[symbol]["data"]

  per, inter = obtener_config_tf(tf_local)
  try:
    tendencia_macro = "ALZA"
    if tf_local in ["1h", "4h"]:
      try:
        df_m = yf.download(tickers=symbol, period="6mo", interval="1d", progress=False)
        if not df_m.empty:
          if hasattr(df_m.columns, "nlevels") and df_m.columns.nlevels > 1: df_m.columns = df_m.columns.get_level_values(0)
          sm9 = df_m["Close"].rolling(9).mean().iloc[-1]
          sm21 = df_m["Close"].rolling(21).mean().iloc[-1]
          if not pd.isna(sm9) and not pd.isna(sm21): tendencia_macro = "ALZA" if sm9 > sm21 else "BAJA"
      except: pass

    df = yf.download(tickers=symbol, period=per, interval=inter, progress=False)
    if df.empty: return None
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1: df.columns = df.columns.get_level_values(0)

    if tf_local == "4h" and len(df) >= 4: df = df.resample("4h").agg({"Open":"first", "High":"max", "Low":"min", "Close":"last", "Volume":"sum"}).dropna()

    if len(df) >= 20:
      df["SMA_9"] = df["Close"].rolling(9).mean()
      df["SMA_21"] = df["Close"].rolling(21).mean()
      df["Res"] = df["High"].rolling(20).max().shift(1)
      df["Sup"] = df["Low"].rolling(10).min().shift(1)
      df["ATR"] = df["High"] - df["Low"]
      atr_medio = round(float(df["ATR"].rolling(14).mean().iloc[-1]), 2)
      delta = df["Close"].diff()
      gain = (delta.where(delta > 0, 0)).rolling(14).mean()
      loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
      df["RSI"] = 100 - (100 / (1 + (gain / loss)))
      rsi_val = round(float(df["RSI"].iloc[-1]), 1) if not pd.isna(df["RSI"].iloc[-1]) else 50.0
      vol_val = True
      if "Volume" in df.columns:
        df["VSMA"] = df["Volume"].rolling(20).mean()
        if float(df["VSMA"].iloc[-1]) > 0: vol_val = float(df["Volume"].iloc[-1]) >= (1.5 * float(df["VSMA"].iloc[-1]))

      ult = df.iloc[-1]
      ant = df.iloc[-2]
      precio = round(float(ult["Close"]), 2)
      resistencia = round(float(ant["Res"]), 2)
      soporte = round(float(ant["Sup"]), 2)
      sma9 = round(float(ult["SMA_9"]), 2)
      sma21 = round(float(ult["SMA_21"]), 2)
      tendencia = "ALZA" if sma9 > sma21 else "BAJA"
      hora = datetime.now().strftime("%H:%M:%S")

      max_t = float(df["High"].tail(20).max())
      min_t = float(df["Low"].tail(20).min())
      dif = max_t - min_t
      fib5 = round(max_t - (dif * 0.5), 2)
      fib6 = round(max_t - (dif * 0.618), 2)
      en_fib = (fib6 * 0.99) <= precio <= (fib5 * 1.01)

      riesgo = precio - soporte
      tp_t = round(precio + (riesgo * 2), 2) if riesgo > 0 else round(precio * 1.02, 2)
      r_m_s = " | ⚠️ Macro BAJA" if tendencia_macro == "BAJA" else ""

      if precio > resistencia and tendencia == "ALZA":
        if rsi_val >= 70: est = f"⚠️ SOBRECOMPRADO (RSI {rsi_val})"
        elif not vol_val: est = f"⚠️ FALSO QUIEBRE (RSI {rsi_val})"
        else: est = f"🟢 BUENA ENTRADA (Quiebre){r_m_s}"
      elif 0 < ((resistencia - precio)/precio*100) <= 1.2 and tendencia == "ALZA": est = f"⏳ PREPARANDO (RSI {rsi_val}){r_m_s}"
      elif rsi_val <= 30 and en_fib: est = f"💥 REBOTE EN ZONA{r_m_s}"
      elif rsi_val <= 30: est = f"📉 SOBREVENDIDO (RSI {rsi_val})"
      else: est = f"⏳ ESPERAR (RSI {rsi_val})"

      upre = df["Close"].tail(15).tolist()
      mp, Map = min(upre), max(upre)
      rp = Map - mp if Map != mp else 1
      spark = " ".join([f"{round((i/14)*100,1)},{round(35-((v-mp)/rp)*30,1)}" for i, v in enumerate(upre)])

      meta = CATALOGO_TICKERS.get(symbol, {"nombre": symbol, "desc": "Activo global.", "estrategia": "Análisis técnico estándar."})

      res = {
          "symbol": symbol, "nombre": meta["nombre"], "descripcion": meta["desc"],
          "estrategia_explicacion": meta["estrategia"], "timeframe": tf_local.upper(),
          "precio": precio, "resistencia": resistencia, "soporte_tecnico": soporte,
          "tp_tecnico": tp_t, "sma9": sma9, "sma21": sma21, "rsi": rsi_val,
          "tendencia": tendencia, "tendencia_macro": tendencia_macro, "vol_valido": vol_val,
          "estado_entrada": est, "atr": atr_medio, "fib_50": fib5, "fib_618": fib6,
          "en_zona_fib": en_fib, "hora": hora, "sparkline": spark,
          "sparkline_color": "#4ade80" if tendencia == "ALZA" else "#f87171",
          "bandera": get_bandera(mercado)
      }
      cache_yf[symbol] = {"tf": tf_local, "time": ahora, "data": res}
      return res
  except Exception: pass
  return None

def escaneo_autonomo():
  global recomendaciones_escaner
  while True:
    try:
      m_act = APP_CONFIG["mercado_actual"]
      pool = POOLS_ESCANER.get(m_act, POOLS_ESCANER["NY"])
      with ThreadPoolExecutor(max_workers=5) as ex:
        res = [r for r in ex.map(lambda s: procesar_ticker(s, timeframe_actual, m_act), pool) if r]
      ops = [r for r in res if "BUENA ENTRADA" in r["estado_entrada"] or "PREPARANDO" in r["estado_entrada"] or "REBOTE" in r["estado_entrada"]]
      ops.sort(key=lambda x: 0 if "BUENA ENTRADA" in x["estado_entrada"] else (1 if "REBOTE" in x["estado_entrada"] else 2))
      recomendaciones_escaner = [{"ticker": r["symbol"], "precio": r["precio"], "tp": r["tp_tecnico"], "sl": r["soporte_tecnico"], "rsi": r["rsi"], "estado": r["estado_entrada"], "bandera": r["bandera"]} for r in ops[:5]]
    except: pass
    time.sleep(120)

def procesar_lote_mercado():
  global estado_mercado
  m_act = APP_CONFIG["mercado_actual"]
  lista = APP_CONFIG["datos"][m_act]["activos"]
  with ThreadPoolExecutor(max_workers=5) as ex:
    res = list(ex.map(lambda s: procesar_ticker(s, timeframe_actual, m_act), lista))
  nuevo_estado = {}
  for r in res:
    if r:
      sym = r["symbol"]
      nuevo_estado[sym] = r
      if "BUENA ENTRADA" in r["estado_entrada"] or "REBOTE" in r["estado_entrada"]:
        _registrar_alerta(sym, f"🟢 ALERTA ({r['estado_entrada']}) | TP: ${r['tp_tecnico']}", r["precio"], r["hora"], r["soporte_tecnico"], r["tp_tecnico"], m_act)
      _evaluar_cartera(sym, r["precio"], r["sma9"], r["sma21"], r["soporte_tecnico"], r["atr"], r["hora"], m_act)
  estado_mercado = nuevo_estado
  for q in SSE_SUBSCRIBERS:
    try: q.put_nowait("update")
    except: pass

def analizar_mercado():
  while True:
    procesar_lote_mercado()
    time.sleep(INTERVALO_SEGUNDOS)

def _evaluar_cartera(symbol, precio, sma9, sma21, sop, atr, hora, mercado):
  cartera = APP_CONFIG["datos"][mercado]["cartera"]
  mod = False
  for p in cartera:
    if p["ticker"] == symbol:
      p_c = p["precio_compra"]
      sl_u = p["sl_usuario"]
      gan = ((precio - p_c) / p_c) * 100
      est = "🔵 MANTENER"
      if sma9 < sma21:
        est = "⚠️ CRUCE BAJA"
        _registrar_alerta(symbol, "⚠️ CARTERA: Pérdida impulso.", precio, hora, sop, p["tp_usuario"], mercado)
      elif gan >= 2.0: est = "🟢 GANANCIA"
      dsl = precio - sl_u
      msl = "✔️ SL Correcto"
      if sl_u >= precio: msl = "❌ SL Inválido"
      elif gan >= 3.0 and sl_u < sop: msl = f"📈 Sube SL a soporte (${sop})"
      elif gan >= 1.5 and sl_u < p_c: msl = f"🔔 Asegura a BE (${p_c})"
      elif dsl < (atr * 0.5): msl = "⚠️ SL MUY CORTO"
      elif sl_u < (sop * 0.95): msl = "⚠️ SL MUY LEJOS"
      p.update({"precio_actual": precio, "pnl_porcentaje": round(gan, 2), "estado": est, "analisis_sl": msl, "bandera": get_bandera(mercado)})
      mod = True
  if mod: db_save_all_data()

def _registrar_alerta(sym, ev, pre, hr, sl, tp, mer):
  global historial_alertas
  L = historial_alertas[mer]
  if not L or L[0]["symbol"] != sym or L[0]["evento"] != ev:
    L.insert(0, {"symbol": sym, "evento": ev, "precio": pre, "hora": hr, "sl": sl, "tp": tp, "bandera": get_bandera(mer)})
    historial_alertas[mer] = L[:20]

threading.Thread(target=analizar_mercado, daemon=True).start()
threading.Thread(target=escaneo_autonomo, daemon=True).start()

@app.get("/api/data")
def obtener_datos():
  m_act = APP_CONFIG["mercado_actual"]
  inf = APP_CONFIG["datos"].get(m_act, {"activos": [], "cartera": []})
  est, t_ts, tz_n, pfx = obtener_info_horario()
  return {
      "mercado": estado_mercado, "alertas": historial_alertas[m_act], "cartera": inf.get("cartera", []),
      "timeframe": timeframe_actual, "mercado_actual": m_act, "estado_horario": est, "target_ts": t_ts,
      "prefix_cuenta": pfx, "timezone": tz_n, "sugerencias": recomendaciones_escaner,
      "catalogo": CATALOGO_TICKERS, "activos_orden": inf.get("activos", [])
  }

@app.get("/api/stream")
async def stream_endpoint(req: Request):
  q = asyncio.Queue()
  SSE_SUBSCRIBERS.append(q)
  async def gen():
    try:
      while True:
        if await req.is_disconnected(): break
        try:
          await asyncio.wait_for(q.get(), timeout=15.0)
          yield "data: update\n\n"
        except asyncio.TimeoutError: yield ":ping\n\n"
    except: pass
    finally:
      if q in SSE_SUBSCRIBERS: SSE_SUBSCRIBERS.remove(q)
  return StreamingResponse(gen(), media_type="text/event-stream")

def auto_sufijo(sym, mer):
  if "-" in sym or sym in ["SPY", "QQQ"]: return sym
  if mer == "LONDRES" and not sym.endswith(".L"): return f"{sym}.L"
  if mer == "ASIA" and not sym.endswith(".T"): return f"{sym}.T"
  return sym

@app.post("/api/add")
async def agregar_activo(req: Request):
  d = await req.json()
  sym = auto_sufijo(d.get("ticker", "").strip().upper(), APP_CONFIG["mercado_actual"])
  if sym and sym not in APP_CONFIG["datos"][APP_CONFIG["mercado_actual"]]["activos"]:
    APP_CONFIG["datos"][APP_CONFIG["mercado_actual"]]["activos"].append(sym)
    db_save_all_data()
    threading.Thread(target=procesar_lote_mercado, daemon=True).start()
  return {"status": "ok"}

@app.post("/api/remove")
async def eliminar_activo(req: Request):
  d = await req.json()
  sym = d.get("ticker", "").strip().upper()
  if sym in APP_CONFIG["datos"][APP_CONFIG["mercado_actual"]]["activos"]:
    APP_CONFIG["datos"][APP_CONFIG["mercado_actual"]]["activos"].remove(sym)
    db_save_all_data()
    if sym in estado_mercado: del estado_mercado[sym]
    for q in SSE_SUBSCRIBERS:
      try: q.put_nowait("update")
      except: pass
  return {"status": "ok"}

@app.post("/api/mercado")
def cambiar_mercado(item: MercadoModel):
  global estado_mercado
  if item.mercado in ["NY", "LONDRES", "ASIA"]:
    APP_CONFIG["mercado_actual"] = item.mercado
    db_save_all_data()
    estado_mercado = {}
    threading.Thread(target=procesar_lote_mercado, daemon=True).start()
  return {"status": "ok"}

@app.post("/api/timeframe")
def cambiar_timeframe(item: TimeframeModel):
  global timeframe_actual, estado_mercado
  if item.timeframe in ["1h", "4h", "1d"]:
    timeframe_actual = item.timeframe
    estado_mercado = {}
    threading.Thread(target=procesar_lote_mercado, daemon=True).start()
  return {"status": "ok"}

@app.post("/api/cartera/add")
def agregar_cartera(i: PosicionModel):
  m = APP_CONFIG["mercado_actual"]
  t = auto_sufijo(i.ticker.strip().upper(), m)
  dsl = abs(i.precio_compra - i.sl_usuario)
  acc = round(i.riesgo_usd / dsl, 2) if dsl > 0 else 0
  APP_CONFIG["datos"][m]["cartera"] = [p for p in APP_CONFIG["datos"][m]["cartera"] if p["ticker"] != t]
  APP_CONFIG["datos"][m]["cartera"].append({
      "ticker": t, "precio_compra": i.precio_compra, "sl_usuario": i.sl_usuario, "tp_usuario": i.tp_usuario,
      "riesgo_usd": i.riesgo_usd, "acciones": acc, "inversion_total": round(acc * i.precio_compra, 2),
      "timeframe": i.timeframe, "precio_actual": i.precio_compra, "pnl_porcentaje": 0.0, "estado": "🔵 MANTENER",
      "analisis_sl": "Analizando...", "bandera": get_bandera(m)
  })
  db_save_all_data()
  threading.Thread(target=procesar_lote_mercado, daemon=True).start()
  return {"status": "ok"}

@app.post("/api/cartera/remove")
async def eliminar_cartera(req: Request):
  d = await req.json()
  sym = d.get("ticker", "").strip().upper()
  m = APP_CONFIG["mercado_actual"]
  APP_CONFIG["datos"][m]["cartera"] = [p for p in APP_CONFIG["datos"][m]["cartera"] if p["ticker"] != sym]
  db_save_all_data()
  for q in SSE_SUBSCRIBERS:
    try: q.put_nowait("update")
    except: pass
  return {"status": "ok"}

@app.post("/api/simulador")
def endpoint_simulador(item: SimuladorModel):
  # Combina los activos default de los 3 mercados para hacer un barrido real
  tickers_eval = []
  for m in ["NY", "LONDRES", "ASIA"]: tickers_eval.extend([(t, m) for t in POOLS_ESCANER[m]])
  
  def fetch_sim(t_tuple):
    sym, m = t_tuple
    if item.sin_materias and sym in BLACKLIST_MATERIAS: return None
    res = procesar_ticker(sym, timeframe_actual, m)
    if not res: return None
    st = res["estado_entrada"]
    if not ("BUENA ENTRADA" in st or "PREPARANDO" in st or "REBOTE" in st): return None
    
    precio = res["precio"]
    lote = 100 if m == "ASIA" else 1 # Lógica real TSE (Tokio)
    
    if item.fracciones: acciones = item.capital / precio
    else: acciones = (item.capital // (precio * lote)) * lote
    
    if acciones <= 0: return None
    
    inv = acciones * precio
    sob = item.capital - inv
    rie = (precio - res["soporte_tecnico"]) * acciones
    ben = (res["tp_tecnico"] - precio) * acciones
    
    if rie <= 0 or ben <= 0: return None
    
    res["sim_acc"] = round(acciones, 4) if item.fracciones else int(acciones)
    res["sim_inv"] = round(inv, 2)
    res["sim_sob"] = round(sob, 2)
    res["sim_rie"] = round(rie, 2)
    res["sim_ben"] = round(ben, 2)
    res["sim_rat"] = round(ben / rie, 1)
    return res

  with ThreadPoolExecutor(max_workers=10) as ex:
    r_list = list(ex.map(fetch_sim, tickers_eval))
    
  validos = [r for r in r_list if r]
  validos.sort(key=lambda x: x["sim_rat"], reverse=True)
  return {"resultados": validos[:5]}

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
            .grid-activos.list-view .card-header { margin-bottom: 0; width: 160px; }
            .grid-activos.list-view .price { font-size: 1.1rem; margin-bottom: 0; width: 75px; }
            .grid-activos.list-view .entrada-ok, .grid-activos.list-view .entrada-prep, .grid-activos.list-view .entrada-wait, .grid-activos.list-view .entrada-warn, .grid-activos.list-view .entrada-rebote { margin-bottom: 0; width: 160px; text-align: center; font-size: 0.72rem; cursor: pointer; }
            .grid-activos.list-view .sparkline-container { width: 80px; height: 25px; margin-top: 0; }
            .grid-activos.list-view .levels-box { display: none; }
            .grid-activos.list-view .list-actions-bar { display: flex; gap: 6px; align-items: center; }

            .badge { padding: 3px 6px; border-radius: 10px; font-size: 0.68rem; font-weight: bold; }
            .tf-badge { background: #3a506b; color: #cbd5e1; padding: 2px 5px; border-radius: 4px; font-size: 0.65rem; }
            .flag-badge { font-size: 0.8rem; margin-right: 4px; }
            
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
            
            .sim-card { background: #0b132b; border: 1px solid #38bdf8; padding: 10px; border-radius: 8px; margin-bottom: 10px; }
            .sim-card .sim-title { font-weight: bold; color: #f8fafc; display: flex; justify-content: space-between; margin-bottom: 6px; }
            .sim-row { display: flex; justify-content: space-between; font-size: 0.85rem; color: #cbd5e1; margin-top: 3px; }
        </style>
    </head>
    <body>
        <div id="pantalla-carga">
            <div class="spinner"></div>
            <h3 style="color:#38bdf8; margin:0;" id="txt-carga">Sincronizando...</h3>
        </div>

        <div style="display: flex; justify-content: space-between; align-items: center; max-width: 1200px; margin: 0 auto;">
            <div></div>
            <h1>📊 Trading Monitor Pro</h1>
            <div></div>
        </div>

        <div class="reloj-box">
            <div id="reloj-mercado" class="reloj">Cargando Horarios... <span class="live-indicator"></span></div>
            <div id="reloj-cuenta" class="reloj-sub">--:--:--</div>
            <div style="font-size: 0.9rem; margin-top: 5px; color: #4ade80;">Hora Bolsa Local: <b id="reloj-hora-mercado">--:--:--</b></div>
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
        </div>

        <div class="container">
            <div>
                <h3>Activos bajo Monitoreo (Orden Automático por Urgencia)</h3>
                <div class="grid-activos" id="grid-mercado"><p style="color:#94a3b8;">⏳ Descargando base de datos...</p></div>

                <div class="cartera-panel" style="margin-top: 16px;">
                    <div class="feed-title"><span>💼 Mi Cartera y Gestión de Riesgo</span></div>
                    <div class="metrics-bar" id="resumen-cartera">
                        <span>Capital: <b>$0.00</b></span><span>Rendimiento: <b>0.00%</b></span>
                    </div>
                    <div style="font-size:0.8rem; font-weight:bold; color:#38bdf8; margin-bottom:6px;">🧮 CALCULADORA MANUAL DE RIESGO</div>
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
                
                <!-- SIMULADOR ESTRICTO -->
                <div class="feed-panel" style="border-color:#10b981;">
                    <div class="feed-title" style="color:#10b981;">🧪 Simulador Inversión Estricto</div>
                    <p style="font-size:0.8rem; color:#cbd5e1; margin-top:0;">Filtra y calcula operaciones matemáticas en TODOS los mercados según tu liquidez real.</p>
                    <div style="display:flex; gap:6px; margin-bottom:8px;">
                        <input type="number" id="sim-capital" placeholder="Capital Ej: 300" style="width:100%; border-color:#10b981;">
                        <button onclick="ejecutarSimulador()" style="background:#10b981; color:#fff;">Simular</button>
                    </div>
                    <label style="font-size:0.8rem; color:#cbd5e1; display:block; margin-bottom:4px;"><input type="checkbox" id="sim-nomat" checked> Excluir Materias Primas</label>
                    <label style="font-size:0.8rem; color:#cbd5e1; display:block;"><input type="checkbox" id="sim-frac"> Permitir fracciones en IBKR</label>
                    <div id="sim-resultados" style="margin-top:12px;"></div>
                </div>

                <div class="feed-panel">
                    <div class="feed-title">🚨 Feed de Alertas en Vivo</div>
                    <div id="lista-alertas">Sin señales recientes.</div>
                </div>

                <div class="feed-panel">
                    <div class="feed-title">🤖 Escáner Dinámico (Este Mercado)</div>
                    <div id="lista-sugerencias" style="font-size:0.85rem; color:#cbd5e1;">Buscando Momentum y Rebotes...</div>
                </div>

                <div class="edu-panel">
                    <div class="feed-title">📖 Manual PRO Integral</div>
                    <button onclick="toggleManual()" style="width:100%; font-size:0.78rem; background:#3a506b; color:#fff; margin-bottom:8px; border:none; padding:8px; border-radius:4px; cursor:pointer; font-weight:bold;">📚 Desplegar / Ocultar Guía de Uso</button>
                    <div id="box-manual" class="manual-box" style="display:none;">
                        <details><summary>🎯 Estrategias y Estados</summary><ul><li><b>🟢 Buena Entrada:</b> Superó resistencia con volumen real. Ideal entrar.</li><li><b>⏳ Preparando:</b> Retrocediendo a soporte sano (Fibonacci). Esperar rebote.</li><li><b>💥 Rebote en Zona:</b> Caída fuerte a piso técnico extremo. Riesgoso pero rentable con SL ajustado.</li><li><b>⚠️ Falso Quiebre:</b> Ruptura sin volumen. Evitar trampa.</li></ul></details>
                        <details><summary>📊 Indicadores Utilizados</summary><ul><li><b>SMA 9 / 21:</b> Medias Móviles. 9 > 21 es alcista.</li><li><b>RSI (14):</b> Mide agotamiento. >70 Sobrecomprado, <30 Sobrevendido.</li><li><b>Macro (1D):</b> Filtro de seguridad del gráfico diario.</li></ul></details>
                        <details><summary>💼 Gestión (SL Audit)</summary><p>Audita tu SL en vivo:</p><ul><li><b>SL Muy Corto / Lejos:</b> Según volatilidad (ATR).</li><li><b>Sube SL a Soporte (Trailing):</b> Si ganas +3%, asegura.</li></ul></details>
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

            setInterval(() => {
                if(!currentTz) return;
                const options = { timeZone: currentTz, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false };
                try { document.getElementById('reloj-hora-mercado').innerText = new Intl.DateTimeFormat('es-ES', options).format(new Date()); } catch(e) {}
                if(targetTimestampGlobal > 0) {
                    const diffMs = Math.max(0, (targetTimestampGlobal * 1000) - Date.now());
                    const ts = Math.floor(diffMs / 1000);
                    const d = Math.floor(ts / 86400), h = Math.floor((ts % 86400) / 3600), m = Math.floor((ts % 3600) / 60), s = ts % 60;
                    document.getElementById('reloj-cuenta').innerText = `${prefixCuentaGlobal} en ${d>0?d+'d ':''}${h}h ${m}m ${s}s`;
                }
            }, 1000);

            const explicacionesEstados = {
                "BUENA ENTRADA": { titulo: "🟢 Buena Entrada", porque: "Superó resistencia con volumen.", resultado: "Compradores al mando.", queHacer: "Operar." },
                "PREPARANDO": { titulo: "⏳ Preparando", porque: "Retrocediendo a zona de soporte sano.", resultado: "Descanso técnico.", queHacer: "Vigilar rebote." },
                "REBOTE EN ZONA": { titulo: "💥 Rebote en Zona", porque: "Tocó piso técnico en sobreventa.", resultado: "Posible giro rápido.", queHacer: "Operar agresivo con SL." },
                "FALSO QUIEBRE": { titulo: "⚠️ Falso Quiebre", porque: "Rompió sin volumen.", resultado: "Trampa.", queHacer: "No operar." },
                "SOBRECOMPRADO": { titulo: "⚠️ Sobrecomprado", porque: "Subió vertical.", resultado: "Riesgo de caída.", queHacer: "No entrar." },
                "SOBREVENDIDO": { titulo: "📉 Sobrevendido", porque: "Caída sin frenos.", resultado: "Sin suelo aún.", queHacer: "Esperar confirmación." },
                "ESPERAR": { titulo: "⏳ Esperar", porque: "Lateralidad.", resultado: "Ruido.", queHacer: "Buscar otra cosa." }
            };

            function toggleManual() { const el = document.getElementById('box-manual'); el.style.display = el.style.display === 'none' ? 'block' : 'none'; }
            function toggleVista() { modoLista = !modoLista; document.getElementById('grid-mercado').classList.toggle('list-view'); document.getElementById('btn-vista').innerText = modoLista ? "🔲 Cuadrícula" : "📋 Lista"; renderizarGridMercado(); }
            function mostrarCarga(msg="Sincronizando...") { document.getElementById('txt-carga').innerText = msg; document.getElementById('pantalla-carga').style.display = 'flex'; }
            function ocultarCarga() { document.getElementById('pantalla-carga').style.display = 'none'; }

            async function cambiarMercado(mercado) {
                mostrarCarga("Cambiando Bolsa...");
                document.getElementById('grid-mercado').innerHTML = ''; document.getElementById('lista-alertas').innerHTML = '';
                await fetch('/api/mercado', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ mercado: mercado }) });
                setTimeout(async () => { await actualizarApp(); ocultarCarga(); }, 1000);
            }

            function formatearLinkTV(ticker, mercado) {
                let sym = ticker;
                if(mercado === 'LONDRES') sym = 'LSE:' + ticker.replace('.L', '');
                else if(mercado === 'ASIA') sym = 'TSE:' + ticker.replace('.T', '');
                return `https://www.tradingview.com/chart/?symbol=${sym}`;
            }

            function mostrarModal(ticker) {
                const info = mercadoGlobalData[ticker] || (catalogoGlobal[ticker] ? { nombre: catalogoGlobal[ticker].nombre, descripcion: catalogoGlobal[ticker].desc, estrategia_explicacion: catalogoGlobal[ticker].estrategia } : { nombre: ticker, descripcion: "Activo alta volatilidad.", estrategia_explicacion: "Monitoreo técnico base."});
                document.getElementById('modal-titulo').innerText = `${ticker} - ${info.nombre}`;
                document.getElementById('modal-body-content').innerHTML = `<p><b>Contexto:</b></p><p style="color:#cbd5e1; font-size:0.85rem;">${info.descripcion}</p><p><b>Estrategia de Operación:</b></p><p style="color:#cbd5e1; font-size:0.85rem;">${info.estrategia_explicacion}</p>`;
                document.getElementById('info-modal').style.display = 'flex';
            }
            function mostrarExplicacionEstado(cl) {
                let enc = Object.entries(explicacionesEstados).find(([k, v]) => cl.toUpperCase().includes(k))?.[1] || {titulo: "ℹ️ Evaluación", porque: "Procesando.", resultado: "Activo.", queHacer: "Vigilar."};
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
                mostrarCarga("Recalculando temporalidad...");
                await fetch('/api/timeframe', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ timeframe: tf }) });
                setTimeout(async () => { await actualizarApp(); ocultarCarga(); }, 800);
            }

            async function agregarActivo(tP = null) {
                const tk = tP || document.getElementById('new-ticker').value.trim();
                if (!tk) return;
                mostrarCarga("Agregando activo...");
                await fetch('/api/add', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker: tk }) });
                if(!tP) document.getElementById('new-ticker').value = '';
                setTimeout(async () => { await actualizarApp(); ocultarCarga(); }, 800);
            }

            function usarParaOperar(ticker, precio, sl, tp) {
                document.getElementById('c-ticker').value = ticker; document.getElementById('c-precio').value = precio;
                document.getElementById('c-sl').value = sl; document.getElementById('c-tp').value = tp;
                window.scrollTo({ top: 0, behavior: 'smooth' });
            }

            async function eliminarActivo(ticker) {
                mostrarCarga("Eliminando...");
                await fetch('/api/remove', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker: ticker }) });
                setTimeout(async () => { await actualizarApp(); ocultarCarga(); }, 500);
            }

            async function registrarPosicion() {
                const t = document.getElementById('c-ticker').value.trim(), p = parseFloat(document.getElementById('c-precio').value), sl = parseFloat(document.getElementById('c-sl').value), tp = parseFloat(document.getElementById('c-tp').value), r = parseFloat(document.getElementById('c-riesgo').value) || 50, tf = document.getElementById('select-tf').value;
                if (!t || isNaN(p) || isNaN(sl) || isNaN(tp)) return;
                mostrarCarga("Guardando posición...");
                await fetch('/api/cartera/add', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker: t, precio_compra: p, sl_usuario: sl, tp_usuario: tp, riesgo_usd: r, timeframe: tf }) });
                document.getElementById('c-ticker').value = ''; document.getElementById('c-precio').value = ''; document.getElementById('c-sl').value = ''; document.getElementById('c-tp').value = '';
                setTimeout(async () => { await actualizarApp(); ocultarCarga(); }, 800);
            }

            async function eliminarPosicion(ticker) {
                mostrarCarga("Borrando...");
                await fetch('/api/cartera/remove', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker: ticker }) });
                setTimeout(async () => { await actualizarApp(); ocultarCarga(); }, 500);
            }

            async function ejecutarSimulador() {
                const cap = parseFloat(document.getElementById('sim-capital').value);
                if (isNaN(cap) || cap <= 0) { alert("Ingresa un capital válido."); return; }
                const divRes = document.getElementById('sim-resultados');
                divRes.innerHTML = `<span style="color:#facc15; font-size:0.85rem;">Analizando matemáticamente 3 mercados (Esto puede tardar 10-20 seg)...</span>`;
                
                const payload = {
                    capital: cap,
                    fracciones: document.getElementById('sim-frac').checked,
                    sin_materias: document.getElementById('sim-nomat').checked
                };

                try {
                    const r = await fetch('/api/simulador', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) });
                    const d = await r.json();
                    if (!d.resultados || d.resultados.length === 0) {
                        divRes.innerHTML = `<span style="color:#f87171; font-size:0.85rem;">Ningún activo válido para tu capital y reglas. (¿Poco capital para Asia sin fracciones?)</span>`;
                        return;
                    }

                    divRes.innerHTML = d.resultados.map(res => `
                        <div class="sim-card">
                            <div class="sim-title">
                                <span><span class="flag-badge">${res.bandera}</span> ${res.symbol}</span>
                                <span style="color:#4ade80; font-size:0.75rem;">${res.estado_entrada}</span>
                            </div>
                            <div class="sim-row"><span>Precio Activo:</span> <b>$${res.precio}</b></div>
                            <div class="sim-row" style="color:#38bdf8;"><span>Comprar Acciones:</span> <b>${res.sim_acc} un.</b></div>
                            <div class="sim-row"><span>Inversión Real:</span> <b>$${res.sim_inv}</b> (Sobra: $${res.sim_sob})</div>
                            <hr style="border:0; border-top:1px solid #3a506b; margin:6px 0;">
                            <div class="sim-row" style="color:#f87171;"><span>Riesgo Máximo (SL $${res.soporte_tecnico}):</span> <b>-$${res.sim_rie}</b></div>
                            <div class="sim-row" style="color:#4ade80;"><span>Beneficio (TP $${res.tp_tecnico}):</span> <b>+$${res.sim_ben}</b></div>
                            <div style="margin-top:8px; text-align:right;">
                                <button onclick="usarParaOperar('${res.symbol}', ${res.precio}, ${res.soporte_tecnico}, ${res.tp_tecnico})" style="font-size:0.7rem; background:#10b981; color:#fff; padding:3px 6px;">Llevar a Calculadora</button>
                            </div>
                        </div>
                    `).join('');
                } catch(e) { divRes.innerHTML = `<span style="color:#f87171; font-size:0.85rem;">Error al simular.</span>`; }
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
                    currentTz = data.timezone;
                    targetTimestampGlobal = data.target_ts;
                    prefixCuentaGlobal = data.prefix_cuenta;

                    ['NY', 'LONDRES', 'ASIA'].forEach(m => {
                        const btn = document.getElementById(`btn-mercado-${m}`);
                        if(btn) btn.classList.toggle('active', m === mercadoEnUso);
                    });

                    if(ordenActivosGlobal.length > 0 && Object.keys(mercadoGlobalData).length > 0) {
                        ordenActivosGlobal.sort((a, b) => {
                            const sa = mercadoGlobalData[a]?.estado_entrada || "";
                            const sb = mercadoGlobalData[b]?.estado_entrada || "";
                            const pa = (sa.includes("BUENA")||sa.includes("REBOTE"))?1 : (sa.includes("PREPARANDO")||sa.includes("SOBRE")||sa.includes("FALSO"))?2 : 3;
                            const pb = (sb.includes("BUENA")||sb.includes("REBOTE"))?1 : (sb.includes("PREPARANDO")||sb.includes("SOBRE")||sb.includes("FALSO"))?2 : 3;
                            return pa - pb;
                        });
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

                    const alertas = data.alertas || [];
                    const lista = document.getElementById('lista-alertas');
                    if (alertas.length > 0) {
                        lista.innerHTML = alertas.map(a => `<div class="alerta-item"><div style="display:flex; justify-content:space-between; font-weight:bold; font-size:0.85rem;"><span><span class="flag-badge">${a.bandera}</span> ${a.symbol} - $${a.precio}</span><span style="font-size:0.70rem; color:#64748b;">${a.hora}</span></div><div style="font-size:0.78rem; margin-top:3px; cursor:pointer;" onclick="mostrarExplicacionEstado('${a.evento}')">🔔 <span style="text-decoration:underline;">${a.evento}</span></div><div style="display:flex; gap:6px; margin-top:6px;"><button onclick="agregarActivo('${a.symbol}')" style="font-size:0.68rem; padding:3px 6px;">+ Seguir</button><button onclick="usarParaOperar('${a.symbol}', ${a.precio}, ${a.sl || 0}, ${a.tp || 0})" style="font-size:0.68rem; padding:3px 6px; background:#10b981; color:#fff;">💼 Op</button><a href="${formatearLinkTV(a.symbol, mercadoEnUso)}" target="_blank" class="tv-btn" style="padding: 3px 6px;">📈 TV</a></div></div>`).join('');
                    } else { lista.innerHTML = `<span style="font-size:0.8rem; color:#94a3b8;">Sin alertas en ${mercadoEnUso}.</span>`; }

                    let cap = 0, pnl = 0;
                    const divC = document.getElementById('lista-cartera');
                    if (data.cartera.length > 0) {
                        divC.innerHTML = data.cartera.map(p => {
                            cap += p.inversion_total || 0; pnl += p.pnl_porcentaje || 0;
                            const cPnl = p.pnl_porcentaje >= 0 ? '#4ade80' : '#f87171';
                            let cSl = p.analisis_sl.includes("Correcto") ? '#4ade80' : '#f87171';
                            if (/Sube SL|Trailing|Asegura/.test(p.analisis_sl)) cSl = '#facc15';
                            return `<div class="alerta-item" style="border-left-color: ${cPnl};"><div style="display:flex; justify-content:space-between; font-weight:bold; font-size:0.9rem;"><span><span class="flag-badge">${p.bandera}</span> ${p.ticker}</span><span style="color:${cPnl};">${p.pnl_porcentaje>=0?'+':''}${p.pnl_porcentaje}%</span><button onclick="eliminarPosicion('${p.ticker}')" style="background:none;color:#ef4444;border:none;cursor:pointer;">✕</button></div><div style="font-size:0.8rem; margin-top:4px;">Entrada: $${p.precio_compra} | Actual: $${p.precio_actual} | TP: $${p.tp_usuario}</div><div style="font-size:0.8rem; color:#38bdf8; font-weight:bold;">Acciones: ${p.acciones||0} ($${p.inversion_total||0})</div><div style="font-size:0.8rem; font-weight:bold; color:${cSl}; cursor:pointer;" onclick="mostrarExplicacionEstado('${p.analisis_sl}')">SL Audit: <span style="text-decoration:underline;">${p.analisis_sl}</span> 🔍</div></div>`;
                        }).join('');
                        const pnlProm = (pnl / data.cartera.length).toFixed(2);
                        document.getElementById('resumen-cartera').innerHTML = `<span>Capital: <b>$${cap.toFixed(2)}</b></span><span>Rendimiento: <b style="color:${pnlProm>=0?'#4ade80':'#f87171'}">${pnlProm>=0?'+':''}${pnlProm}%</b></span>`;
                    } else { 
                        divC.innerHTML = `<span style="font-size:0.8rem; color:#94a3b8;">Sin posiciones.</span>`;
                        document.getElementById('resumen-cartera').innerHTML = `<span>Capital: <b>$0.00</b></span><span>Rendimiento: <b>0.00%</b></span>`;
                    }

                    const divSug = document.getElementById('lista-sugerencias');
                    if(data.sugerencias.length > 0) {
                        divSug.innerHTML = data.sugerencias.map(s => `<div style="background:#0b132b; padding:8px; border-radius:6px; margin-bottom:6px; border:1px solid #3a506b;"><div style="font-weight:bold; ${s.estado.includes('REBOTE')?'color:#c084fc;':'color:#4ade80;'} font-size:0.85rem;"><span class="flag-badge">${s.bandera}</span> ${s.ticker} a $${s.precio}</div><div style="font-size:0.75rem; color:#facc15; margin: 2px 0; cursor:pointer;" onclick="mostrarExplicacionEstado('${s.estado}')">📌 <span style="text-decoration:underline;">${s.estado}</span> 🔍</div><div style="display:flex; gap:6px; margin-top:6px;"><button onclick="agregarActivo('${s.ticker}')" style="font-size:0.68rem; padding:4px 6px;">+ Seguir</button><button onclick="usarParaOperar('${s.ticker}', ${s.precio}, ${s.sl}, ${s.tp})" style="font-size:0.68rem; padding:4px 6px; background:#10b981; color:#fff;">💼 Op</button><a href="${formatearLinkTV(s.ticker, mercadoEnUso)}" target="_blank" class="tv-btn">📈 TV</a></div></div>`).join('');
                    } else { divSug.innerHTML = `<span style="font-size:0.8rem; color:#94a3b8;">Buscando...</span>`; }

                    renderizarGridMercado();
                } catch (e) {}
            }

            function renderizarGridMercado() {
                const grid = document.getElementById('grid-mercado');
                if (ordenActivosGlobal.length > 0 && Object.keys(mercadoGlobalData).length > 0) {
                    grid.innerHTML = ordenActivosGlobal.map((ticker, idx) => {
                        const i = mercadoGlobalData[ticker];
                        if(!i) return '';
                        let clE = 'entrada-wait';
                        if (i.estado_entrada.includes("FALSO")) clE = 'entrada-warn';
                        else if (i.estado_entrada.includes("BUENA ENTRADA")) clE = 'entrada-ok';
                        else if (i.estado_entrada.includes("PREPARANDO")) clE = 'entrada-prep';
                        else if (i.estado_entrada.includes("REBOTE")) clE = 'entrada-rebote';

                        const tv = formatearLinkTV(ticker, mercadoEnUso);

                        if(modoLista) {
                            return `<div class="card"><div class="card-header" style="margin-bottom:0; width:170px;"><span class="ticker"><span class="flag-badge">${i.bandera}</span> ${ticker}</span></div><div class="price" style="width:75px;">$${i.precio}</div><div class="${clE}" style="width:160px;" onclick="mostrarExplicacionEstado('${i.estado_entrada}')">${i.estado_entrada} 🔍</div><div class="sparkline-container" style="width:80px; height:25px; margin-top:0;"><svg width="100%" height="25" viewBox="0 0 100 35" preserveAspectRatio="none"><polyline fill="none" stroke="${i.sparkline_color}" stroke-width="2" points="${i.sparkline}" /></svg></div><div class="list-actions-bar"><button onclick="usarParaOperar('${ticker}', ${i.precio}, ${i.soporte_tecnico}, ${i.tp_tecnico})" style="font-size:0.68rem; background:#10b981; color:#fff; padding:4px 6px;">💼 Op</button><a href="${tv}" target="_blank" class="tv-btn">📈 TV</a><button onclick="mostrarModal('${ticker}')" style="background:#3a506b; color:#fff; font-size:0.68rem; padding:4px 6px;">ℹ️</button><button class="btn-remove" onclick="eliminarActivo('${ticker}')">✕</button></div></div>`;
                        } else {
                            return `<div class="card"><div class="card-top-toolbar"><span style="font-size:0.7rem; color:#38bdf8; font-weight:bold;">Prioridad #${idx + 1}</span><button class="btn-remove" onclick="eliminarActivo('${ticker}')">✕ Eliminar</button></div><div class="card-header"><span class="ticker"><span class="flag-badge">${i.bandera}</span> ${ticker}</span><span class="badge ${i.tendencia === 'ALZA' ? 'bullish' : 'bearish'}">${i.tendencia}</span></div><div class="price">$${i.precio}</div><div class="${clE}" onclick="mostrarExplicacionEstado('${i.estado_entrada}')">${i.estado_entrada} 🔍</div><div class="sparkline-container"><svg width="100%" height="35" viewBox="0 0 100 35" preserveAspectRatio="none"><polyline fill="none" stroke="${i.sparkline_color}" stroke-width="2" points="${i.sparkline}" /></svg></div><div class="levels-box"><div class="stat"><span>🛡️ Soporte (SL):</span> <span class="sl-text">$${i.soporte_tecnico}</span></div><div class="stat"><span>🎯 TP Técnico:</span> <span class="tp-text">$${i.tp_tecnico}</span></div>
                            <!-- RESTAURACIÓN DE LAS SMA -->
                            <div class="stat" style="margin-top:6px; border-top:1px dashed #3a506b; padding-top:4px;"><span>📉 SMA 9:</span> <span>$${i.sma9}</span></div>
                            <div class="stat"><span>📈 SMA 21:</span> <span>$${i.sma21}</span></div>
                            <!-- RESTAURACIÓN DE LAS SMA -->
                            <div class="stat" style="margin-top:6px; border-top:1px dashed #3a506b; padding-top:4px;"><span>📊 RSI (14):</span> <span style="font-weight:bold; color:${i.rsi >= 70 ? '#ef4444' : (i.rsi <= 30 ? '#c084fc' : '#38bdf8')}">${i.rsi}</span></div><div class="stat"><span>🌊 Macro (1D):</span> <span style="font-weight:bold; color:${i.tendencia_macro === 'ALZA' ? '#4ade80' : '#f87171'}">${i.tendencia_macro}</span></div></div><div style="display:flex; gap:6px; margin-top:8px;"><button onclick="usarParaOperar('${ticker}', ${i.precio}, ${i.soporte_tecnico}, ${i.tp_tecnico})" style="flex:1; font-size:0.72rem; background:#10b981; color:#fff; padding:5px;">💼 Operar</button><a href="${tv}" target="_blank" class="tv-btn" style="flex:1;">📈 TV</a><button onclick="mostrarModal('${ticker}')" style="background:#3a506b; color:#fff; font-size:0.72rem; padding:4px 6px;">ℹ️ Info</button></div></div>`;
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
