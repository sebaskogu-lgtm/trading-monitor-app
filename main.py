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
except ImportError:
  subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

# ==============================================================================
# CÓDIGO PRINCIPAL
# ==============================================================================
import json
import sqlite3
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import pandas as pd
import yfinance as yf
import pytz

app = FastAPI(title="Trading Monitor Pro")

DB_FILE = "trading_data.db"
INTERVALO_SEGUNDOS = 60
TICKERS_ESCANER = ["AAPL", "MSFT", "AMZN", "META", "GOOGL", "NFLX", "AMD", "COIN", "TSLA", "NVDA", "MSTR", "PLTR"]

# --- PERSISTENCIA SQLITE ---
def init_db():
  conn = sqlite3.connect(DB_FILE)
  cursor = conn.cursor()
  cursor.execute("""
        CREATE TABLE IF NOT EXISTS configuracion (
            id INTEGER PRIMARY KEY,
            activos TEXT,
            cartera TEXT
        )
    """)
  cursor.execute("SELECT COUNT(*) FROM configuracion")
  if cursor.fetchone()[0] == 0:
    cursor.execute(
        "INSERT INTO configuracion (id, activos, cartera) VALUES (1, ?, ?)",
        (json.dumps(["QQQ", "SPY"]), json.dumps([])),
    )
  conn.commit()
  conn.close()

init_db()

# --- ESTADO EN MEMORIA ---
timeframe_actual = "1h"
estado_mercado = {}
historial_alertas = []
recomendaciones_escaner = []

def db_get(campo):
  conn = sqlite3.connect(DB_FILE)
  cursor = conn.cursor()
  cursor.execute(f"SELECT {campo} FROM configuracion WHERE id=1")
  row = cursor.fetchone()
  conn.close()
  return json.loads(row[0]) if row else []

def db_set(campo, valor):
  conn = sqlite3.connect(DB_FILE)
  cursor = conn.cursor()
  cursor.execute(f"UPDATE configuracion SET {campo} = ? WHERE id=1", (json.dumps(valor),))
  conn.commit()
  conn.close()

# --- MODELOS ---
class TickerModel(BaseModel):
  ticker: str
class TimeframeModel(BaseModel):
  timeframe: str
class PosicionModel(BaseModel):
  ticker: str
  precio_compra: float
  sl_usuario: float
  tp_usuario: float
  timeframe: str

# --- RELOJ Y HORARIO NYSE ---
def estado_horario():
  ny_tz = pytz.timezone('America/New_York')
  ny_time = datetime.now(ny_tz)
  if ny_time.weekday() > 4: 
      return "🔴 CERRADO (Fin de semana)"
  
  m_open = ny_time.replace(hour=9, minute=30, second=0, microsecond=0)
  m_close = ny_time.replace(hour=16, minute=0, second=0, microsecond=0)
  pre_close = ny_time.replace(hour=15, minute=30, second=0, microsecond=0)

  if ny_time < m_open or ny_time > m_close:
      return "🔴 CERRADO"
  elif ny_time >= pre_close:
      return "⚠️ PRE-CIERRE (ALERTA: Cierra intradiarias en 30 min)"
  else:
      return "🟢 ABIERTO"

# --- MOTOR DE ANÁLISIS ---
def obtener_config_tf(tf: str):
  if tf == "4h": return "60d", "60m"
  elif tf == "1d": return "6mo", "1d"
  return "1mo", "1h"

def procesar_ticker(symbol, tf_local):
  periodo, intervalo = obtener_config_tf(tf_local)
  try:
    df = yf.download(tickers=symbol, period=periodo, interval=intervalo, progress=False)
    if df.empty: return None
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
      df.columns = df.columns.get_level_values(0)

    if tf_local == "4h" and len(df) >= 4:
      df = df.resample("4h").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"}).dropna()

    if len(df) >= 20:
      df["SMA_9"] = df["Close"].rolling(window=9).mean()
      df["SMA_21"] = df["Close"].rolling(window=21).mean()
      df["Resistencia"] = df["High"].rolling(window=20).max().shift(1)
      df["Soporte_SL"] = df["Low"].rolling(window=10).min().shift(1)
      df["ATR"] = df["High"] - df["Low"]
      atr_medio = df["ATR"].rolling(14).mean().iloc[-1]

      ultima = df.iloc[-1]
      anterior = df.iloc[-2]

      precio = round(float(ultima["Close"]), 2)
      resistencia = round(float(anterior["Resistencia"]), 2)
      soporte_tecnico = round(float(anterior["Soporte_SL"]), 2)
      sma9 = round(float(ultima["SMA_9"]), 2)
      sma21 = round(float(ultima["SMA_21"]), 2)

      tendencia = "ALZA" if sma9 > sma21 else "BAJA"
      hora = datetime.now().strftime("%H:%M:%S")

      # Cálculo técnico puro
      riesgo = precio - soporte_tecnico
      tp_tecnico = round(precio + (riesgo * 2), 2) if riesgo > 0 else round(precio * 1.02, 2)
      es_buena = (precio > resistencia) and (tendencia == "ALZA")

      return {
          "symbol": symbol,
          "precio": precio,
          "resistencia": resistencia,
          "soporte_tecnico": soporte_tecnico,
          "tp_tecnico": tp_tecnico,
          "sma9": sma9,
          "sma21": sma21,
          "tendencia": tendencia,
          "estado_entrada": "🟢 BUENA ENTRADA" if es_buena else "⏳ ESPERAR",
          "atr": atr_medio,
          "hora": hora
      }
  except Exception:
    pass
  return None

def escaneo_autonomo():
  global recomendaciones_escaner
  while True:
    buenas = []
    with ThreadPoolExecutor(max_workers=5) as executor:
      resultados = executor.map(lambda s: procesar_ticker(s, "1h"), TICKERS_ESCANER)
    
    for r in resultados:
      if r and "BUENA ENTRADA" in r["estado_entrada"]:
        buenas.append(f"{r['symbol']} (${r['precio']}) - Ruptura en 1H. TP Objetivo: ${r['tp_tecnico']}")
    
    recomendaciones_escaner = buenas[:3] # Mostrar maximo 3
    time.sleep(300) # Escanea cada 5 minutos

def analizar_mercado():
  global estado_mercado, historial_alertas, timeframe_actual
  while True:
    lista_actual = db_get("activos")
    tf_local = timeframe_actual

    with ThreadPoolExecutor(max_workers=5) as executor:
      resultados = executor.map(lambda s: procesar_ticker(s, tf_local), lista_actual)

    nuevo_estado = {}
    for r in resultados:
      if r:
        sym = r["symbol"]
        nuevo_estado[sym] = r
        if "BUENA ENTRADA" in r["estado_entrada"]:
          _registrar_alerta(sym, f"🟢 SEÑAL DE COMPRA | Objetivo: ${r['tp_tecnico']}", r["precio"], r["hora"])
        
        _evaluar_cartera(sym, r["precio"], r["sma9"], r["sma21"], r["soporte_tecnico"], r["atr"], r["hora"])
    
    estado_mercado = nuevo_estado
    time.sleep(INTERVALO_SEGUNDOS)

def _evaluar_cartera(symbol, precio_actual, sma9, sma21, soporte_tecnico, atr, hora):
  cartera = db_get("cartera")
  modificado = False
  for pos in cartera:
    if pos["ticker"] == symbol:
      p_compra = pos["precio_compra"]
      sl_user = pos["sl_usuario"]
      tp_user = pos["tp_usuario"]
      
      p_ganancia = ((precio_actual - p_compra) / p_compra) * 100
      estado_pos = "🔵 MANTENER"

      if sma9 < sma21:
        estado_pos = "⚠️ GIRO A LA BAJA (Evaluar salida)"
        _registrar_alerta(symbol, f"⚠️ ALERTA CARTERA: Pérdida de impulso.", precio_actual, hora)
      elif p_ganancia >= 2.0:
        estado_pos = "🟢 EN GANANCIA"

      # Inteligencia sobre el SL del usuario
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
  if not historial_alertas or historial_alertas[0]["symbol"] != symbol or historial_alertas[0]["evento"] != evento:
    historial_alertas.insert(0, {"symbol": symbol, "evento": evento, "precio": precio, "hora": hora})
    historial_alertas = historial_alertas[:20]

threading.Thread(target=analizar_mercado, daemon=True).start()
threading.Thread(target=escaneo_autonomo, daemon=True).start()

# --- API ---
@app.get("/api/data")
def obtener_datos():
  return {
      "mercado": estado_mercado,
      "alertas": historial_alertas,
      "cartera": db_get("cartera"),
      "timeframe": timeframe_actual,
      "horario": estado_horario(),
      "sugerencias": recomendaciones_escaner
  }

@app.post("/api/add")
def agregar_activo(item: TickerModel):
  activos = db_get("activos")
  symbol = item.ticker.strip().upper()
  if symbol and symbol not in activos:
    activos.append(symbol)
    db_set("activos", activos)
  return {"status": "ok"}

@app.post("/api/remove")
def eliminar_activo(item: TickerModel):
  activos = db_get("activos")
  symbol = item.ticker.strip().upper()
  if symbol in activos:
    activos.remove(symbol)
    db_set("activos", activos)
  return {"status": "ok"}

@app.post("/api/cartera/add")
def agregar_cartera(item: PosicionModel):
  cartera = db_get("cartera")
  ticker = item.ticker.strip().upper()
  cartera = [p for p in cartera if p["ticker"] != ticker]
  cartera.append({
      "ticker": ticker,
      "precio_compra": item.precio_compra,
      "sl_usuario": item.sl_usuario,
      "tp_usuario": item.tp_usuario,
      "timeframe": item.timeframe,
      "precio_actual": item.precio_compra,
      "pnl_porcentaje": 0.0,
      "estado": "🔵 MANTENER",
      "analisis_sl": "Analizando..."
  })
  db_set("cartera", cartera)
  return {"status": "ok"}

@app.post("/api/cartera/remove")
def eliminar_cartera(item: TickerModel):
  cartera = db_get("cartera")
  ticker = item.ticker.strip().upper()
  cartera = [p for p in cartera if p["ticker"] != ticker]
  db_set("cartera", cartera)
  return {"status": "ok"}

@app.post("/api/timeframe")
def cambiar_timeframe(item: TimeframeModel):
  global timeframe_actual, estado_mercado
  if item.timeframe in ["1h", "4h", "1d"]:
    timeframe_actual = item.timeframe
    estado_mercado = {} # Fuerza limpieza para recalcular
  return {"status": "ok"}

# --- HTML ---
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
            .reloj { text-align: center; font-weight: bold; margin-bottom: 20px; font-size: 0.9rem; }
            
            .control-panel { max-width: 1200px; margin: 0 auto 16px auto; background: #1c2541; padding: 12px; border-radius: 10px; display: flex; gap: 8px; align-items: center; justify-content: center; flex-wrap: wrap; border: 1px solid #3a506b; }
            input[type="text"], input[type="number"], select { background: #0b132b; border: 1px solid #3a506b; color: #fff; padding: 8px; border-radius: 6px; font-size: 0.9rem; }
            input[type="text"] { width: 100px; text-transform: uppercase; }
            button { background: #38bdf8; color: #0b132b; border: none; padding: 8px 12px; font-weight: bold; border-radius: 6px; cursor: pointer; }
            
            .container { max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1fr; gap: 16px; }
            @media (max-width: 850px) { .container { grid-template-columns: 1fr; } }
            
            .grid-activos { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; }
            .card { background: #1c2541; border-radius: 10px; padding: 14px; border: 1px solid #3a506b; position: relative; }
            .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
            .ticker { font-weight: bold; font-size: 1.1rem; }
            .price { font-size: 1.4rem; font-weight: 800; margin-bottom: 6px; }
            
            .badge { padding: 3px 6px; border-radius: 10px; font-size: 0.68rem; font-weight: bold; }
            .bullish { background: rgba(34, 197, 94, 0.2); color: #4ade80; border: 1px solid #22c55e; }
            .bearish { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #ef4444; }
            .entrada-ok { background: rgba(34, 197, 94, 0.3); color: #4ade80; font-weight: bold; padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; display: inline-block; margin-bottom: 8px; }
            .entrada-wait { background: rgba(148, 163, 184, 0.1); color: #94a3b8; padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; display: inline-block; margin-bottom: 8px; }
            
            .stat { display: flex; justify-content: space-between; margin-top: 5px; font-size: 0.82rem; color: #cbd5e1; }
            .levels-box { background: #0b132b; padding: 8px; border-radius: 6px; margin-top: 6px; border: 1px solid #3a506b; }
            .sl-text { color: #f87171; font-weight: bold; }
            .tp-text { color: #4ade80; font-weight: bold; }
            .btn-remove { position: absolute; top: 10px; right: 10px; background: transparent; color: #ef4444; border: none; font-size: 1.1rem; cursor: pointer; }
            
            .feed-panel, .cartera-panel { background: #1c2541; border-radius: 10px; padding: 14px; border: 1px solid #3a506b; margin-bottom: 16px; }
            .feed-title { font-size: 1rem; color: #38bdf8; margin-bottom: 10px; border-bottom: 1px solid #3a506b; padding-bottom: 6px; }
            .alerta-item { background: #0b132b; border-left: 4px solid #38bdf8; padding: 8px; margin-bottom: 6px; border-radius: 4px; }
        </style>
    </head>
    <body>
        <h1>📊 Trading Monitor Pro</h1>
        <div id="reloj-mercado" class="reloj">Cargando estado del mercado...</div>
        
        <div class="control-panel">
            <input type="text" id="new-ticker" placeholder="TICKER" />
            <button onclick="agregarActivo()">+ Seguir</button>
            <select id="select-tf" onchange="cambiarTimeframe(this.value)">
                <option value="1h">1H (Hora)</option>
                <option value="4h">4H (Swing)</option>
                <option value="1d">1D (Diario)</option>
            </select>
        </div>

        <div class="container">
            <div>
                <div class="cartera-panel">
                    <div class="feed-title">💼 Mi Cartera / Posiciones</div>
                    <div style="display:flex; flex-wrap:wrap; gap:6px; margin-bottom:10px;">
                        <input type="text" id="c-ticker" placeholder="Activo" style="width:70px;" />
                        <input type="number" id="c-precio" placeholder="Entrada $" style="width:90px;" step="any" />
                        <input type="number" id="c-sl" placeholder="Tu SL $" style="width:90px;" step="any" />
                        <input type="number" id="c-tp" placeholder="Tu TP $" style="width:90px;" step="any" />
                        <button onclick="registrarPosicion()">Guardar</button>
                    </div>
                    <div id="lista-cartera">Sin posiciones.</div>
                </div>

                <h3>Activos bajo Monitoreo</h3>
                <div class="grid-activos" id="grid-mercado">Procesando datos en paralelo...</div>
            </div>
            
            <div>
                <div class="feed-panel">
                    <div class="feed-title">🤖 Escáner Autónomo</div>
                    <div id="lista-sugerencias" style="font-size:0.85rem; color:#cbd5e1;">Buscando oportunidades en Wall Street...</div>
                </div>

                <div class="feed-panel">
                    <div class="feed-title">🚨 Feed de Señales</div>
                    <div id="lista-alertas">Sin señales recientes.</div>
                </div>
            </div>
        </div>

        <script>
            async function cambiarTimeframe(tf) {
                document.getElementById('grid-mercado').innerHTML = '<p>Recalculando niveles...</p>';
                await fetch('/api/timeframe', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ timeframe: tf })
                });
                actualizarApp(); // Fuerza refresco instantaneo
            }

            async function agregarActivo() {
                const ticker = document.getElementById('new-ticker').value.trim();
                if (!ticker) return;
                await fetch('/api/add', {
                    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker })
                });
                document.getElementById('new-ticker').value = '';
                actualizarApp();
            }

            async function eliminarActivo(ticker) {
                await fetch('/api/remove', {
                    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker })
                });
                actualizarApp();
            }

            async function registrarPosicion() {
                const ticker = document.getElementById('c-ticker').value.trim();
                const precio = parseFloat(document.getElementById('c-precio').value);
                const sl = parseFloat(document.getElementById('c-sl').value);
                const tp = parseFloat(document.getElementById('c-tp').value);
                const tf = document.getElementById('select-tf').value;
                if (!ticker || isNaN(precio) || isNaN(sl) || isNaN(tp)) return;

                await fetch('/api/cartera/add', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ ticker: ticker, precio_compra: precio, sl_usuario: sl, tp_usuario: tp, timeframe: tf })
                });
                document.getElementById('c-ticker').value = '';
                document.getElementById('c-precio').value = '';
                document.getElementById('c-sl').value = '';
                document.getElementById('c-tp').value = '';
                actualizarApp();
            }

            async function eliminarPosicion(ticker) {
                await fetch('/api/cartera/remove', {
                    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker })
                });
                actualizarApp();
            }

            async function actualizarApp() {
                try {
                    const res = await fetch('/api/data');
                    const { mercado, alertas, cartera, timeframe, horario, sugerencias } = await res.json();
                    
                    document.getElementById('select-tf').value = timeframe;
                    document.getElementById('reloj-mercado').innerHTML = horario;
                    if(horario.includes("CERRADO") || horario.includes("PRE-CIERRE")) {
                        document.getElementById('reloj-mercado').style.color = '#f87171';
                    } else {
                        document.getElementById('reloj-mercado').style.color = '#4ade80';
                    }

                    const divCartera = document.getElementById('lista-cartera');
                    if (cartera.length > 0) {
                        divCartera.innerHTML = '';
                        cartera.forEach(p => {
                            const pnlColor = p.pnl_porcentaje >= 0 ? '#4ade80' : '#f87171';
                            const slColor = p.analisis_sl.includes("Correcto") ? '#4ade80' : '#f87171';
                            divCartera.innerHTML += `
                                <div class="alerta-item" style="border-left-color: ${pnlColor};">
                                    <div style="display:flex; justify-content:space-between; font-weight:bold; font-size:0.9rem;">
                                        <span>${p.ticker} (${p.timeframe.toUpperCase()})</span>
                                        <span style="color:${pnlColor};">${p.pnl_porcentaje >= 0 ? '+' : ''}${p.pnl_porcentaje}%</span>
                                        <button onclick="eliminarPosicion('${p.ticker}')" style="background:none;color:#ef4444;border:none;cursor:pointer;">✕</button>
                                    </div>
                                    <div style="font-size:0.8rem; margin-top:4px;">Entrada: $${p.precio_compra} | TP: $${p.tp_usuario}</div>
                                    <div style="font-size:0.8rem; margin-top:2px;">Estado: ${p.estado}</div>
                                    <div style="font-size:0.8rem; margin-top:2px; font-weight:bold; color:${slColor};">Analisis SL: ${p.analisis_sl} (Tu SL: $${p.sl_usuario})</div>
                                </div>
                            `;
                        });
                    } else { divCartera.innerHTML = '<span style="font-size:0.8rem;">Sin posiciones.</span>'; }

                    const divSug = document.getElementById('lista-sugerencias');
                    if(sugerencias.length > 0) {
                        divSug.innerHTML = sugerencias.map(s => `<div style="margin-bottom:6px; color:#4ade80;">⭐ ${s}</div>`).join('');
                    }

                    const grid = document.getElementById('grid-mercado');
                    if (Object.keys(mercado).length > 0) {
                        grid.innerHTML = '';
                        for (const [ticker, info] of Object.entries(mercado)) {
                            const isBull = info.tendencia === 'ALZA';
                            const isGoodEntry = info.estado_entrada.includes("BUENA ENTRADA");
                            grid.innerHTML += `
                                <div class="card">
                                    <button class="btn-remove" onclick="eliminarActivo('${ticker}')">✕</button>
                                    <div class="card-header" style="padding-right: 20px;">
                                        <span class="ticker">${ticker}</span>
                                        <span class="badge ${isBull ? 'bullish' : 'bearish'}">${info.tendencia}</span>
                                    </div>
                                    <div class="price">$${info.precio}</div>
                                    <div class="${isGoodEntry ? 'entrada-ok' : 'entrada-wait'}">${info.estado_entrada}</div>
                                    <div class="levels-box">
                                        <div class="stat"><span>🛡️ Soporte (SL Ideal):</span> <span class="sl-text">$${info.soporte_tecnico}</span></div>
                                        <div class="stat"><span>🎯 TP Técnico (1:2):</span> <span class="tp-text">$${info.tp_tecnico}</span></div>
                                    </div>
                                    <div class="stat" style="margin-top:6px;"><span>SMA 9 / 21:</span> <span>$${info.sma9} / $${info.sma21}</span></div>
                                </div>
                            `;
                        }
                    }

                    const lista = document.getElementById('lista-alertas');
                    if (alertas.length > 0) {
                        lista.innerHTML = alertas.map(a => `
                            <div class="alerta-item">
                                <div style="display:flex; justify-content:space-between; font-weight:bold; font-size:0.85rem;">
                                    <span>${a.symbol} - $${a.precio}</span><span style="font-size:0.7rem;">${a.hora}</span>
                                </div>
                                <div style="font-size:0.78rem; margin-top:3px;">${a.evento}</div>
                            </div>
                        `).join('');
                    }
                } catch (e) { console.error(e); }
            }
            actualizarApp();
            setInterval(actualizarApp, 5000); // Polling frontend
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
  port = int(os.environ.get("PORT", 8000))
  uvicorn.run(app, host="0.0.0.0", port=port)
