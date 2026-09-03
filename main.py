# ==============================================================================
# REQUERIMIENTOS E INSTALACIÓN AUTOMÁTICA
# ==============================================================================
import os
import subprocess
import sys

# Crear requirements.txt automáticamente en el proyecto
requirements_content = """fastapi
uvicorn
yfinance
pandas-ta
pydantic
"""
if not os.path.exists("requirements.txt"):
  with open("requirements.txt", "w") as f:
    f.write(requirements_content)

# Verificar e instalar dependencias si no están presentes
try:
  import fastapi
  import pandas_ta
  import pydantic
  import uvicorn
  import yfinance
except ImportError:
  print("Instalando dependencias necesarias...")
  subprocess.check_call(
      [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"]
  )

# ==============================================================================
# CÓDIGO PRINCIPAL DE LA APLICACIÓN (main.py)
# ==============================================================================
import json
import threading
import time
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import pandas_ta as ta
import uvicorn
import yfinance as yf

app = FastAPI(title="Trading Monitor & Portfolio System")

ARCHIVADOR_JSON = "activos.json"
INTERVALO_SEGUNDOS = 60

# --- ESTRUCTURA DE DATOS EN MEMORIA ---
activos_monitoreados = []
cartera_posiciones = []
timeframe_actual = "1h"
estado_mercado = {}
historial_alertas = []


# --- CARGA Y GUARDADO EN ARCHIVO LOCAL (PERSISTENCIA) ---
def cargar_datos_locales():
  global activos_monitoreados, cartera_posiciones
  if os.path.exists(ARCHIVADOR_JSON):
    try:
      with open(ARCHIVADOR_JSON, "r") as f:
        data = json.load(f)
        activos_monitoreados = data.get(
            "activos", ["QQQ", "SPY", "NVDA", "TSLA", "BTC-USD"]
        )
        cartera_posiciones = data.get("cartera", [])
    except Exception as e:
      print(f"Error cargando JSON local: {e}")
      activos_monitoreados = ["QQQ", "SPY", "NVDA", "TSLA", "BTC-USD"]
      cartera_posiciones = []
  else:
    activos_monitoreados = ["QQQ", "SPY", "NVDA", "TSLA", "BTC-USD"]
    cartera_posiciones = []
    guardar_datos_locales()


def guardar_datos_locales():
  try:
    with open(ARCHIVADOR_JSON, "w") as f:
      json.dump(
          {"activos": activos_monitoreados, "cartera": cartera_posiciones},
          f,
          indent=4,
      )
  except Exception as e:
    print(f"Error guardando JSON local: {e}")


cargar_datos_locales()


# --- MODELOS PYDANTIC ---
class TickerModel(BaseModel):
  ticker: str


class TimeframeModel(BaseModel):
  timeframe: str


class PosicionModel(BaseModel):
  ticker: str
  precio_compra: float
  timeframe: str


# --- MOTOR DE ANÁLISIS EN SEGUNDO PLANO ---
def obtener_config_timeframe(tf: str):
  if tf == "4h":
    return "60d", "60m"
  elif tf == "1d":
    return "6mo", "1d"
  else:
    return "1mo", "1h"


def calcular_risk_score(df):
  try:
    returns = df["Close"].pct_change().dropna()
    volatilidad = returns.std() * (252**0.5)
    if volatilidad < 0.15:
      return "1/5 (Bajo)"
    elif volatilidad < 0.25:
      return "2/5 (Moderado)"
    elif volatilidad < 0.40:
      return "3/5 (Medio)"
    elif volatilidad < 0.60:
      return "4/5 (Alto)"
    else:
      return "5/5 (Especulativo)"
  except Exception:
    return "3/5 (Medio)"


def analizar_mercado():
  global estado_mercado, historial_alertas, activos_monitoreados, timeframe_actual, cartera_posiciones
  while True:
    lista_actual = list(activos_monitoreados)
    tf_local = timeframe_actual
    periodo, intervalo = obtener_config_timeframe(tf_local)

    for symbol in lista_actual:
      try:
        df = yf.download(
            tickers=symbol, period=periodo, interval=intervalo, progress=False
        )

        if not df.empty:
          if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
            df.columns = df.columns.get_level_values(0)

          if tf_local == "4h" and len(df) >= 4:
            df = df.resample("4h").agg({
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }).dropna()

          if len(df) >= 20:
            df["SMA_9"] = ta.sma(df["Close"], length=9)
            df["SMA_21"] = ta.sma(df["Close"], length=21)
            df["Resistencia"] = df["High"].rolling(window=20).max().shift(1)
            df["Soporte_SL"] = df["Low"].rolling(window=10).min().shift(1)

            ultima = df.iloc[-1]
            anterior = df.iloc[-2]

            precio = round(float(ultima["Close"]), 2)
            resistencia = round(float(anterior["Resistencia"]), 2)
            soporte_reciente = round(float(anterior["Soporte_SL"]), 2)
            sma9 = round(float(ultima["SMA_9"]), 2)
            sma21 = round(float(ultima["SMA_21"]), 2)

            tendencia = "BULLISH" if sma9 > sma21 else "BEARISH"
            hora_actual = datetime.now().strftime("%H:%M:%S")
            risk_score = calcular_risk_score(df)

            stop_loss = round(soporte_reciente * 0.998, 2)
            riesgo = precio - stop_loss
            take_profit = (
                round(precio + (riesgo * 2), 2)
                if riesgo > 0
                else round(precio * 1.02, 2)
            )

            es_buena_entrada = (precio > resistencia) and (tendencia == "BULLISH")
            estado_entrada = (
                "🟢 BUENA ENTRADA" if es_buena_entrada else "⏳ ESPERAR"
            )

            clean_symbol = symbol.replace("-USD", "").replace("=", "")
            tv_link = f"https://www.tradingview.com/symbols/{clean_symbol}/"

            estado_mercado[symbol] = {
                "precio": precio,
                "resistencia": resistencia,
                "sma9": sma9,
                "sma21": sma21,
                "tendencia": tendencia,
                "estado_entrada": estado_entrada,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "risk_score": risk_score,
                "timeframe": tf_local.upper(),
                "tv_link": tv_link,
                "ultima_actualizacion": hora_actual,
            }

            if es_buena_entrada:
              evento = (
                  f"🟢 SEÑAL DE COMPRA | SL: ${stop_loss} | TP:"
                  f" ${take_profit}"
              )
              _registrar_alerta_si_nueva(symbol, evento, precio, hora_actual)

            _evaluar_cartera(symbol, precio, sma9, sma21, hora_actual)

      except Exception as e:
        print(f"Error analizando {symbol}: {e}")
      time.sleep(1)

    time.sleep(INTERVALO_SEGUNDOS)


def _evaluar_cartera(symbol, precio_actual, sma9, sma21, hora):
  global cartera_posiciones
  for pos in cartera_posiciones:
    if pos["ticker"] == symbol:
      p_compra = pos["precio_compra"]
      p_ganancia = ((precio_actual - p_compra) / p_compra) * 100
      sl_sugerido = round(precio_actual * 0.98, 2)

      if sma9 < sma21:
        estado_pos = "⚠️ DEBILIDAD (Giro Bajista)"
        _registrar_alerta_si_nueva(
            symbol,
            f"⚠️ ALERTA CARTERA: Pérdida de impulso en {symbol}.",
            precio_actual,
            hora,
        )
      elif p_ganancia >= 2.0:
        estado_pos = "🟢 EN GANANCIA (Asegurar SL)"
        sl_sugerido = max(p_compra, sl_sugerido)
      else:
        estado_pos = "🔵 MANTENER"

      pos["precio_actual"] = precio_actual
      pos["pnl_porcentaje"] = round(p_ganancia, 2)
      pos["sl_sugerido"] = sl_sugerido
      pos["estado"] = estado_pos


def _registrar_alerta_si_nueva(symbol, evento, precio, hora):
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
            "id": len(historial_alertas) + 1,
        },
    )
    historial_alertas = historial_alertas[:30]


threading.Thread(target=analizar_mercado, daemon=True).start()


# --- API ENDPOINTS ---
@app.get("/api/data")
def obtener_datos():
  return {
      "mercado": estado_mercado,
      "alertas": historial_alertas,
      "cartera": cartera_posiciones,
      "timeframe": timeframe_actual,
  }


@app.post("/api/add")
def agregar_activo(item: TickerModel):
  symbol = item.ticker.strip().upper()
  if symbol and symbol not in activos_monitoreados:
    activos_monitoreados.append(symbol)
    guardar_datos_locales()
  return {"status": "ok"}


@app.post("/api/remove")
def eliminar_activo(item: TickerModel):
  symbol = item.ticker.strip().upper()
  if symbol in activos_monitoreados:
    activos_monitoreados.remove(symbol)
    if symbol in estado_mercado:
      del estado_mercado[symbol]
    guardar_datos_locales()
  return {"status": "ok"}


@app.post("/api/cartera/add")
def agregar_cartera(item: PosicionModel):
  global cartera_posiciones
  ticker = item.ticker.strip().upper()
  cartera_posiciones = [p for p in cartera_posiciones if p["ticker"] != ticker]
  cartera_posiciones.append({
      "ticker": ticker,
      "precio_compra": item.precio_compra,
      "timeframe": item.timeframe,
      "precio_actual": item.precio_compra,
      "pnl_porcentaje": 0.0,
      "sl_sugerido": round(item.precio_compra * 0.98, 2),
      "estado": "🔵 MANTENER",
  })
  guardar_datos_locales()
  return {"status": "ok"}


@app.post("/api/cartera/remove")
def eliminar_cartera(item: TickerModel):
  global cartera_posiciones
  ticker = item.ticker.strip().upper()
  cartera_posiciones = [p for p in cartera_posiciones if p["ticker"] != ticker]
  guardar_datos_locales()
  return {"status": "ok"}


@app.post("/api/timeframe")
def cambiar_timeframe(item: TimeframeModel):
  global timeframe_actual, estado_mercado
  if item.timeframe in ["1h", "4h", "1d"]:
    timeframe_actual = item.timeframe
    estado_mercado = {}
  return {"status": "ok", "timeframe": timeframe_actual}


# --- INTERFAZ WEB RESPONSIVE ---
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
            h1 { text-align: center; color: #38bdf8; font-size: 1.6rem; margin: 10px 0 20px 0; }
            
            .control-panel { max-width: 1200px; margin: 0 auto 16px auto; background: #1c2541; padding: 12px; border-radius: 10px; display: flex; gap: 8px; align-items: center; justify-content: center; flex-wrap: wrap; border: 1px solid #3a506b; }
            input[type="text"], input[type="number"], select { background: #0b132b; border: 1px solid #3a506b; color: #fff; padding: 10px; border-radius: 6px; font-size: 0.9rem; }
            input[type="text"] { width: 120px; text-transform: uppercase; }
            select { cursor: pointer; font-weight: bold; color: #38bdf8; }
            button { background: #38bdf8; color: #0b132b; border: none; padding: 10px 14px; font-weight: bold; border-radius: 6px; cursor: pointer; }
            button:hover { background: #7dd3fc; }

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
            .tv-link { color: #38bdf8; text-decoration: none; font-size: 0.75rem; font-weight: bold; float: right; margin-top: 4px; }
            .tv-link:hover { text-decoration: underline; }
            .btn-remove { position: absolute; top: 10px; right: 10px; background: transparent; color: #ef4444; border: none; font-size: 1.1rem; cursor: pointer; }
            
            .feed-panel, .cartera-panel { background: #1c2541; border-radius: 10px; padding: 14px; border: 1px solid #3a506b; margin-bottom: 16px; }
            .feed-title { font-size: 1rem; color: #38bdf8; margin-top: 0; margin-bottom: 10px; border-bottom: 1px solid #3a506b; padding-bottom: 6px; }
            .alerta-item { background: #0b132b; border-left: 4px solid #38bdf8; padding: 8px; margin-bottom: 6px; border-radius: 4px; }
            .alerta-header { display: flex; justify-content: space-between; font-weight: bold; font-size: 0.85rem; }
            .alerta-evento { font-size: 0.78rem; color: #cbd5e1; margin-top: 3px; }
        </style>
    </head>
    <body>
        <h1>📊 Trading Monitor Pro</h1>
        
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
                    <div class="feed-title">💼 Mi Cartera / Posiciones Activas</div>
                    <div style="display:flex; gap:6px; margin-bottom:10px;">
                        <input type="text" id="c-ticker" placeholder="Ej: AAPL" style="width:90px;" />
                        <input type="number" id="c-precio" placeholder="Precio $" style="width:100px;" step="any" />
                        <button onclick="registrarPosicion()">Ingresar Posición</button>
                    </div>
                    <div id="lista-cartera">Sin posiciones en seguimiento.</div>
                </div>

                <h3>Activos bajo Monitoreo</h3>
                <div class="grid-activos" id="grid-mercado">Cargando datos...</div>
            </div>
            
            <div>
                <div class="feed-panel">
                    <div class="feed-title">🚨 Feed de Señales en Vivo</div>
                    <div id="lista-alertas">Sin señales recientes.</div>
                </div>
            </div>
        </div>

        <script>
            async function cambiarTimeframe(tf) {
                await fetch('/api/timeframe', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ timeframe: tf })
                });
                actualizarApp();
            }

            async function agregarActivo() {
                const input = document.getElementById('new-ticker');
                const ticker = input.value.trim();
                if (!ticker) return;
                await fetch('/api/add', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ ticker: ticker })
                });
                input.value = '';
                actualizarApp();
            }

            async function eliminarActivo(ticker) {
                await fetch('/api/remove', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ ticker: ticker })
                });
                actualizarApp();
            }

            async function registrarPosicion() {
                const ticker = document.getElementById('c-ticker').value.trim();
                const precio = parseFloat(document.getElementById('c-precio').value);
                const tf = document.getElementById('select-tf').value;
                if (!ticker || isNaN(precio)) return;

                await fetch('/api/cartera/add', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ ticker: ticker, precio_compra: precio, timeframe: tf })
                });
                document.getElementById('c-ticker').value = '';
                document.getElementById('c-precio').value = '';
                actualizarApp();
            }

            async function eliminarPosicion(ticker) {
                await fetch('/api/cartera/remove', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ ticker: ticker })
                });
                actualizarApp();
            }

            async function actualizarApp() {
                try {
                    const res = await fetch('/api/data');
                    const { mercado, alertas, cartera, timeframe } = await res.json();
                    
                    document.getElementById('select-tf').value = timeframe;

                    const divCartera = document.getElementById('lista-cartera');
                    if (cartera.length > 0) {
                        divCartera.innerHTML = '';
                        cartera.forEach(p => {
                            const pnlColor = p.pnl_porcentaje >= 0 ? '#4ade80' : '#f87171';
                            divCartera.innerHTML += `
                                <div class="alerta-item" style="border-left-color: ${pnlColor};">
                                    <div class="alerta-header">
                                        <span>${p.ticker} (${p.timeframe.toUpperCase()})</span>
                                        <span style="color:${pnlColor};">${p.pnl_porcentaje >= 0 ? '+' : ''}${p.pnl_porcentaje}%</span>
                                        <button onclick="eliminarPosicion('${p.ticker}')" style="background:none;color:#ef4444;border:none;cursor:pointer;">✕</button>
                                    </div>
                                    <div class="alerta-evento">Entrada: $${p.precio_compra} | Actual: $${p.precio_actual}</div>
                                    <div class="alerta-evento" style="font-weight:bold;margin-top:2px;">${p.estado} | SL Sugerido: <span style="color:#4ade80;">$${p.sl_sugerido}</span></div>
                                </div>
                            `;
                        });
                    } else {
                        divCartera.innerHTML = '<span style="color:#94a3b8;font-size:0.8rem;">Sin posiciones abiertas ingresadas.</span>';
                    }

                    const grid = document.getElementById('grid-mercado');
                    if (Object.keys(mercado).length > 0) {
                        grid.innerHTML = '';
                        for (const [ticker, info] of Object.entries(mercado)) {
                            const isBull = info.tendencia === 'BULLISH';
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
                                        <div class="stat"><span>🛡️ Stop Loss:</span> <span class="sl-text">$${info.stop_loss}</span></div>
                                        <div class="stat"><span>🎯 Take Profit:</span> <span class="tp-text">$${info.take_profit}</span></div>
                                    </div>

                                    <div class="stat" style="margin-top:6px;"><span>Riesgo Volatilidad:</span> <span>${info.risk_score}</span></div>
                                    <div class="stat"><span>SMA 9 / 21:</span> <span>$${info.sma9} / $${info.sma21}</span></div>
                                    <a class="tv-link" href="${info.tv_link}" target="_blank">📈 Ver Gráfico TradingView</a>
                                </div>
                            `;
                        }
                    } else {
                        grid.innerHTML = '<p style="color:#94a3b8;">Analizando activos...</p>';
                    }

                    const lista = document.getElementById('lista-alertas');
                    if (alertas.length > 0) {
                        lista.innerHTML = '';
                        alertas.forEach(a => {
                            lista.innerHTML += `
                                <div class="alerta-item">
                                    <div class="alerta-header">
                                        <span>${a.symbol} - $${a.precio}</span>
                                        <span style="font-size:0.7rem;color:#64748b;">${a.hora}</span>
                                    </div>
                                    <div class="alerta-evento">${a.evento}</div>
                                </div>
                            `;
                        });
                    }
                } catch (e) {
                    console.error(e);
                }
            }

            actualizarApp();
            setInterval(actualizarApp, 5000);
        </script>
    </body>
    </html>
    """


if __name__ == "__main__":
  uvicorn.run(app, host="0.0.0.0", port=8000)
