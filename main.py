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
# CÓDIGO PRINCIPAL
# ==============================================================================
import asyncio
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from pydantic import BaseModel
import pandas as pd
import psycopg2
import pytz
import yfinance as yf

app = FastAPI(title="Trading Monitor Pro")

INTERVALO_SEGUNDOS = 60
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

ADMIN_USER = os.environ.get("ADMIN_USER", "admin").strip()
ADMIN_PASS = os.environ.get("ADMIN_PASS", "admin123").strip()
GUEST_USER = os.environ.get("GUEST_USER", "guest").strip()
GUEST_PASS = os.environ.get("GUEST_PASS", "demo123").strip()


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
    cursor.execute("SELECT COUNT(*) FROM configuracion WHERE id IN (1, 2);")
    count = cursor.fetchone()[0]
    if count == 0:
      cursor.execute(
          "INSERT INTO configuracion (id, activos, cartera) VALUES (1, %s,"
          " %s);",
          (json.dumps(["QQQ", "SPY", "NVDA", "AAPL"]), json.dumps([])),
      )
      cursor.execute(
          "INSERT INTO configuracion (id, activos, cartera) VALUES (2, %s,"
          " %s);",
          (json.dumps(["SPY", "AAPL"]), json.dumps([])),
      )
    conn.commit()
    conn.close()
  except Exception as e:
    print("Error inicializando DB:", e)


init_db()


def db_get(user_id, campo):
  if not DATABASE_URL:
    return ["QQQ", "SPY", "NVDA", "AAPL"] if campo == "activos" else []
  try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(f"SELECT {campo} FROM configuracion WHERE id=%s;", (user_id,))
    row = cursor.fetchone()
    conn.close()
    return json.loads(row[0]) if row and row[0] else []
  except Exception as e:
    return ["QQQ", "SPY"] if campo == "activos" else []


def db_set(user_id, campo, valor):
  if not DATABASE_URL or user_id == 2:
    return  # El usuario guest no persiste cambios reales en base de datos para proteger el demo
  try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE configuracion SET {campo} = %s WHERE id=%s;",
        (json.dumps(valor), user_id),
    )
    conn.commit()
    conn.close()
  except Exception as e:
    print(f"Error guardando DB ({campo}):", e)


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
}

TICKERS_ESCANER = list(CATALOGO_TICKERS.keys())

timeframe_actual = "1h"
estado_mercado = {}
historial_alertas = []
recomendaciones_escaner = []
cache_yf = {}
SSE_SUBSCRIBERS = []


class TimeframeModel(BaseModel):
  timeframe: str


class PosicionModel(BaseModel):
  ticker: str
  precio_compra: float
  sl_usuario: float
  tp_usuario: float
  riesgo_usd: float
  timeframe: str


class LoginModel(BaseModel):
  username: str
  password: str


def obtener_info_horario():
  ny_tz = pytz.timezone("America/New_York")
  ny_time = datetime.now(ny_tz)
  if ny_time.weekday() > 4:
    return "🔴 CERRADO", "Fin de semana"
  m_open = ny_time.replace(hour=9, minute=30, second=0, microsecond=0)
  m_close = ny_time.replace(hour=16, minute=0, second=0, microsecond=0)
  if ny_time < m_open or ny_time > m_close:
    return "🔴 CERRADO", "Fuera de horario"
  return "🟢 ABIERTO", "Mercado en curso"


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
        estado_entrada = "🟢 BUENA ENTRADA"
      elif 0 < distancia_resistencia <= 1.2 and tendencia == "ALZA":
        estado_entrada = "⏳ PREPARANDO"
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


def analizar_mercado():
  global estado_mercado, timeframe_actual
  while True:
    # Monitoreamos union de activos de ambos perfiles para simplificar
    activos_admin = db_get(1, "activos")
    activos_guest = db_get(2, "activos")
    lista_actual = list(set(activos_admin + activos_guest))

    with ThreadPoolExecutor(max_workers=5) as executor:
      resultados = executor.map(
          lambda s: procesar_ticker(s, timeframe_actual), lista_actual
      )

    nuevo_estado = {}
    for r in resultados:
      if r:
        nuevo_estado[r["symbol"]] = r

    estado_mercado = nuevo_estado
    time.sleep(INTERVALO_SEGUNDOS)


threading.Thread(target=analizar_mercado, daemon=True).start()


def obtener_user_id(request: Request):
  token = request.cookies.get("session_token")
  if token == "guest_token":
    return 2
  return 1  # Por defecto admin si no hay cookie o es admin


@app.post("/api/login")
async def login(item: LoginModel, response: Response):
  if item.username == ADMIN_USER and item.password == ADMIN_PASS:
    response.set_cookie(key="session_token", value="admin_token")
    return {"status": "ok", "role": "admin"}
  elif item.username == GUEST_USER and item.password == GUEST_PASS:
    response.set_cookie(key="session_token", value="guest_token")
    return {"status": "ok", "role": "guest"}
  return {"status": "error", "message": "Credenciales incorrectas"}


@app.post("/api/logout")
async def logout(response: Response):
  response.delete_cookie("session_token")
  return {"status": "ok"}


@app.get("/api/data")
def obtener_datos(request: Request):
  user_id = obtener_user_id(request)
  activos_usuario = db_get(user_id, "activos")
  cartera_usuario = db_get(user_id, "cartera")

  mercado_filtrado = {
      k: v for k, v in estado_mercado.items() if k in activos_usuario
  }

  estado, cuenta_reg = obtener_info_horario()
  return {
      "mercado": mercado_filtrado,
      "cartera": cartera_usuario,
      "timeframe": timeframe_actual,
      "horario": estado,
      "es_guest": user_id == 2,
      "catalogo": CATALOGO_TICKERS,
  }


@app.post("/api/add")
async def agregar_activo(request: Request):
  user_id = obtener_user_id(request)
  try:
    data = await request.json()
    symbol = data.get("ticker", "").strip().upper()
  except Exception:
    symbol = ""

  if symbol:
    activos = db_get(user_id, "activos")
    if symbol not in activos:
      activos.append(symbol)
      db_set(user_id, "activos", activos)
      threading.Thread(
          target=lambda: procesar_ticker(symbol, timeframe_actual), daemon=True
      ).start()
  return {"status": "ok"}


@app.post("/api/remove")
async def eliminar_activo(request: Request):
  user_id = obtener_user_id(request)
  try:
    data = await request.json()
    symbol = data.get("ticker", "").strip().upper()
  except Exception:
    symbol = ""

  if symbol:
    activos = db_get(user_id, "activos")
    if symbol in activos:
      activos.remove(symbol)
      db_set(user_id, "activos", activos)
  return {"status": "ok"}


@app.post("/api/cartera/add")
def agregar_cartera(item: PosicionModel, request: Request):
  user_id = obtener_user_id(request)
  cartera = db_get(user_id, "cartera")
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
  })
  db_set(user_id, "cartera", cartera)
  return {"status": "ok"}


@app.post("/api/cartera/remove")
async def eliminar_cartera(request: Request):
  user_id = obtener_user_id(request)
  try:
    data = await request.json()
    ticker = data.get("ticker", "").strip().upper()
  except Exception:
    ticker = ""
  if ticker:
    cartera = db_get(user_id, "cartera")
    cartera = [p for p in cartera if p["ticker"] != ticker]
    db_set(user_id, "cartera", cartera)
  return {"status": "ok"}


@app.post("/api/timeframe")
def cambiar_timeframe(item: TimeframeModel):
  global timeframe_actual, estado_mercado
  if item.timeframe in ["1h", "4h", "1d"]:
    timeframe_actual = item.timeframe
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
            h1 { text-align: center; color: #38bdf8; font-size: 1.5rem; margin: 5px 0; }
            .top-bar { max-width: 1200px; margin: 0 auto 10px auto; display: flex; justify-content: space-between; align-items: center; background: #1c2541; padding: 8px 12px; border-radius: 8px; border: 1px solid #3a506b; font-size: 0.85rem; }
            button { background: #38bdf8; color: #0b132b; border: none; padding: 6px 10px; font-weight: bold; border-radius: 6px; cursor: pointer; font-size: 0.8rem; }
            button:hover { background: #7dd3fc; }
            .control-panel { max-width: 1200px; margin: 0 auto 16px auto; background: #1c2541; padding: 10px; border-radius: 10px; display: flex; gap: 8px; align-items: center; justify-content: center; flex-wrap: wrap; border: 1px solid #3a506b; }
            input, select { background: #0b132b; border: 1px solid #3a506b; color: #fff; padding: 6px; border-radius: 6px; font-size: 0.85rem; }
            input[type="text"] { width: 160px; text-transform: uppercase; }
            .container { max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1fr; gap: 16px; }
            @media (max-width: 850px) { .container { grid-template-columns: 1fr; } }
            .card { background: #1c2541; border-radius: 10px; padding: 12px; border: 1px solid #3a506b; position: relative; margin-bottom: 12px; }
            .grid-activos { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 12px; }
            .badge { padding: 2px 5px; border-radius: 8px; font-size: 0.65rem; font-weight: bold; }
            .bullish { background: rgba(34, 197, 94, 0.2); color: #4ade80; border: 1px solid #22c55e; }
            .bearish { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #ef4444; }
            .modal { position: fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.8); display:flex; justify-content:center; align-items:center; z-index:999; }
            .modal-box { background:#1c2541; padding:20px; border-radius:10px; border:1px solid #3a506b; width:300px; text-align:center; }
            .modal-box input { width:100%; margin-bottom:10px; }
            .links-externos { display: flex; gap: 6px; margin-top: 8px; justify-content: center; font-size: 0.75rem; }
            .links-externos a { background: #0b132b; color: #38bdf8; border: 1px solid #3a506b; padding: 3px 6px; border-radius: 4px; text-decoration: none; font-weight: bold; }
        </style>
    </head>
    <body>
        <div class="top-bar">
            <div id="lbl-status">Cargando...</div>
            <div>
                <button onclick="cambiarIdioma()" id="btn-lang">EN / ES</button>
                <button onclick="mostrarLogin()" id="btn-login-logout" style="background:#f43f5e; color:#fff;">Login</button>
            </div>
        </div>

        <h1>📊 Trading Monitor Pro</h1>
        
        <div class="control-panel">
            <input type="text" id="new-ticker" placeholder="TICKER..." list="datalist-tickers" onkeydown="if(event.key==='Enter') agregarActivo()" autocomplete="off" />
            <datalist id="datalist-tickers"></datalist>
            <button onclick="agregarActivo()" id="btn-add">+ Seguir</button>
            <select id="select-tf" onchange="cambiarTimeframe(this.value)">
                <option value="1h">1H (Intradiario)</option>
                <option value="4h">4H (Swing)</option>
                <option value="1d">1D (Diario)</option>
            </select>
        </div>

        <div class="container">
            <div>
                <div class="card">
                    <div style="font-weight:bold; color:#38bdf8; margin-bottom:8px;" id="lbl-portfolio">💼 Mi Cartera</div>
                    <div style="display:flex; flex-wrap:wrap; gap:6px; margin-bottom:8px;">
                        <input type="text" id="c-ticker" placeholder="Activo" style="width:65px;" />
                        <input type="number" id="c-precio" placeholder="Entrada" style="width:80px;" step="any" />
                        <input type="number" id="c-sl" placeholder="SL" style="width:70px;" step="any" />
                        <input type="number" id="c-tp" placeholder="TP" style="width:70px;" step="any" />
                        <input type="number" id="c-riesgo" placeholder="Riesgo$" style="width:70px;" value="50" step="any" />
                        <button onclick="registrarPosicion()" id="btn-save">Guardar</button>
                    </div>
                    <div id="lista-cartera" style="font-size:0.85rem;">Sin posiciones.</div>
                </div>

                <h3 id="lbl-watched" style="font-size:1rem; color:#38bdf8;">Activos Monitoreados</h3>
                <div class="grid-activos" id="grid-mercado"><p style="color:#94a3b8;">Cargando...</p></div>
            </div>
            
            <div>
                <div class="card">
                    <div style="font-weight:bold; color:#38bdf8; margin-bottom:6px;" id="lbl-guide">📖 Guía Rápida</div>
                    <div style="font-size:0.78rem; color:#cbd5e1; line-height:1.3;" id="lbl-guide-text">
                        <b>1H:</b> Horas/días. Requiere supervisión.<br>
                        <b>4H/1D:</b> Semanas. Menos ruido.<br>
                        Vigila que SMA 9 no cruce bajo SMA 21.
                    </div>
                </div>
            </div>
        </div>

        <div id="login-modal" class="modal" style="display:none;">
            <div class="modal-box">
                <h3 style="color:#38bdf8; margin-top:0;">Acceso</h3>
                <input type="text" id="log-user" placeholder="Usuario" />
                <input type="password" id="log-pass" placeholder="Contraseña" />
                <button onclick="ejecutarLogin()" style="width:100%; margin-bottom:6px;">Entrar</button>
                <button onclick="cerrarLogin()" style="width:100%; background:#3a506b; color:#fff;">Cancelar</button>
            </div>
        </div>

        <script>
            let lang = 'es';
            const dict = {
                es: { status: "Mercado Operativo", portfolio: "💼 Mi Cartera / Posiciones", watched: "Activos Monitoreados", guide: "📖 Guía Rápida", add: "+ Seguir", save: "Guardar", login: "Login", logout: "Salir" },
                en: { status: "Market Live", portfolio: "💼 My Portfolio / Positions", watched: "Monitored Assets", guide: "📖 Quick Guide", add: "+ Track", save: "Save", login: "Login", logout: "Logout" }
            };

            function cambiarIdioma() {
                lang = lang === 'es' ? 'en' : 'es';
                document.getElementById('btn-lang').innerText = lang.toUpperCase();
                document.getElementById('lbl-portfolio').innerText = dict[lang].portfolio;
                document.getElementById('lbl-watched').innerText = dict[lang].watched;
                document.getElementById('lbl-guide').innerText = dict[lang].guide;
                document.getElementById('btn-add').innerText = dict[lang].add;
                document.getElementById('btn-save').innerText = dict[lang].save;
            }

            function mostrarLogin() { document.getElementById('login-modal').style.display = 'flex'; }
            function cerrarLogin() { document.getElementById('login-modal').style.display = 'none'; }

            async function ejecutarLogin() {
                const u = document.getElementById('log-user').value;
                const p = document.getElementById('log-pass').value;
                const res = await fetch('/api/login', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({username: u, password: p})
                });
                const data = await res.json();
                if(data.status === 'ok') { location.reload(); } else { alert('Error de acceso'); }
            }

            async function logout() {
                await fetch('/api/logout', {method: 'POST'});
                location.reload();
            }

            async function agregarActivo(tickerParam = null) {
                const input = document.getElementById('new-ticker');
                const ticker = tickerParam || input.value.trim();
                if (!ticker) return;
                await fetch('/api/add', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker }) });
                if(!tickerParam) input.value = '';
                actualizarApp();
            }

            async function eliminarActivo(ticker) {
                await fetch('/api/remove', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker }) });
                actualizarApp();
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
                    body: JSON.stringify({ ticker, precio_compra: precio, sl_usuario: sl, tp_usuario: tp, riesgo_usd: riesgo, timeframe: tf })
                });
                actualizarApp();
            }

            async function eliminarPosicion(ticker) {
                await fetch('/api/cartera/remove', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ticker }) });
                actualizarApp();
            }

            async function cambiarTimeframe(tf) {
                await fetch('/api/timeframe', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ timeframe: tf }) });
                actualizarApp();
            }

            async function actualizarApp() {
                try {
                    const res = await fetch('/api/data');
                    const { mercado, cartera, timeframe, horario, es_guest, catalogo } = await res.json();
                    
                    document.getElementById('select-tf').value = timeframe;
                    document.getElementById('lbl-status').innerHTML = `${horario} | ${es_guest ? '👁️ Modo Invitado (Demo)' : '🔐 Modo Administrador'}`;
                    
                    const btnLoginLogout = document.getElementById('btn-login-logout');
                    if(es_guest) {
                        btnLoginLogout.innerText = dict[lang].login;
                        btnLoginLogout.onclick = mostrarLogin;
                        btnLoginLogout.style.background = '#38bdf8';
                        btnLoginLogout.style.color = '#0b132b';
                    } else {
                        btnLoginLogout.innerText = dict[lang].logout;
                        btnLoginLogout.onclick = logout;
                        btnLoginLogout.style.background = '#f43f5e';
                        btnLoginLogout.style.color = '#fff';
                    }

                    const datalist = document.getElementById('datalist-tickers');
                    if(datalist.children.length === 0 && catalogo) {
                        for(const [t, desc] of Object.entries(catalogo)) {
                            const opt = document.createElement('option'); opt.value = t; opt.textContent = desc; datalist.appendChild(opt);
                        }
                    }

                    const divCartera = document.getElementById('lista-cartera');
                    if (cartera.length > 0) {
                        divCartera.innerHTML = cartera.map(p => `
                            <div style="background:#0b132b; padding:6px; border-radius:6px; margin-bottom:4px;">
                                <b>${p.ticker}</b> - Comprado: $${p.precio_compra} | Acciones: ${p.acciones} 
                                <button onclick="eliminarPosicion('${p.ticker}')" style="background:none; color:#ef4444; border:none; float:right;">✕</button>
                            </div>
                        `).join('');
                    } else { divCartera.innerHTML = '<span style="color:#94a3b8;">Sin posiciones.</span>'; }

                    const grid = document.getElementById('grid-mercado');
                    if (Object.keys(mercado).length > 0) {
                        grid.innerHTML = '';
                        for (const [ticker, info] of Object.entries(mercado)) {
                            const isBull = info.tendencia === 'ALZA';
                            grid.innerHTML = grid.innerHTML + `
                                <div class="card" style="margin-bottom:0;">
                                    <button onclick="eliminarActivo('${ticker}')" style="position:absolute; top:8px; right:8px; background:none; color:#ef4444; border:none; cursor:pointer;">✕</button>
                                    <div style="font-weight:bold; font-size:1rem;">${ticker} <span class="badge ${isBull ? 'bullish' : 'bearish'}">${info.tendencia}</span></div>
                                    <div style="font-size:1.2rem; font-weight:bold; margin:4px 0;">$${info.precio}</div>
                                    <div style="font-size:0.75rem; color:#cbd5e1;">SL: $${info.soporte_tecnico} | TP: $${info.tp_tecnico}</div>
                                    <div style="background:#0b132b; padding:2px; border-radius:4px; margin-top:6px;">
                                        <svg width="100%" height="30" viewBox="0 0 100 35" preserveAspectRatio="none">
                                            <polyline fill="none" stroke="${info.sparkline_color}" stroke-width="2" points="${info.sparkline}" />
                                        </svg>
                                    </div>
                                    <div class="links-externos">
                                        <a href="https://es.finance.yahoo.com/quote/${ticker}" target="_blank">Yahoo (ES)</a>
                                        <a href="https://finance.yahoo.com/quote/${ticker}" target="_blank">Yahoo (EN)</a>
                                    </div>
                                </div>
                            `;
                        }
                    } else { grid.innerHTML = '<p style="color:#94a3b8;">Sin activos visibles.</p>'; }
                } catch(e) {}
            }
            actualizarApp();
            setInterval(actualizarApp, 15000);
        </script>
    </body>
    </html>
    """


if __name__ == "__main__":
  port = int(os.environ.get("PORT", 8000))
  uvicorn.run(app, host="0.0.0.0", port=port)
