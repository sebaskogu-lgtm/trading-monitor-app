import ssl

def get_db_connection():
    if not DATABASE_URL:
        return None
    try:
        # Forzar SSL requerido por Supabase
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
                "INSERT INTO configuracion (id, activos, cartera) VALUES (1, %s, %s);",
                (json.dumps(["QQQ", "SPY", "NVDA", "AAPL"]), json.dumps([]))
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
        cursor.execute(f"UPDATE configuracion SET {campo} = %s WHERE id=1;", (json.dumps(valor),))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error guardando DB ({campo}):", e)
