from __future__ import annotations
import os, re, threading, time, hashlib, json, logging, contextlib
from collections import deque
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
import paho.mqtt.client as paho_mqtt
import psycopg2
from psycopg2 import pool

log = logging.getLogger('werkzeug')
# Load .env baik dari root directory maupun folder backend/
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
load_dotenv()

_mqtt_live_data = {}
_mqtt_last_seen = {}
_db_last_ping = {}

MAP_METRIC = {
    'Voltage_V': 'Voltage (V)',
    'Current_A': 'Current (A)',
    'Power_W': 'Power (W)',
    'Apparent_Power_kVA': 'Apparent Power (kVA)',
    'Reactive_Power_kVAR': 'Reactive Power (kVAR)',
    'Power_Factor': 'Power Factor',
    'Phase_Angle_deg': 'Sensor Angle (°)',
    'Frequency_Hz': 'Frequency (Hz)',
    'Active_Energy_kWh': 'Active Energy (kWh)',
    'Apparent_Energy_kVAh': 'Apparent Energy (kVAh)',
    'Reactive_Energy_kVARh': 'Reactive Energy (kVARh)',
}

# Mapping kunci JSON singkat dari ESP32 → nama field standar
_ESP32_JSON_MAP = {
    'V': 'Voltage (V)', 'A': 'Current (A)', 'W': 'Power (W)',
    'Hz': 'Frequency (Hz)', 'kWh': 'Active Energy (kWh)', 'pf': 'Power Factor',
}

def _on_message(client, userdata, msg):
    try:
        if msg.retain:
            return
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        parts = topic.split('/')
        if len(parts) >= 2 and parts[0] == 'energymeter':
            device_id = parts[1].strip().rstrip(':').strip()
            _mqtt_last_seen[device_id] = time.time()
            if device_id not in _mqtt_live_data:
                _mqtt_live_data[device_id] = {}

            if len(parts) == 3 and parts[2] == 'Timestamp':
                # energymeter/alat1/Timestamp
                _mqtt_live_data[device_id]['Timestamp'] = payload

            elif len(parts) == 3 and re.match(r'^L\d+$', parts[2]):
                # [FORMAT BARU] energymeter/alat1/L1
                # ESP32 kirim 1 JSON per channel:
                # {"V":220.1,"A":1.234,"W":270.1,"Hz":50.01,"kWh":0.123,"pf":0.987}
                phase = parts[2]
                try:
                    data = json.loads(payload)
                    if phase not in _mqtt_live_data[device_id]:
                        _mqtt_live_data[device_id][phase] = {}
                    for k, v in data.items():
                        mapped = _ESP32_JSON_MAP.get(k, k)
                        try:
                            _mqtt_live_data[device_id][phase][mapped] = float(v)
                        except (TypeError, ValueError):
                            pass
                except (json.JSONDecodeError, Exception):
                    pass

            elif len(parts) == 4:
                # [FORMAT LAMA] energymeter/alat1/L1/Voltage_V → nilai float tunggal
                phase, metric = parts[2], parts[3]
                if phase not in _mqtt_live_data[device_id]:
                    _mqtt_live_data[device_id][phase] = {}
                try:
                    val = float(payload)
                except ValueError:
                    val = 0.0
                mapped = MAP_METRIC.get(metric, metric)
                _mqtt_live_data[device_id][phase][mapped] = val
    except Exception:
        pass

def _start_mqtt():
    broker = os.environ.get("MQTT_BROKER")
    if not broker:
        print("WARNING: MQTT_BROKER is not set. MQTT client will not be started.")
        return None
    try:
        try:
            client = paho_mqtt.Client(callback_api_version=paho_mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            client = paho_mqtt.Client()
        
        try:
            port = int(os.environ.get("MQTT_PORT", "1883"))
        except ValueError:
            port = 1883
            
        username = os.environ.get("MQTT_USERNAME")
        password = os.environ.get("MQTT_PASSWORD")

        if username:
            client.username_pw_set(username, password)
            
        # Gunakan TLS jika diaktifkan secara eksplisit atau jika port adalah 8883
        use_tls_env = os.environ.get("MQTT_USE_TLS")
        if use_tls_env is not None:
            use_tls = use_tls_env.lower() == "true"
        else:
            use_tls = (port == 8883)

        if use_tls:
            client.tls_set()

        def on_connect(c, userdata, flags, rc, properties=None):
            c.subscribe("energymeter/#")
        client.on_connect = on_connect
        client.on_message = _on_message
        client.connect(broker, port, keepalive=60)
        client.loop_start()
        return client
    except Exception as e:
        print(f"MQTT init failed: {e}")
        return None

app = Flask(__name__, template_folder='../frontend', static_folder='../frontend', static_url_path='')
_mqtt_client = _start_mqtt() if (not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true") else None
_PHASE_RE = re.compile(r'^L\d+$')
def _detect_phases(device_data: dict) -> list[str]:
    keys: set[str] = set()
    for src in [device_data.get('RealTime') or {}, (device_data.get('meta') or {}).get('sensors') or {}]:
        if isinstance(src, dict): keys.update(k for k in src if _PHASE_RE.match(k))
    return sorted(keys, key=lambda x: int(x[1:]))

db_pool = None
sparta_db_pool = None
_db_init_lock = threading.Lock()
_sparta_db_init_lock = threading.Lock()

def init_sparta_db():
    global sparta_db_pool
    with _sparta_db_init_lock:
        if sparta_db_pool:
            return
        sparta_url = os.environ.get('SPARTA_DATABASE_URL')
        if not sparta_url or not (sparta_url.startswith("postgres://") or sparta_url.startswith("postgresql://")):
            return
        try:
            sparta_db_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, sparta_url)
            print("Sparta PostgreSQL connection pool initialized successfully.")
        except Exception as e:
            print(f"Failed to initialize Sparta PostgreSQL pool: {e}")

def init_db():
    global db_pool
    with _db_init_lock:
        if db_pool:  # Sudah diinisialisasi oleh thread lain
            return
        db_url = os.environ.get('DATABASE_URL')
        if not db_url or not (db_url.startswith("postgres://") or db_url.startswith("postgresql://")):
            print("WARNING: DATABASE_URL not set or invalid. Database operations will fail.")
            return
        try:
            pool = psycopg2.pool.ThreadedConnectionPool(1, 20, db_url)
            print("PostgreSQL connection pool initialized successfully.")

            # Inisialisasi DB Sparta juga jika tersedia
            init_sparta_db()

            # Create tables if not exist
            conn = pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS devices (
                            id VARCHAR(50) PRIMARY KEY,
                            name VARCHAR(100) NOT NULL,
                            online BOOLEAN DEFAULT FALSE,
                            last_seen VARCHAR(50),
                            sensors JSONB DEFAULT '[]'::jsonb
                        );
                    """)
                    # Tabel telemetry: snapshot otomatis tiap 15 menit (untuk grafik Day/Week)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS telemetry (
                            id SERIAL PRIMARY KEY,
                            device_id VARCHAR(50) NOT NULL,
                            phase VARCHAR(10) NOT NULL,
                            timestamp VARCHAR(50) NOT NULL,
                            epoch BIGINT NOT NULL,
                            voltage FLOAT DEFAULT 0.0,
                            current FLOAT DEFAULT 0.0,
                            power FLOAT DEFAULT 0.0,
                            frequency FLOAT DEFAULT 0.0,
                            energy FLOAT DEFAULT 0.0,
                            power_factor FLOAT DEFAULT 0.0,
                            offline BOOLEAN DEFAULT FALSE
                        );
                    """)
                    # Tabel history: rekaman sesi Start Capture (untuk tab Database)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS history (
                            id SERIAL PRIMARY KEY,
                            device_id VARCHAR(50) NOT NULL,
                            phase VARCHAR(10) NOT NULL,
                            timestamp VARCHAR(50) NOT NULL,
                            epoch BIGINT NOT NULL,
                            voltage FLOAT DEFAULT 0.0,
                            current FLOAT DEFAULT 0.0,
                            power FLOAT DEFAULT 0.0,
                            frequency FLOAT DEFAULT 0.0,
                            energy FLOAT DEFAULT 0.0,
                            power_factor FLOAT DEFAULT 0.0,
                            offline BOOLEAN DEFAULT FALSE,
                            session_id VARCHAR(50) NOT NULL,
                            session_name VARCHAR(100) DEFAULT NULL,
                            phase_name VARCHAR(100) DEFAULT NULL
                        );
                    """)
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='history' AND column_name='phase_name';")
                    if not cur.fetchone():
                        cur.execute("ALTER TABLE history ADD COLUMN phase_name VARCHAR(100) DEFAULT NULL;")
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='history' AND column_name='device_name';")
                    if not cur.fetchone():
                        cur.execute("ALTER TABLE history ADD COLUMN device_name VARCHAR(100) DEFAULT NULL;")
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='devices' AND column_name='store_id';")
                    if not cur.fetchone():
                        cur.execute("ALTER TABLE devices ADD COLUMN store_id VARCHAR(50) DEFAULT NULL;")
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='history' AND column_name='store_id';")
                    if not cur.fetchone():
                        cur.execute("ALTER TABLE history ADD COLUMN store_id VARCHAR(50) DEFAULT NULL;")
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='devices' AND column_name='latitude';")
                    if not cur.fetchone():
                        cur.execute("ALTER TABLE devices ADD COLUMN latitude FLOAT DEFAULT NULL;")
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='devices' AND column_name='longitude';")
                    if not cur.fetchone():
                        cur.execute("ALTER TABLE devices ADD COLUMN longitude FLOAT DEFAULT NULL;")
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='history' AND column_name='latitude';")
                    if not cur.fetchone():
                        cur.execute("ALTER TABLE history ADD COLUMN latitude FLOAT DEFAULT NULL;")
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='history' AND column_name='longitude';")
                    if not cur.fetchone():
                        cur.execute("ALTER TABLE history ADD COLUMN longitude FLOAT DEFAULT NULL;")
                    # Pastikan kolom name & device_name bertipe TEXT tanpa batasan panjang karakter
                    cur.execute("ALTER TABLE devices ALTER COLUMN name TYPE TEXT;")
                    cur.execute("ALTER TABLE history ALTER COLUMN device_name TYPE TEXT;")
                    cur.execute("ALTER TABLE history ALTER COLUMN session_name TYPE TEXT;")
                    def create_index_safe(idx_name, create_sql):
                        cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = %s", (idx_name,))
                        if not cur.fetchone():
                            cur.execute(create_sql)

                    create_index_safe('idx_telemetry_device_epoch', "CREATE INDEX idx_telemetry_device_epoch ON telemetry(device_id, epoch);")
                    create_index_safe('idx_telemetry_device_phase', "CREATE INDEX idx_telemetry_device_phase ON telemetry(device_id, phase);")
                    create_index_safe('idx_history_session', "CREATE INDEX idx_history_session ON history(session_id);")
                    create_index_safe('idx_history_device_session', "CREATE INDEX idx_history_device_session ON history(device_id, session_id);")

                    # Fix broken sequences from database migration
                    for table_name in ['telemetry', 'history']:
                        cur.execute(f"SELECT column_default FROM information_schema.columns WHERE table_name='{table_name}' AND column_name='id';")
                        res = cur.fetchone()
                        if res and res[0] is None:
                            seq_name = f"{table_name}_id_seq_fallback"
                            cur.execute("SELECT 1 FROM pg_class WHERE relname=%s", (seq_name,))
                            if not cur.fetchone():
                                cur.execute(f"CREATE SEQUENCE {seq_name};")
                                cur.execute(f"SELECT MAX(id) FROM {table_name};")
                                max_id = cur.fetchone()[0]
                                if max_id:
                                    cur.execute(f"SELECT setval('{seq_name}', {max_id});")
                            cur.execute(f"ALTER TABLE {table_name} ALTER COLUMN id SET DEFAULT nextval('{seq_name}');")

                    # Unique constraint untuk cegah duplikat snapshot telemetry
                    # (terjadi saat Flask debug mode menjalankan 2 proses sekaligus)
                    cur.execute("""
                        DELETE FROM telemetry a
                        USING telemetry b
                        WHERE a.id > b.id
                          AND a.device_id = b.device_id
                          AND a.phase = b.phase
                          AND a.timestamp = b.timestamp
                    """)
                    create_index_safe('idx_telemetry_unique_slot', "CREATE UNIQUE INDEX idx_telemetry_unique_slot ON telemetry(device_id, phase, timestamp);")
                    conn.commit()
                    print("Database tables initialized successfully.")
            except Exception as e:
                conn.rollback()
                print(f"Error initializing database tables: {e}")
            finally:
                pool.putconn(conn)

            db_pool = pool  # Simpan ke global hanya setelah semuanya sukses
        except Exception as e:
            print(f"Failed to initialize PostgreSQL pool: {e}")

@contextlib.contextmanager
def get_sparta_db_cursor():
    global sparta_db_pool
    if not sparta_db_pool:
        init_sparta_db()
        if not sparta_db_pool:
            raise Exception("Sparta database connection pool not available.")
    conn = None
    for _ in range(3):
        try:
            conn = sparta_db_pool.getconn()
            if conn.closed:
                raise psycopg2.InterfaceError("connection closed")
            with conn.cursor() as ping_cur:
                ping_cur.execute("SELECT 1")
            conn.rollback()
            break
        except Exception:
            if conn:
                try: sparta_db_pool.putconn(conn, close=True)
                except Exception: pass
            conn = None
    if not conn:
        raise Exception("Gagal mendapatkan koneksi aktif ke database Sparta")
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        raise e
    finally:
        try: sparta_db_pool.putconn(conn)
        except Exception: pass

def _get_healthy_conn():
    """Ambil koneksi dari pool yang masih aktif. Buang dan coba lagi jika stale."""
    global db_pool
    if not db_pool:
        init_db()
        if not db_pool:
            raise Exception("Database connection pool not available.")

    last_exc = None
    for _ in range(3):
        conn = db_pool.getconn()
        try:
            if conn.closed:
                raise psycopg2.InterfaceError("connection already closed")
            # Ping nyata ke server — satu-satunya cara andal deteksi koneksi mati
            with conn.cursor() as ping_cur:
                ping_cur.execute("SELECT 1")
            conn.rollback()  # Jangan tinggalkan transaksi terbuka
            return conn
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            last_exc = e
            try:
                db_pool.putconn(conn, close=True)  # Buang koneksi mati dari pool
            except Exception:
                pass

    raise last_exc or Exception("Gagal mendapatkan koneksi aktif setelah 3 percobaan")

@contextlib.contextmanager
def get_db_cursor():
    conn = _get_healthy_conn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise e
    finally:
        try:
            db_pool.putconn(conn)
        except Exception:
            pass
def normalize(raw: dict | None) -> dict | None:
    if not raw: return None
    try:
        phases = sorted([k for k in raw if _PHASE_RE.match(k)], key=lambda x: int(x[1:]))
        if not phases: return None
        def g(p, k):
            try: return float((raw.get(p) or {}).get(k, 0))
            except: return 0.0
        V  = [g(p, 'Voltage (V)')          for p in phases]
        I  = [g(p, 'Current (A)')            for p in phases]
        P  = [g(p, 'Power (W)')              for p in phases]
        F  = [g(p, 'Frequency (Hz)')         for p in phases]
        S  = [g(p, 'Apparent Power (kVA)')   for p in phases]
        Q  = [g(p, 'Reactive Power (kVAR)')  for p in phases]
        E  = [g(p, 'Active Energy (kWh)')    for p in phases]
        PF = [g(p, 'Power Factor')           for p in phases]
        d  = max(sum(1 for v in V if v > 0), 1)
        return {
            'Voltage': sum(V)/d, 'Current': sum(I), 'Power': sum(P), 'Frequency': sum(F)/d,
            'Apparent': sum(S), 'Reactive': sum(Q), 'Energy': sum(E), 'PowerFactor': sum(PF)/d,
            'Phase1': g(phases[0], 'Sensor Angle (°)'),
            'EnergyApparent': sum(g(p, 'Apparent Energy (kVAh)')  for p in phases),
            'EnergyReactive': sum(g(p, 'Reactive Energy (kVARh)') for p in phases),
        }
    except: return None
_WIB = timezone(timedelta(hours=7))
def _ts_now() -> str: return datetime.now(_WIB).strftime('%H:%M:%S %d/%m/%Y')
def validate_device_name(name: str) -> tuple[bool, str]:
    if not name or not isinstance(name, str): return False, 'Nama harus berupa text'
    name = name.strip()
    if not name: return False, 'Nama toko tidak boleh kosong'
    if '\x00' in name: return False, 'Karakter tidak diizinkan'
    return True, ''
def validate_phase_key(phase: str) -> bool: return bool(_PHASE_RE.match(phase))
_capture_lock = threading.Lock()
_capture_states: dict[str, dict] = {}  # { device_id: capture_state_dict }

def _get_device_capture_state(device_id: str) -> dict:
    if not device_id:
        device_id = 'default'
    if device_id not in _capture_states:
        _capture_states[device_id] = {
            'active': False, 'device_id': device_id, 'device_name': device_id,
            'session_id': None, 'session_name': None, 'interval': 15,
            'count': 0, 'started_at': None, 'enabled_phases': None,
            '_thread': None, '_stop_event': None, '_wake_event': None, '_finalizing': False, 'time_offset_ms': 0,
            'sensor_names': {},
        }
    return _capture_states[device_id]

STATE_FILE = 'active_captures.json'

def _save_capture_states():
    try:
        with _capture_lock:
            safe_states = {}
            for did, state in _capture_states.items():
                if state.get('active'):
                    safe_states[did] = {
                        'active': True,
                        'session_id': state.get('session_id'),
                        'session_name': state.get('session_name'),
                        'device_id': state.get('device_id'),
                        'device_name': state.get('device_name'),
                        'interval': state.get('interval'),
                        'started_at': state.get('started_at'),
                        'count': state.get('count', 0),
                        'enabled_phases': state.get('enabled_phases'),
                        'sensor_names': state.get('sensor_names', {}),
                        'time_offset_ms': state.get('time_offset_ms', 0)
                    }
        with open(STATE_FILE, 'w') as f:
            json.dump(safe_states, f)
    except Exception as e:
        print(f"Error saving capture states: {e}")

def _load_capture_states():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                saved = json.load(f)
            to_start = []
            with _capture_lock:
                for did, state in saved.items():
                    if state.get('active'):
                        cstate = _get_device_capture_state(did)
                        cstate.update(state)
                        to_start.append(did)
            for did in to_start:
                _start_thread(did)
    except Exception as e:
        print(f"Error loading capture states: {e}")

def _data_hash(raw): return hashlib.md5(json.dumps(raw, sort_keys=True).encode()).hexdigest() if raw else None
_hourly_stop = threading.Event()
_last_write_per_device: dict[str, str] = {}  # guard duplikat per device
_live_buffer_last_push: dict[str, float] = {}  # waktu terakhir data dipush ke live buffer per device
HEARTBEAT_INTERVAL = 30  # detik; push heartbeat ke live-buffer agar frontend tidak stale

def _get_telemetry_phases(device_id: str, raw: dict) -> list[str]:
    """Dapatkan daftar phase yang harus di-snapshot untuk device ini.
    Prioritas: sensor terdaftar di DB → MQTT live data → fallback L1–L5.
    Selalu mencakup semua phase yang ada di MQTT agar L6–L10 tersimpan."""
    phases: set[str] = set()

    # 1. Phase dari MQTT live data (paling aktual)
    for k in raw:
        if _PHASE_RE.match(k):
            phases.add(k)

    # 2. Phase dari sensor yang terdaftar di DB (termasuk yang mungkin sedang offline)
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s", (device_id,))
            row = cur.fetchone()
        if row and row[0]:
            sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
            for s in sensors_list:
                ph = s.get('phase', '')
                if ph and _PHASE_RE.match(ph):
                    phases.add(ph)
    except Exception:
        pass

    # 3. Fallback minimal jika tidak ada data sama sekali
    if not phases:
        phases = {'L1', 'L2', 'L3', 'L4', 'L5'}

    return sorted(phases, key=lambda x: int(x[1:]))

def _do_hourly_capture_device(device_id: str) -> None:
    try:
        now = datetime.now(_WIB)
        m   = (now.minute // 15) * 15  # Interval 15 menit
        key = f'{now.strftime("%H")}{str(m).zfill(2)}'

        # Cegah duplikat dalam slot waktu yang sama (per device)
        state_key = f"{device_id}_{key}_{now.strftime('%Y-%m-%d')}"
        if _last_write_per_device.get(device_id) == state_key:
            return
        _last_write_per_device[device_id] = state_key

        ts        = f'{now.strftime("%H")}:{str(m).zfill(2)} {now.strftime("%d/%m/%Y")}'
        epoch_val = int(now.timestamp() * 1000)

        # Data MQTT terakhir dari cache (bisa None jika device belum pernah kirim data)
        raw            = _mqtt_live_data.get(device_id) or {}
        device_offline = time.time() - _mqtt_last_seen.get(device_id, 0) > 300

        # Deteksi phase secara dinamis — mendukung L6–L10 dan seterusnya
        telemetry_phases = _get_telemetry_phases(device_id, raw)
        print(f"[Telemetry] Snapshot {device_id} slot={ts} offline={device_offline} phases={telemetry_phases}")

        with get_db_cursor() as cur:
            for ph in telemetry_phases:
                pd = raw.get(ph) or {}  # {} jika phase belum ada data → semua nilai 0
                phase_offline = device_offline or not bool(pd)

                def f(k, _pd=pd):
                    try: return float(_pd.get(k) or 0)
                    except: return 0.0

                cur.execute("""
                    INSERT INTO telemetry (
                        device_id, phase, timestamp, epoch,
                        voltage, current, power, frequency, energy, power_factor, offline
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (device_id, phase, timestamp) DO NOTHING
                """, (
                    device_id, ph, ts, epoch_val,
                    f('Voltage (V)'), f('Current (A)'), f('Power (W)'),
                    f('Frequency (Hz)'), f('Active Energy (kWh)'), f('Power Factor'),
                    phase_offline
                ))

            # Hapus data lama (>30 hari)
            thirty_days_ago = int((now - timedelta(days=30)).timestamp() * 1000)
            cur.execute("DELETE FROM telemetry WHERE epoch < %s", (thirty_days_ago,))
    except Exception as e:
        print(f"Error in _do_hourly_capture_device: {e}")

def _do_hourly_capture_all() -> None:
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT id FROM devices")
            device_ids = [row[0] for row in cur.fetchall()]
        for did in device_ids:
            threading.Thread(target=_do_hourly_capture_device, args=(did,), daemon=True).start()
    except Exception as e:
        print(f"Error in _do_hourly_capture_all: {e}")

def _hourly_worker() -> None:
    # Tunggu 30s setelah start: memberi waktu ESP32 reconnect & kirim semua phase
    # ESP32 mengirim semua phase sekaligus saat pertama connect (sebelum delta mode)
    time.sleep(30)

    while not _hourly_stop.is_set():
        now = datetime.now(_WIB)
        ns  = now.replace(minute=((now.minute // 15) + 1) * 15 % 60, second=0, microsecond=0)
        if ns <= now: ns += timedelta(hours=1)
        wait_seconds = (ns - now).total_seconds()
        if _hourly_stop.wait(timeout=wait_seconds):
            break
        _do_hourly_capture_all()

if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    threading.Thread(target=_hourly_worker, daemon=True).start()

_device_live_hash = {}
_device_last_change_ms = {}
_device_is_offline = {}
_device_live_buffer = {}

def _live_buffer_worker() -> None:
    time.sleep(2)
    while True:
        try:
            if not db_pool:
                time.sleep(10)
                continue
            now_ms = int(time.time() * 1000)
            now = time.time()

            # --- Langkah 1: Baca state semua device (1 koneksi) ---
            with get_db_cursor() as cur:
                cur.execute("SELECT id, online, last_seen FROM devices")
                db_devices = cur.fetchall()

            devices_meta = {row[0]: {'online': row[1], 'last_seen': row[2]} for row in db_devices}
            dids = set(devices_meta.keys()) | set(_mqtt_live_data.keys())

            # --- Langkah 2: Hitung perubahan di memori (tanpa DB) ---
            offline_updates = []    # device_id yang perlu di-set offline
            online_updates  = []    # (device_id, ts_str) yang perlu di-set online

            for did in dids:
                raw = _mqtt_live_data.get(did)
                last_seen = _mqtt_last_seen.get(did, 0)
                is_offline = (now - last_seen > 300) or not raw

                if did not in _device_live_buffer:
                    _device_live_buffer[did] = deque(maxlen=600)

                if is_offline:
                    if not _device_is_offline.get(did, False):
                        _device_is_offline[did] = True
                        _device_live_buffer[did].append({'timestamp': now_ms, 'data': {"offline": True}})

                    meta = devices_meta.get(did)
                    if meta and meta['online'] != False:
                        offline_updates.append(did)
                else:
                    h = _data_hash(raw)
                    data_changed = _device_live_hash.get(did) != h
                    time_since_push = now - _live_buffer_last_push.get(did, 0)
                    should_heartbeat = time_since_push >= HEARTBEAT_INTERVAL

                    if data_changed:
                        _device_live_hash[did] = h
                        _device_last_change_ms[did] = now_ms
                        _device_is_offline[did] = False
                        _device_live_buffer[did].append({'timestamp': now_ms, 'data': raw})
                        _live_buffer_last_push[did] = now
                    elif should_heartbeat:
                        # Heartbeat: push data terakhir lagi walau tidak berubah
                        # Tujuannya agar frontend tahu device masih online dan rawRealtimeData tidak stale
                        _device_is_offline[did] = False
                        _device_live_buffer[did].append({'timestamp': now_ms, 'data': raw})
                        _live_buffer_last_push[did] = now

                    meta = devices_meta.get(did)
                    status_changed = not meta or meta['online'] != True
                    time_for_ping = (now - _db_last_ping.get(did, 0) > 30)
                    if status_changed or time_for_ping:
                        _db_last_ping[did] = now
                        ts_str = (raw or {}).get('Timestamp') or _ts_now()
                        online_updates.append((did, ts_str))

            # --- Langkah 3: Tulis semua perubahan dalam 1 koneksi ---
            if offline_updates or online_updates:
                with get_db_cursor() as cur:
                    for did in offline_updates:
                        cur.execute("""
                            INSERT INTO devices (id, name, online, last_seen)
                            VALUES (%s, %s, FALSE, '---')
                            ON CONFLICT (id) DO UPDATE SET online = FALSE;
                        """, (did, did))
                    for did, ts_str in online_updates:
                        cur.execute("""
                            INSERT INTO devices (id, name, online, last_seen)
                            VALUES (%s, %s, TRUE, %s)
                            ON CONFLICT (id) DO UPDATE SET online = TRUE, last_seen = %s;
                        """, (did, did, ts_str, ts_str))

        except Exception as e:
            print(f"Error in _live_buffer_worker: {e}")
        time.sleep(3)

if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    init_db()
    threading.Thread(target=_live_buffer_worker, daemon=True).start()

@app.route('/api/live-buffer/<device_id>')
def get_live_buffer(device_id: str):
    buf = _device_live_buffer.get(device_id)
    if not buf: return jsonify([])
    return jsonify(list(buf))

@app.route('/api/history-buffer/<device_id>')
def get_history_buffer(device_id: str):
    """Return data snapshot 15-menit dari tabel telemetry untuk pre-fill chart Day."""
    try:
        hours = min(max(1, int(request.args.get('hours', 6))), 24)
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - (hours * 3600 * 1000)

        buckets = {}
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT phase, epoch, voltage, current, power, frequency, energy, power_factor
                FROM telemetry
                WHERE device_id = %s AND epoch >= %s
                ORDER BY epoch ASC
            """, (device_id, cutoff_ms))
            rows = cur.fetchall()
            
        import math
        for row in rows:
            phase, epoch_val, v, c, w, hz, kwh, pf = row
            ts_ms = int(epoch_val)
            if ts_ms not in buckets:
                buckets[ts_ms] = {}
            
            # Compute compatibility fields on the fly
            apparent = (v * c) / 1000.0
            power_kw = w / 1000.0
            reactive = math.sqrt(max(0.0, (apparent ** 2) - (power_kw ** 2)))
            try:
                phase_angle = math.acos(max(-1.0, min(1.0, pf))) * 180.0 / math.pi
            except:
                phase_angle = 0.0
                
            buckets[ts_ms][phase] = {
                'Voltage (V)':          v,
                'Current (A)':          c,
                'Power (W)':            w,
                'Frequency (Hz)':       hz,
                'Active Energy (kWh)':  kwh,
                'Power Factor':         pf,
                'Apparent Power (kVA)': round(apparent, 4),
                'Reactive Power (kVAR)':round(reactive, 4),
                'Sensor Angle (°)':      round(phase_angle, 2),
            }
            
        result = [{'timestamp': ts, 'data': d} for ts, d in sorted(buckets.items())]
        return jsonify(result)
    except Exception as e:
        print(f"Error in get_history_buffer: {e}")
        return jsonify([]), 500

def _do_capture_io(device_id, session_id, sched_ts, interval, last_hash, last_change, enabled_phases, time_offset_ms):
    try:
        raw = _mqtt_live_data.get(device_id)
        h   = _data_hash(raw); now = time.time()
        if h != last_hash[0]: last_hash[0] = h; last_change[0] = now
        stale   = (now - _mqtt_last_seen.get(device_id, 0))
        offline = raw is None or stale > 300
        sched_shifted = sched_ts + (time_offset_ms / 1000.0)
        ts  = datetime.fromtimestamp(sched_shifted, tz=_WIB).strftime('%H:%M:%S %d/%m/%Y')
        epoch_val = int(sched_shifted * 1000)
        
        with _capture_lock:
            cstate = _get_device_capture_state(device_id)
            sname = cstate.get('session_name') or 'Rekaman'
            sensor_names = cstate.get('sensor_names') or {}
            dev_name = cstate.get('device_name') or device_id
            store_id = cstate.get('store_id')
            
        phases = enabled_phases if enabled_phases else sorted([k for k in (raw or {}) if _PHASE_RE.match(k)], key=lambda x: int(x[1:]))
        if not phases: return
        
        with get_db_cursor() as cur:
            for ph in phases:
                pd = {} if offline else ((raw or {}).get(ph) if isinstance((raw or {}).get(ph), dict) else {})
                phase_offline = offline or not bool(pd)
                def f(k):
                    try: return float((pd or {}).get(k) or 0)
                    except: return 0.0
                cur.execute("""
                    INSERT INTO history (
                        device_id, phase, timestamp, epoch,
                        voltage, current, power, frequency, energy, power_factor,
                        offline, session_id, session_name, phase_name, device_name, store_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    device_id, ph, ts, epoch_val,
                    f('Voltage (V)'), f('Current (A)'), f('Power (W)'),
                    f('Frequency (Hz)'), f('Active Energy (kWh)'), f('Power Factor'),
                    phase_offline, session_id, sname, sensor_names.get(ph, ph), dev_name, store_id
                ))
                
        with _capture_lock:
            cstate = _get_device_capture_state(device_id)
            cstate['count'] += len(phases)
    except Exception as e:
        print(f"Error in _do_capture_io ({device_id}): {e}")
        try:
            with open("capture_error.log", "a") as f:
                f.write(f"Error in _do_capture_io ({device_id}): {e}\n")
        except:
            pass

def _capture_worker(device_id: str, stop: threading.Event, wake: threading.Event) -> None:
    last_hash: list = [None]; last_change: list = [None]
    nxt = time.time() + 3
    while not stop.is_set():
        if stop.wait(timeout=min(max(0., nxt - time.time()), 0.2)): break
        if wake.is_set():
            wake.clear()
            with _capture_lock: nxt = time.time()
            continue
        if time.time() < nxt: continue
        with _capture_lock:
            cstate = _get_device_capture_state(device_id)
            if not cstate.get('active'): break
            sid = cstate['session_id']; did = cstate['device_id']
            iv  = float(cstate['interval']); ep = cstate.get('enabled_phases')
            to_ms = cstate.get('time_offset_ms', 0)
        sched = nxt; nxt += iv
        threading.Thread(target=_do_capture_io, args=(did, sid, sched, iv, last_hash, last_change, ep, to_ms), daemon=True).start()

def _start_thread(device_id: str) -> None:
    with _capture_lock:
        cstate = _get_device_capture_state(device_id)
        if cstate.get('_thread') and cstate['_thread'].is_alive(): return
        stop = threading.Event(); wake = threading.Event()
        t = threading.Thread(target=_capture_worker, args=(device_id, stop, wake), daemon=True, name=f'Capture-{device_id}')
        cstate.update({'_thread': t, '_stop_event': stop, '_wake_event': wake, '_finalizing': False})
        t.start()

def _finalize_bg(sid, did, count, se, th, enabled_phases, time_offset_ms) -> None:
    try:
        if se: se.set()
        if th and th.is_alive(): th.join(timeout=5)
    except Exception as e:
        print(f"Error in _finalize_bg ({did}): {e}")
    finally:
        with _capture_lock:
            if did in _capture_states:
                _capture_states[did]['_finalizing'] = False

def _stop_device_capture(device_id: str) -> bool:
    with _capture_lock:
        cstate = _capture_states.get(device_id)
        if not cstate or not cstate.get('active'):
            return False
        sid = cstate.get('session_id')
        cnt = cstate.get('count', 0)
        did = cstate.get('device_id', device_id)
        se  = cstate.get('_stop_event')
        th  = cstate.get('_thread')
        ep  = cstate.get('enabled_phases')
        to_ms = cstate.get('time_offset_ms', 0)
        cstate.update({
            'active': False, '_finalizing': True,
            'session_id': None, 'session_name': None,
            'count': 0, 'started_at': None, 'enabled_phases': None,
            '_thread': None, '_stop_event': None, '_wake_event': None,
        })
    _save_capture_states()
    threading.Thread(target=_finalize_bg, args=(sid, did, cnt, se, th, ep, to_ms), daemon=True).start()
    return True

_APP_VERSION = int(time.time())

DEMO_TESTING_STORE = {
    'id': 'demo-head-office-001',
    'code': 'DEMO-HO',
    'name': 'Alfamart Head Office (Testing IoT)',
    'branch': 'HEAD OFFICE',
    'latitude': -6.2238,
    'longitude': 106.6508,
    'is_demo': True
}

@app.route('/')
def index():
    return render_template('index.html', v=_APP_VERSION)

@app.route('/health', methods=['GET'])
def health(): return jsonify({"status": "ok", "message": "Service is alive"}), 200

@app.route('/api/config')
def get_config(): return jsonify({})

@app.route('/api/stores', methods=['GET'])
def get_stores():
    q = (request.args.get('q') or '').strip()
    stores = []
    
    # Toko demo testing disisipkan jika query kosong atau mengandung kata kunci demo/head office/test
    include_demo = not q or any(kw in q.lower() for kw in ['demo', 'head', 'office', 'test', 'ho', 'alfa'])
    if include_demo:
        stores.append(DEMO_TESTING_STORE)
        
    try:
        with get_sparta_db_cursor() as cur:
            if q:
                cur.execute("""
                    SELECT id, code, name, branch, latitude, longitude
                    FROM stores
                    WHERE (code ILIKE %s OR name ILIKE %s OR branch ILIKE %s)
                    ORDER BY code ASC
                    LIMIT 25;
                """, (f"%{q}%", f"%{q}%", f"%{q}%"))
            else:
                cur.execute("""
                    SELECT id, code, name, branch, latitude, longitude
                    FROM stores
                    ORDER BY code ASC
                    LIMIT 25;
                """)
            rows = cur.fetchall()
            for r in rows:
                sid, scode, sname, sbranch, slat, slng = r
                if scode != 'DEMO-HO':
                    stores.append({
                        'id': str(sid),
                        'code': scode or '',
                        'name': sname or '',
                        'branch': sbranch or '',
                        'latitude': float(slat) if slat is not None else None,
                        'longitude': float(slng) if slng is not None else None,
                        'is_demo': False
                    })
    except Exception as e:
        print(f"Error fetching stores from Sparta DB: {e}")
        if not stores:
            stores.append(DEMO_TESTING_STORE)
            
    return jsonify({'ok': True, 'stores': stores, 'count': len(stores)})

@app.route('/api/devices')
def list_devices():
    devices = []
    if not db_pool:
        now = time.time()
        for did in sorted(_mqtt_live_data.keys()):
            raw = _mqtt_live_data.get(did)
            last_seen = _mqtt_last_seen.get(did, 0)
            is_online = (now - last_seen <= 300) and bool(raw)
            phases = []
            if isinstance(raw, dict):
                for ph in sorted([k for k in raw if _PHASE_RE.match(k)], key=lambda x: int(x[1:])):
                    phases.append({'phase': ph, 'name': ph, 'enabled': True})
            devices.append({
                'id': did,
                'name': did,
                'online': is_online,
                'lastSeen': (raw or {}).get('Timestamp') or '---',
                'phases': phases,
                'phaseCount': len(phases),
                'storeId': None
            })
        return jsonify(devices)

    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT id, name, online, last_seen, sensors, store_id, latitude, longitude FROM devices")
            rows = cur.fetchall()
        for row in rows:
            did, name, online, last_seen, sensors_json, store_id, lat, lng = row
            phases = []
            sensors_list = []
            if sensors_json:
                sensors_list = sensors_json if isinstance(sensors_json, list) else json.loads(sensors_json)

            # Auto-discover phase baru dari MQTT live data (misal L17, L18, dst. yang baru terkirim)
            raw = _mqtt_live_data.get(did) or {}
            live_detected = [k for k in raw if _PHASE_RE.match(k)]
            existing_phases = {s.get('phase') for s in sensors_list if isinstance(s, dict)}
            added = False
            for ph in live_detected:
                if ph not in existing_phases:
                    sensors_list.append({
                        'phase': ph,
                        'name': f'Sensor {ph[1:]}',
                        'enabled': True,
                        'properties': []
                    })
                    existing_phases.add(ph)
                    added = True

            if added:
                try:
                    with get_db_cursor() as cur2:
                        cur2.execute("UPDATE devices SET sensors = %s WHERE id = %s", (json.dumps(sensors_list), did))
                except Exception as ex:
                    print(f"Error auto-updating sensors for {did}: {ex}")

            for s in sensors_list:
                phases.append({
                    'phase': s.get('phase'),
                    'name': s.get('name', s.get('phase')),
                    'properties': s.get('properties', []),
                    'enabled': s.get('enabled', True),
                    'color': s.get('color', None)
                })
            devices.append({
                'id': did,
                'name': name,
                'online': online,
                'lastSeen': last_seen,
                'phases': phases,
                'phaseCount': len(phases),
                'storeId': store_id,
                'latitude': lat,
                'longitude': lng
            })
    except Exception as e:
        print(f"Error listing devices: {e}")
    return jsonify(sorted(devices, key=lambda d: d['id']))

@app.route('/api/devices/<device_id>/init-sensors', methods=['POST'])
def init_device_sensors(device_id: str):
    try:
        raw = _mqtt_live_data.get(device_id) or {}
        detected = sorted([k for k in raw if _PHASE_RE.match(k)], key=lambda x: int(x[1:]))
        
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s", (device_id,))
            row = cur.fetchone()
            
            current_sensors = []
            if row and row[0]:
                current_sensors = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            existing_phases = {s.get('phase') for s in current_sensors}
            added = 0
            for ph in detected:
                if ph not in existing_phases:
                    current_sensors.append({
                        'phase': ph,
                        'name': f'Sensor {ph[1:]}',
                        'enabled': True,
                        'properties': []
                    })
                    added += 1
                    
            if added > 0:
                cur.execute("""
                    INSERT INTO devices (id, name, online, last_seen, sensors)
                    VALUES (%s, %s, FALSE, '---', %s)
                    ON CONFLICT (id) DO UPDATE SET sensors = %s;
                """, (device_id, device_id, json.dumps(current_sensors), json.dumps(current_sensors)))
                
        return jsonify({'ok': True, 'initialized': added, 'device_id': device_id, 'phases': detected})
    except Exception as e:
        print(f"Error in init_device_sensors: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>/rename', methods=['POST'])
def rename_device(device_id: str):
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    store_id = data.get('store_id')
    lat = data.get('latitude')
    lng = data.get('longitude')
    try:
        lat = float(lat) if lat is not None and str(lat).strip() != '' else None
        lng = float(lng) if lng is not None and str(lng).strip() != '' else None
    except (ValueError, TypeError):
        lat, lng = None, None

    if store_id is not None:
        store_id = str(store_id).strip() or None

    if not store_id:
        return jsonify({'ok': False, 'error': 'Silakan pilih toko atau gunakan lokasi kustom'}), 400

    ok, err = validate_device_name(name)
    if not ok: return jsonify({'ok': False, 'error': err}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                INSERT INTO devices (id, name, online, last_seen, store_id, latitude, longitude)
                VALUES (%s, %s, FALSE, '---', %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET name = %s, store_id = %s, latitude = %s, longitude = %s;
            """, (device_id, name, store_id, lat, lng, name, store_id, lat, lng))
            
            with _capture_lock:
                cstate = _capture_states.get(device_id)
                if cstate and cstate.get('active'):
                    cstate['device_name'] = name
                    cstate['store_id'] = store_id
                    cstate['latitude'] = lat
                    cstate['longitude'] = lng
                    active_sid = cstate.get('session_id')
                    if active_sid:
                        cur.execute("UPDATE history SET device_name = %s, store_id = %s, latitude = %s, longitude = %s WHERE session_id = %s", (name, store_id, lat, lng, active_sid))

        return jsonify({'ok': True, 'name': name, 'store_id': store_id, 'latitude': lat, 'longitude': lng, 'timestamp': int(time.time() * 1000)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>', methods=['DELETE'])
def delete_device_permanently(device_id: str):
    try:
        # Hapus cache memori backend agar worker mendeteksi ulang
        _mqtt_live_data.pop(device_id, None)
        _mqtt_last_seen.pop(device_id, None)
        _db_last_ping.pop(device_id, None)
        _device_live_hash.pop(device_id, None)
        _device_last_change_ms.pop(device_id, None)
        _device_is_offline.pop(device_id, None)
        _device_live_buffer.pop(device_id, None)
        
        with get_db_cursor() as cur:
            # Hapus metadata pendaftaran device saja agar bisa di-auto register ulang bersih
            # Catatan: Data tabel 'history' dan 'telemetry' TETAP DISIMPAN AMAN di database
            cur.execute("DELETE FROM devices WHERE id = %s", (device_id,))
            
        print(f"[API] Device {device_id} unregistered from devices table. History and telemetry preserved.")
        return jsonify({'ok': True, 'device_id': device_id})
    except Exception as e:
        print(f"Error in delete_device_permanently: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>/rename', methods=['POST'])
def rename_sensor(device_id: str, phase: str):
    phase = phase.upper()
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    name = ((request.get_json(silent=True) or {}).get('name') or '').strip()
    ok, err = validate_device_name(name)
    if not ok: return jsonify({'ok': False, 'error': err}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s", (device_id,))
            row = cur.fetchone()
            sensors_list = []
            if row and row[0]:
                sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            found = False
            for s in sensors_list:
                if s.get('phase') == phase:
                    s['name'] = name
                    found = True
                    break
            if not found:
                sensors_list.append({
                    'phase': phase,
                    'name': name,
                    'enabled': True,
                    'properties': []
                })
                
            cur.execute("UPDATE devices SET sensors = %s WHERE id = %s", (json.dumps(sensors_list), device_id))
        return jsonify({'ok': True, 'name': name, 'phase': phase, 'timestamp': int(time.time() * 1000)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>/color', methods=['POST'])
def set_sensor_color(device_id: str, phase: str):
    phase = phase.upper()
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    color = ((request.get_json(silent=True) or {}).get('color') or '').strip()
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s", (device_id,))
            row = cur.fetchone()
            sensors_list = []
            if row and row[0]:
                sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            found = False
            for s in sensors_list:
                if s.get('phase') == phase:
                    s['color'] = color
                    found = True
                    break
            if not found:
                sensors_list.append({
                    'phase': phase,
                    'name': phase,
                    'color': color,
                    'enabled': True,
                    'properties': []
                })
                
            cur.execute("UPDATE devices SET sensors = %s WHERE id = %s", (json.dumps(sensors_list), device_id))
        return jsonify({'ok': True, 'color': color, 'phase': phase})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>/enabled', methods=['POST'])
def set_sensor_enabled(device_id: str, phase: str):
    phase = phase.upper()
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    body = request.get_json(silent=True) or {}
    enabled = bool(body.get('enabled', True))
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s", (device_id,))
            row = cur.fetchone()
            sensors_list = []
            if row and row[0]:
                sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            found = False
            for s in sensors_list:
                if s.get('phase') == phase:
                    s['enabled'] = enabled
                    found = True
                    break
            if not found:
                sensors_list.append({
                    'phase': phase,
                    'name': f'Sensor {phase[1:]}',
                    'enabled': enabled,
                    'properties': []
                })
                
            cur.execute("UPDATE devices SET sensors = %s WHERE id = %s", (json.dumps(sensors_list), device_id))
        return jsonify({'ok': True, 'enabled': enabled, 'phase': phase, 'device_id': device_id})
    except Exception as e:
        print(f"Error in set_sensor_enabled: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>', methods=['DELETE'])
def delete_sensor(device_id: str, phase: str):
    phase = phase.upper()
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s", (device_id,))
            row = cur.fetchone()
            sensors_list = []
            if row and row[0]:
                sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            filtered_sensors = [s for s in sensors_list if s.get('phase') != phase]
            cur.execute("UPDATE devices SET sensors = %s WHERE id = %s", (json.dumps(filtered_sensors), device_id))
            
        if device_id in _mqtt_live_data and isinstance(_mqtt_live_data[device_id], dict):
            _mqtt_live_data[device_id].pop(phase, None)

        return jsonify({'ok': True, 'phase': phase, 'device_id': device_id})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>/init', methods=['POST'])
def init_sensor(device_id: str, phase: str):
    phase = phase.upper()
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s", (device_id,))
            row = cur.fetchone()
            sensors_list = []
            if row and row[0]:
                sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            for s in sensors_list:
                if s.get('phase') == phase:
                    return jsonify({'ok': True, 'exists': True, 'sensor': s})
                    
            new_s = {'phase': phase, 'name': f'Sensor {phase[1:]}', 'enabled': True, 'properties': []}
            sensors_list.append(new_s)
            cur.execute("UPDATE devices SET sensors = %s WHERE id = %s", (json.dumps(sensors_list), device_id))
        return jsonify({'ok': True, 'sensor': new_s, 'timestamp': int(time.time() * 1000)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>/enabled', methods=['POST'])
def set_phase_enabled(device_id: str, phase: str):
    phase = phase.upper()
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    enabled = bool((request.get_json(silent=True) or {}).get('enabled', True))
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s", (device_id,))
            row = cur.fetchone()
            sensors_list = []
            if row and row[0]:
                sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
            for s in sensors_list:
                if s.get('phase') == phase:
                    s['enabled'] = enabled
                    break
            cur.execute("UPDATE devices SET sensors = %s WHERE id = %s", (json.dumps(sensors_list), device_id))
        return jsonify({'ok': True, 'phase': phase, 'enabled': enabled})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>/hourly-capture', methods=['POST'])
def trigger_hourly_capture(device_id: str):
    threading.Thread(target=_do_hourly_capture_device, args=(device_id,), daemon=True).start()
    return jsonify({'ok': True, 'device_id': device_id, 'triggered_at': _ts_now()})

@app.route('/api/hourly-capture/trigger-all', methods=['POST'])
def trigger_hourly_all():
    threading.Thread(target=_do_hourly_capture_all, daemon=True).start()
    return jsonify({'ok': True, 'triggered_at': _ts_now()})

@app.route('/api/capture/status')
def capture_status():
    req_did = (request.args.get('deviceId') or '').strip()
    with _capture_lock:
        active_devices = []
        devices_summary = {}
        target_state = None

        for did, s in _capture_states.items():
            if s.get('active') or s.get('_finalizing'):
                active_devices.append(did)
            info = {
                'active': s.get('active', False), 'device_id': did, 'device_name': s.get('device_name', did),
                'session_id': s.get('session_id'), 'session_name': s.get('session_name'), 'interval': s.get('interval', 15),
                'count': s.get('count', 0), 'started_at': s.get('started_at'), 'finalizing': s.get('_finalizing', False),
            }
            devices_summary[did] = info
            if req_did and did == req_did:
                target_state = info

        if req_did and not target_state:
            target_state = {
                'active': False, 'device_id': req_did, 'device_name': req_did,
                'session_id': None, 'session_name': None, 'interval': 15,
                'count': 0, 'started_at': None, 'finalizing': False,
            }

        if not target_state:
            first_active_did = active_devices[0] if active_devices else None
            if first_active_did:
                target_state = devices_summary[first_active_did]
            else:
                target_state = {
                    'active': False, 'device_id': None, 'device_name': None,
                    'session_id': None, 'session_name': None, 'interval': 15,
                    'count': 0, 'started_at': None, 'finalizing': False,
                }

        res = dict(target_state)
        res['active_device_ids'] = active_devices
        res['devices'] = devices_summary
        return jsonify(res)

@app.route('/api/capture/start', methods=['POST'])
def capture_start():
    body  = request.get_json(silent=True) or {}
    did   = (body.get('deviceId')    or '').strip()
    dname = (body.get('deviceName')  or '').strip()
    sname = (body.get('sessionName') or '').strip() or f'Rekaman {_ts_now()}'
    iv    = max(15, int(body.get('interval', 15)))
    hints = sorted([p for p in (body.get('phases') or []) if _PHASE_RE.match(p)], key=lambda x: int(x[1:]))
    if not did: return jsonify({'ok': False, 'error': 'deviceId harus diisi'}), 400
    if not hints: return jsonify({'ok': False, 'error': 'Minimal 1 sensor harus diaktifkan'}), 400
    with _capture_lock:
        cstate = _get_device_capture_state(did)
        if cstate.get('active') or cstate.get('_finalizing'):
            return jsonify({'ok': False, 'error': f'Capture untuk perangkat {did} sudah berjalan'}), 409
        sid    = f'session_{int(time.time() * 1000)}'
        now_s  = _ts_now()
        sensor_names = {}
        store_id = None
        try:
            with get_db_cursor() as cur:
                cur.execute("SELECT sensors, store_id FROM devices WHERE id = %s", (did,))
                row = cur.fetchone()
                if row:
                    if row[0]:
                        sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                        for s in sensors_list:
                            ph = s.get('phase')
                            nm = s.get('name')
                            if ph and nm:
                                sensor_names[ph] = nm
                    store_id = row[1]
        except Exception:
            pass

        cstate.update({
            'active': True, 'device_id': did, 'device_name': dname or did,
            'session_id': sid, 'session_name': sname, 'interval': iv,
            'count': 0, 'started_at': now_s, 'enabled_phases': hints, 'time_offset_ms': 0,
            'sensor_names': sensor_names, 'store_id': store_id,
        })
        if _mqtt_client:
            try:
                _mqtt_client.publish(f"energymeter/{did}/cmd/resetEnergy", "1")
            except Exception:
                pass
        _start_thread(did)
        _save_capture_states()
    return jsonify({'ok': True, 'session_id': sid, 'session_name': sname, 'device_id': did})

@app.route('/api/capture/stop', methods=['POST'])
def capture_stop():
    body = request.get_json(silent=True) or {}
    did  = (body.get('deviceId') or '').strip()
    if not did:
        with _capture_lock:
            active_dids = [d for d, s in _capture_states.items() if s.get('active')]
            did = active_dids[0] if active_dids else None
    if not did:
        return jsonify({'ok': False, 'error': 'Tidak ada capture aktif'}), 400
    
    stopped = _stop_device_capture(did)
    if not stopped:
        return jsonify({'ok': False, 'error': f'Tidak ada capture aktif untuk perangkat {did}'}), 400
    return jsonify({'ok': True, 'device_id': did})

@app.route('/api/capture/interval', methods=['POST'])
def capture_interval():
    body = request.get_json(silent=True) or {}
    iv = max(15, int(body.get('interval', 15)))
    did = (body.get('deviceId') or '').strip()
    with _capture_lock:
        if did and did in _capture_states:
            _capture_states[did]['interval'] = iv
            ev = _capture_states[did].get('_wake_event')
            if ev: ev.set()
        else:
            for s in _capture_states.values():
                s['interval'] = iv
                ev = s.get('_wake_event')
                if ev: ev.set()
    return jsonify({'ok': True, 'interval': iv})

@app.route('/api/capture/shift_time', methods=['POST'])
def capture_shift_time():
    body = request.get_json(silent=True) or {}
    delta_ms = int(body.get('deltaMs', 0))
    sid = body.get('sessionId')
    if not sid: return jsonify({'ok': False, 'error': 'sessionId harus diisi'}), 400
    
    with _capture_lock:
        for s in _capture_states.values():
            if s.get('active') and s.get('session_id') == sid:
                s['time_offset_ms'] = s.get('time_offset_ms', 0) + delta_ms
            
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                UPDATE history
                SET epoch = epoch + %s
                WHERE session_id = %s
            """, (delta_ms, sid))

            cur.execute("""
                SELECT id, epoch FROM history
                WHERE session_id = %s
            """, (sid,))
            rows = cur.fetchall()

            for row in rows:
                rid, new_epoch = row
                new_ts = datetime.fromtimestamp(new_epoch / 1000.0, tz=_WIB).strftime('%H:%M:%S %d/%m/%Y')
                cur.execute("""
                    UPDATE history
                    SET timestamp = %s
                    WHERE id = %s
                """, (new_ts, rid))
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/sessions')
@app.route('/api/devices/<device_id>/sessions')
def get_sessions(device_id: str = 'all'):
    try:
        is_all = not device_id or device_id.lower() == 'all'
        with get_db_cursor() as cur:
            if is_all:
                cur.execute("""
                    SELECT session_id, session_name,
                           (SELECT timestamp FROM history h2 WHERE h2.session_id = h.session_id ORDER BY h2.epoch ASC LIMIT 1) as start_time,
                           (SELECT timestamp FROM history h3 WHERE h3.session_id = h.session_id ORDER BY h3.epoch DESC LIMIT 1) as end_time,
                           COUNT(*) as record_count,
                           MIN(epoch) as start_epoch,
                           ARRAY_AGG(DISTINCT phase ORDER BY phase) as phases,
                           MAX(device_name) as snapshot_device_name,
                           h.device_id
                    FROM history h
                    GROUP BY session_id, session_name, h.device_id
                    ORDER BY start_epoch DESC
                """)
            else:
                cur.execute("""
                    SELECT session_id, session_name,
                           (SELECT timestamp FROM history h2 WHERE h2.session_id = h.session_id ORDER BY h2.epoch ASC LIMIT 1) as start_time,
                           (SELECT timestamp FROM history h3 WHERE h3.session_id = h.session_id ORDER BY h3.epoch DESC LIMIT 1) as end_time,
                           COUNT(*) as record_count,
                           MIN(epoch) as start_epoch,
                           ARRAY_AGG(DISTINCT phase ORDER BY phase) as phases,
                           MAX(device_name) as snapshot_device_name,
                           h.device_id
                    FROM history h
                    WHERE device_id = %s
                    GROUP BY session_id, session_name, h.device_id
                    ORDER BY start_epoch DESC
                """, (device_id,))
            rows = cur.fetchall()

        # Ambil nama device dari database sebagai fallback
        device_names_map = {}
        try:
            with get_db_cursor() as cur:
                cur.execute("SELECT id, name FROM devices")
                for d_id, d_name in cur.fetchall():
                    if d_name:
                        device_names_map[d_id] = d_name
        except Exception:
            pass

        # Ambil nama kustom sensor yang direkam pada history (terisolasi per session)
        session_phase_names = {}
        try:
            with get_db_cursor() as cur:
                if is_all:
                    cur.execute("""
                        SELECT session_id, phase, MAX(phase_name)
                        FROM history
                        GROUP BY session_id, phase
                    """)
                else:
                    cur.execute("""
                        SELECT session_id, phase, MAX(phase_name)
                        FROM history
                        WHERE device_id = %s
                        GROUP BY session_id, phase
                    """, (device_id,))
                hist_phases = cur.fetchall()
            for sid, ph, ph_name in hist_phases:
                if sid not in session_phase_names:
                    session_phase_names[sid] = {}
                session_phase_names[sid][ph] = ph_name or ph
        except Exception:
            pass

        sessions = []
        for row in rows:
            sid, sname, start_time, end_time, count, start_epoch, phases_arr, snapshot_dname, row_did = row
            phases_list = sorted(phases_arr or [], key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
            cur_dname = device_names_map.get(row_did, row_did)
            sessions.append({
                'id': sid,
                'name': sname,
                'startTime': start_time,
                'endTime': end_time,
                'recordCount': count,
                'startTimestamp': start_epoch,
                'deviceId': row_did,
                'deviceName': snapshot_dname or cur_dname,
                'phases': phases_list,
                'phaseNames': session_phase_names.get(sid, {ph: ph for ph in phases_list}),
            })

        # Gabungkan active session dari memory jika sedang berjalan
        with _capture_lock:
            for active_did, cstate in _capture_states.items():
                if is_all or active_did == device_id:
                    if cstate.get('active'):
                        active_sid = cstate.get('session_id')
                        active_sname = cstate.get('session_name')
                        active_start = cstate.get('started_at')
                        active_count = cstate.get('count', 0)
                        active_phases = cstate.get('enabled_phases', [])
                        active_names = cstate.get('sensor_names', {})
                        cur_dname = device_names_map.get(active_did, active_did)

                        existing = next((s for s in sessions if s['id'] == active_sid), None)
                        if existing:
                            existing['recordCount'] = max(existing['recordCount'], active_count)
                        else:
                            sessions.insert(0, {
                                'id': active_sid,
                                'name': active_sname,
                                'startTime': active_start,
                                'endTime': None,
                                'recordCount': active_count,
                                'startTimestamp': int(time.time() * 1000),
                                'deviceId': active_did,
                                'deviceName': cstate.get('device_name') or cur_dname,
                                'phases': active_phases,
                                'phaseNames': active_names or {ph: ph for ph in active_phases},
                            })

        return jsonify(sessions)
    except Exception as e:
        print(f"Error getting sessions: {e}")
        return jsonify([]), 500


@app.route('/api/devices/<device_id>/history/<session_id>/<phase>')
def get_session_history_phase(device_id: str, session_id: str, phase: str):
    try:
        phase = phase.upper()
        with get_db_cursor() as cur:
            if not device_id or device_id.lower() == 'all':
                cur.execute("""
                    SELECT timestamp, epoch, voltage, current, power, frequency, energy, power_factor, offline, device_id
                    FROM history
                    WHERE session_id = %s AND phase = %s
                    ORDER BY epoch ASC
                """, (session_id, phase))
            else:
                cur.execute("""
                    SELECT timestamp, epoch, voltage, current, power, frequency, energy, power_factor, offline, device_id
                    FROM history
                    WHERE device_id = %s AND session_id = %s AND phase = %s
                    ORDER BY epoch ASC
                """, (device_id, session_id, phase))
            rows = cur.fetchall()
            
        import math
        history = {}
        for row in rows:
            ts, epoch_val, v, c, w, hz, kwh, pf, offline = row
            key = f'capture_{epoch_val}'
            
            apparent = (v * c) / 1000.0
            power_kw = w / 1000.0
            reactive = math.sqrt(max(0.0, (apparent ** 2) - (power_kw ** 2)))
            try:
                phase_angle = math.acos(max(-1.0, min(1.0, pf))) * 180.0 / math.pi
            except:
                phase_angle = 0.0
                
            history[key] = {
                'timestamp': ts,
                'epoch': epoch_val,
                'offline': offline,
                'Voltage': v,
                'Current': c,
                'Power': w,
                'Frequency': hz,
                'Energy': kwh,
                'PowerFactor': pf,
                'Apparent': round(apparent, 4),
                'Reactive': round(reactive, 4),
                'Phase1': round(phase_angle, 2),
                'EnergyApparent': 0.0,
                'EnergyReactive': 0.0
            }
            
        if history:
            with get_db_cursor() as cur:
                cur.execute("""
                    SELECT session_name, MIN(timestamp), MAX(timestamp), COUNT(*)
                    FROM history
                    WHERE device_id = %s AND session_id = %s
                    GROUP BY session_name
                """, (device_id, session_id))
                m_row = cur.fetchone()
            if m_row:
                sname, start, end, count = m_row
                history['_meta'] = {
                    'id': session_id,
                    'name': sname,
                    'deviceId': device_id,
                    'startTime': start,
                    'endTime': end,
                    'recordCount': count
                }
        return jsonify(history)
    except Exception as e:
        print(f"Error in get_session_history_phase: {e}")
        return jsonify({}), 500

@app.route('/api/history/session-chart-data/<session_id>')
def get_session_chart_data(session_id: str):
    try:
        page_size = min(max(500, int(request.args.get('page_size', 3000))), 10000)
        show_all = request.args.get('all', 'false').lower() == 'true'

        with get_db_cursor() as cur:
            cur.execute("""
                SELECT timestamp, epoch, phase, voltage, current, power, frequency, energy, power_factor, offline, phase_name, device_id, session_name
                FROM history
                WHERE session_id = %s
                ORDER BY epoch ASC
            """, (session_id,))
            rows = cur.fetchall()

        if not rows:
            return jsonify({'meta': {}, 'data': []})

        import math
        buckets = {}
        phase_names = {}
        device_id = rows[0][11]
        session_name = rows[0][12]

        for row in rows:
            ts, epoch_val, phase, v, c, w, hz, kwh, pf, offline, ph_name, did, sname = row
            ts_ms = int(epoch_val)
            if ts_ms not in buckets:
                buckets[ts_ms] = {}
            if ph_name and phase not in phase_names:
                phase_names[phase] = ph_name

            apparent = (v * c) / 1000.0
            power_kw = w / 1000.0
            reactive = math.sqrt(max(0.0, (apparent ** 2) - (power_kw ** 2)))
            try:
                phase_angle = math.acos(max(-1.0, min(1.0, pf))) * 180.0 / math.pi
            except Exception:
                phase_angle = 0.0

            buckets[ts_ms][phase] = {
                'Voltage':     v,
                'Current':     c,
                'Power':       w,
                'Frequency':   hz,
                'Energy':      kwh,
                'PowerFactor': pf,
                'Apparent':    round(apparent, 4),
                'Reactive':    round(reactive, 4),
                'Phase1':      round(phase_angle, 2),
                'offline':     bool(offline),
            }

        sorted_ts = sorted(buckets.keys())
        total_time_slots = len(sorted_ts)

        page_raw = str(request.args.get('page', 'last')).lower()
        if show_all or total_time_slots <= page_size:
            sampled_ts = sorted_ts
            total_pages = 1
            current_page = 1
        else:
            total_pages = math.ceil(total_time_slots / page_size)
            if page_raw in ('last', 'latest'):
                current_page = total_pages
            else:
                try:
                    current_page = min(max(1, int(page_raw)), total_pages)
                except ValueError:
                    current_page = total_pages
            start_idx = (current_page - 1) * page_size
            end_idx = start_idx + page_size
            sampled_ts = sorted_ts[start_idx:end_idx]

        result_data = [{'timestamp': ts, 'data': buckets[ts]} for ts in sampled_ts]
        phases = sorted(list({ph for ts in buckets for ph in buckets[ts]}), key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)

        meta = {
            'sessionId': session_id,
            'sessionName': session_name,
            'deviceId': device_id,
            'totalRows': len(rows),
            'totalTimeSlots': total_time_slots,
            'sampledCount': len(result_data),
            'page': current_page,
            'totalPages': total_pages,
            'pageSize': page_size,
            'phases': phases,
            'phaseNames': phase_names,
        }

        return jsonify({'meta': meta, 'data': result_data})
    except Exception as e:
        print(f"Error in get_session_chart_data: {e}")
        return jsonify({'meta': {}, 'data': []}), 500


@app.route('/api/capture/rename-session', methods=['POST'])
def rename_session():
    body = request.get_json(silent=True) or {}
    sid = body.get('sessionId')
    name = (body.get('name') or '').strip()
    if not sid or not name: return jsonify({'ok': False, 'error': 'Invalid parameters'}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                UPDATE history
                SET session_name = %s
                WHERE session_id = %s
            """, (name, sid))
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/capture/rename-session-sensor', methods=['POST'])
def rename_session_sensor():
    body = request.get_json(silent=True) or {}
    sid = body.get('sessionId')
    phase = body.get('phase')
    name = (body.get('name') or '').strip()
    if not sid or not phase or not name:
        return jsonify({'ok': False, 'error': 'Invalid parameters'}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                UPDATE history
                SET phase_name = %s
                WHERE session_id = %s AND phase = %s
            """, (name, sid, phase.upper()))
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/capture/delete-session', methods=['POST'])
def delete_session():
    body = request.get_json(silent=True) or {}
    sid = body.get('sessionId')
    if not sid: return jsonify({'ok': False, 'error': 'Invalid parameters'}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                DELETE FROM history
                WHERE session_id = %s
            """, (sid,))
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/capture/clear-all', methods=['POST'])
def clear_all_sessions():
    body = request.get_json(silent=True) or {}
    did = body.get('deviceId')
    if not did: return jsonify({'ok': False, 'error': 'Invalid parameters'}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                DELETE FROM history
                WHERE device_id = %s
            """, (did,))
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>/chart-data')
def get_device_chart_data(device_id: str):
    try:
        date_str = request.args.get('date')
        range_type = request.args.get('range', 'day')
        
        if not date_str:
            date_str = datetime.now(_WIB).strftime('%d/%m/%Y')
            
        if '-' in date_str:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            date_ddmmyyyy = dt.strftime('%d/%m/%Y')
        else:
            dt = datetime.strptime(date_str, '%d/%m/%Y')
            date_ddmmyyyy = date_str
            
        import math
        buckets = {}
        
        if range_type == 'day':
            with get_db_cursor() as cur:
                cur.execute("""
                    SELECT phase, epoch, voltage, current, power, frequency, energy, power_factor, offline
                    FROM telemetry
                    WHERE device_id = %s
                      AND timestamp LIKE %s
                    ORDER BY epoch ASC
                """, (device_id, f'% {date_ddmmyyyy}'))
                rows = cur.fetchall()

            for row in rows:
                phase, epoch_val, v, c, w, hz, kwh, pf, is_offline = row
                ts_ms = int(epoch_val)
                if ts_ms not in buckets:
                    buckets[ts_ms] = {}

                apparent = (v * c) / 1000.0
                power_kw = w / 1000.0
                reactive = math.sqrt(max(0.0, (apparent ** 2) - (power_kw ** 2)))
                try:
                    phase_angle = math.acos(max(-1.0, min(1.0, pf))) * 180.0 / math.pi
                except:
                    phase_angle = 0.0

                buckets[ts_ms][phase] = {
                    'Voltage':     v,
                    'Current':     c,
                    'Power':       w,
                    'Frequency':   hz,
                    'Energy':      kwh,
                    'PowerFactor': pf,
                    'Apparent':    round(apparent, 4),
                    'Reactive':    round(reactive, 4),
                    'Phase1':      round(phase_angle, 2),
                    'offline':     bool(is_offline),
                }

            result = [{'timestamp': ts, 'data': d} for ts, d in sorted(buckets.items())]
            return jsonify(result)
            
        elif range_type == 'week':
            start_date = dt - timedelta(days=6)
            start_epoch = int(datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=_WIB).timestamp() * 1000)
            end_epoch = int(datetime(dt.year, dt.month, dt.day, 23, 59, 59, tzinfo=_WIB).timestamp() * 1000)
            
            with get_db_cursor() as cur:
                cur.execute("""
                    SELECT
                        phase,
                        RIGHT(timestamp, 10) as date_part,
                        AVG(voltage) as v,
                        AVG(current) as c,
                        AVG(power) as w,
                        AVG(frequency) as hz,
                        AVG(energy) as kwh,
                        AVG(power_factor) as pf,
                        MIN(epoch) as min_epoch
                    FROM telemetry
                    WHERE device_id = %s
                      AND epoch BETWEEN %s AND %s
                    GROUP BY phase, RIGHT(timestamp, 10)
                    ORDER BY min_epoch ASC
                """, (device_id, start_epoch, end_epoch))
                rows = cur.fetchall()
                
            for row in rows:
                phase, date_part, v, c, w, hz, kwh, pf, min_epoch = row
                try:
                    d_dt = datetime.strptime(date_part, '%d/%m/%Y')
                    ts_ms = int(d_dt.timestamp() * 1000)
                except:
                    ts_ms = int(min_epoch)
                    
                if ts_ms not in buckets:
                    buckets[ts_ms] = {}
                    
                apparent = (v * c) / 1000.0
                power_kw = w / 1000.0
                reactive = math.sqrt(max(0.0, (apparent ** 2) - (power_kw ** 2)))
                try:
                    phase_angle = math.acos(max(-1.0, min(1.0, pf))) * 180.0 / math.pi
                except:
                    phase_angle = 0.0
                    
                buckets[ts_ms][phase] = {
                    'Voltage':          round(v, 4),
                    'Current':          round(c, 4),
                    'Power':            round(w, 4),
                    'Frequency':         round(hz, 4),
                    'Energy':           round(kwh, 4),
                    'PowerFactor':      round(pf, 4),
                    'Apparent':         round(apparent, 4),
                    'Reactive':         round(reactive, 4),
                    'Phase1':           round(phase_angle, 2),
                }
                
            result = [{'timestamp': ts, 'data': d} for ts, d in sorted(buckets.items())]
            return jsonify(result)
            
    except Exception as e:
        print(f"Error in get_device_chart_data: {e}")
        return jsonify([]), 500

@app.route('/api/capture/log')
def get_capture_log():
    try:
        if os.path.exists("capture_error.log"):
            with open("capture_error.log", "r") as f:
                return f.read(), 200, {'Content-Type': 'text/plain'}
        return "No errors logged yet.", 200, {'Content-Type': 'text/plain'}
    except Exception as e:
        return str(e), 500, {'Content-Type': 'text/plain'}

if __name__ == '__main__':
    flask_host = os.environ.get("FLASK_HOST", "0.0.0.0")
    try:
        flask_port = int(os.environ.get("FLASK_PORT", "5000"))
    except ValueError:
        flask_port = 5000
    
    _load_capture_states()
    
    app.run(debug=True, host=flask_host, port=flask_port)
