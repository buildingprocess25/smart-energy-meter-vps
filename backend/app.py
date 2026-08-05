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
_db_init_lock = threading.Lock()

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
                    cur.execute("ALTER TABLE history ADD COLUMN IF NOT EXISTS phase_name VARCHAR(100) DEFAULT NULL;")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_device_epoch ON telemetry(device_id, epoch);")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_device_phase ON telemetry(device_id, phase);")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_history_session ON history(session_id);")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_history_device_session ON history(device_id, session_id);")

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
                    cur.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_telemetry_unique_slot
                        ON telemetry(device_id, phase, timestamp)
                    """)
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
    if not name:        return False, 'Nama tidak boleh kosong'
    if len(name) < 2:   return False, 'Nama minimal 2 karakter' 
    if len(name) > 100: return False, 'Nama maksimal 100 karakter'
    if any(c in name for c in '/.$#[]'): return False, 'Karakter tidak diizinkan: / . $ # [ ]'
    return True, ''
def validate_phase_key(phase: str) -> bool: return bool(_PHASE_RE.match(phase))
_capture_lock  = threading.Lock()
_capture_state = {
    'active': False, 'device_id': None, 'device_name': None,
    'session_id': None, 'session_name': None, 'interval': 15,
    'count': 0, 'started_at': None, 'enabled_phases': None,
    '_thread': None, '_stop_event': None, '_wake_event': None, '_finalizing': False, 'time_offset_ms': 0,
}
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
            sname = _capture_state.get('session_name') or 'Rekaman'
            sensor_names = _capture_state.get('sensor_names') or {}
            
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
                        offline, session_id, session_name, phase_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    device_id, ph, ts, epoch_val,
                    f('Voltage (V)'), f('Current (A)'), f('Power (W)'),
                    f('Frequency (Hz)'), f('Active Energy (kWh)'), f('Power Factor'),
                    phase_offline, session_id, sname, sensor_names.get(ph, ph)
                ))
                
        with _capture_lock:
            _capture_state['count'] += len(phases)
    except Exception as e:
        print(f"Error in _do_capture_io: {e}")

def _capture_worker(stop: threading.Event, wake: threading.Event) -> None:
    last_hash: list = [None]; last_change: list = [None]
    nxt = time.time() + 3  # 3s: cukup untuk reset energi ESP32, jauh lebih cepat dari 10s
    while not stop.is_set():
        if stop.wait(timeout=min(max(0., nxt - time.time()), 0.2)): break
        if wake.is_set():
            wake.clear()
            with _capture_lock: nxt = time.time()
            continue
        if time.time() < nxt: continue
        with _capture_lock:
            if not _capture_state['active']: break
            sid = _capture_state['session_id']; did = _capture_state['device_id']
            iv  = float(_capture_state['interval']); ep = _capture_state.get('enabled_phases')
            to_ms = _capture_state.get('time_offset_ms', 0)
        sched = nxt; nxt += iv
        threading.Thread(target=_do_capture_io, args=(did, sid, sched, iv, last_hash, last_change, ep, to_ms), daemon=True).start()

def _start_thread() -> None:
    se, we = threading.Event(), threading.Event()
    t = threading.Thread(target=_capture_worker, args=(se, we), daemon=True)
    t.start()
    _capture_state.update({'_thread': t, '_stop_event': se, '_wake_event': we})

def _finalize_bg(sid, did, count, se, th, enabled_phases, time_offset_ms) -> None:
    try:
        if se: se.set()
        if th and th.is_alive(): th.join(timeout=5)
    except Exception as e:
        print(f"Error in _finalize_bg: {e}")
    finally:
        with _capture_lock: _capture_state['_finalizing'] = False

def _stop_and_respond() -> None:
    with _capture_lock:
        if not _capture_state['active']: return
        _capture_state.update({'active': False, '_finalizing': True})
        sid = _capture_state['session_id']; did = _capture_state['device_id']
        cnt = _capture_state['count'];      se  = _capture_state.get('_stop_event')
        th  = _capture_state.get('_thread'); ep = _capture_state.get('enabled_phases')
        to_ms = _capture_state.get('time_offset_ms', 0)
        _capture_state.update({
            'device_id': None, 'device_name': None, 'session_id': None, 'session_name': None,
            'count': 0, 'started_at': None, 'enabled_phases': None,
            '_thread': None, '_stop_event': None, '_wake_event': None,
        })
    threading.Thread(target=_finalize_bg, args=(sid, did, cnt, se, th, ep, to_ms), daemon=True).start()

_APP_VERSION = int(time.time())

@app.route('/')
def index():
    return render_template('index.html', v=_APP_VERSION)

@app.route('/health', methods=['GET'])
def health(): return jsonify({"status": "ok", "message": "Service is alive"}), 200

@app.route('/api/config')
def get_config(): return jsonify({})

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
                'phaseCount': len(phases)
            })
        return jsonify(devices)

    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT id, name, online, last_seen, sensors FROM devices")
            rows = cur.fetchall()
        for row in rows:
            did, name, online, last_seen, sensors_json = row
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
                'phaseCount': len(phases)
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
    name = ((request.get_json(silent=True) or {}).get('name') or '').strip()
    ok, err = validate_device_name(name)
    if not ok: return jsonify({'ok': False, 'error': err}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                INSERT INTO devices (id, name, online, last_seen)
                VALUES (%s, %s, FALSE, '---')
                ON CONFLICT (id) DO UPDATE SET name = %s;
            """, (device_id, name, name))
        return jsonify({'ok': True, 'name': name, 'timestamp': int(time.time() * 1000)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/devices/<device_id>', methods=['DELETE'])
def delete_device_permanently(device_id: str):
    try:
        # Hapus cache memori backend agar worker tidak mendeteksi dan mendaftarkannya lagi
        _mqtt_live_data.pop(device_id, None)
        _mqtt_last_seen.pop(device_id, None)
        _db_last_ping.pop(device_id, None)
        _device_live_hash.pop(device_id, None)
        _device_last_change_ms.pop(device_id, None)
        _device_is_offline.pop(device_id, None)
        _device_live_buffer.pop(device_id, None)
        
        with get_db_cursor() as cur:
            # Hapus data histori capture
            cur.execute("DELETE FROM history WHERE device_id = %s", (device_id,))
            # Hapus data snapshot telemetry
            cur.execute("DELETE FROM telemetry WHERE device_id = %s", (device_id,))
            # Hapus metadata device
            cur.execute("DELETE FROM devices WHERE id = %s", (device_id,))
            
        print(f"[API] Device {device_id} deleted permanently.")
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
    with _capture_lock:
        s = _capture_state
        return jsonify({
            'active': s['active'], 'device_id': s['device_id'], 'device_name': s['device_name'],
            'session_id': s['session_id'], 'session_name': s['session_name'], 'interval': s['interval'],
            'count': s['count'], 'started_at': s['started_at'], 'finalizing': s.get('_finalizing', False),
        })

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
        if _capture_state['active'] or _capture_state.get('_finalizing'):
            return jsonify({'ok': False, 'error': 'Capture sudah berjalan atau sedang finalisasi'}), 409
        sid    = f'session_{int(time.time() * 1000)}'
        now_s  = _ts_now()
        # Ambil nama sensor terdaftar di DB saat capture dimulai
        sensor_names = {}
        try:
            with get_db_cursor() as cur:
                cur.execute("SELECT sensors FROM devices WHERE id = %s", (did,))
                row = cur.fetchone()
                if row and row[0]:
                    sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                    for s in sensors_list:
                        ph = s.get('phase')
                        nm = s.get('name')
                        if ph and nm:
                            sensor_names[ph] = nm
        except Exception:
            pass

        _capture_state.update({
            'active': True, 'device_id': did, 'device_name': dname or did,
            'session_id': sid, 'session_name': sname, 'interval': iv,
            'count': 0, 'started_at': now_s, 'enabled_phases': hints, 'time_offset_ms': 0,
            'sensor_names': sensor_names,
        })
        if _mqtt_client:
            try:
                _mqtt_client.publish(f"energymeter/{did}/cmd/resetEnergy", "1")
            except Exception:
                pass
        _start_thread()
    return jsonify({'ok': True, 'session_id': sid, 'session_name': sname, 'device_id': did})

@app.route('/api/capture/stop', methods=['POST'])
def capture_stop():
    with _capture_lock:
        if not _capture_state['active']: return jsonify({'ok': False, 'error': 'Tidak ada capture aktif'}), 400
    _stop_and_respond()
    return jsonify({'ok': True})

@app.route('/api/capture/interval', methods=['POST'])
def capture_interval():
    iv = max(15, int((request.get_json(silent=True) or {}).get('interval', 15)))
    with _capture_lock:
        _capture_state['interval'] = iv
        ev = _capture_state.get('_wake_event')
        if ev: ev.set()
    return jsonify({'ok': True, 'interval': iv})

@app.route('/api/capture/shift_time', methods=['POST'])
def capture_shift_time():
    body = request.get_json(silent=True) or {}
    delta_ms = int(body.get('deltaMs', 0))
    sid = body.get('sessionId')
    if not sid: return jsonify({'ok': False, 'error': 'sessionId harus diisi'}), 400
    
    with _capture_lock:
        if _capture_state['active'] and _capture_state['session_id'] == sid:
            _capture_state['time_offset_ms'] = _capture_state.get('time_offset_ms', 0) + delta_ms
            
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

@app.route('/api/devices/<device_id>/sessions')
def get_sessions(device_id: str):
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT session_id, session_name,
                       (SELECT timestamp FROM history h2 WHERE h2.session_id = h.session_id ORDER BY h2.epoch ASC LIMIT 1) as start_time,
                       (SELECT timestamp FROM history h3 WHERE h3.session_id = h.session_id ORDER BY h3.epoch DESC LIMIT 1) as end_time,
                       COUNT(*) as record_count,
                       MIN(epoch) as start_epoch,
                       ARRAY_AGG(DISTINCT phase ORDER BY phase) as phases
                FROM history h
                WHERE device_id = %s
                GROUP BY session_id, session_name
                ORDER BY start_epoch DESC
            """, (device_id,))
            rows = cur.fetchall()

        # Ambil nama device dari database
        device_name = device_id
        try:
            with get_db_cursor() as cur:
                cur.execute("SELECT name FROM devices WHERE id = %s", (device_id,))
                dev_row = cur.fetchone()
                if dev_row:
                    device_name = dev_row[0]
        except Exception:
            pass

        # Ambil nama kustom sensor yang direkam pada history (terisolasi per session)
        session_phase_names = {}
        try:
            with get_db_cursor() as cur:
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
            sid, sname, start_time, end_time, count, start_epoch, phases_arr = row
            # phases_arr adalah list dari PostgreSQL ARRAY_AGG, misal ['L1','L2','L3']
            phases_list = sorted(phases_arr or [], key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
            sessions.append({
                'id': sid,
                'name': sname,
                'startTime': start_time,
                'endTime': end_time,
                'recordCount': count,
                'startTimestamp': start_epoch,
                'deviceId': device_id,
                'deviceName': device_name,
                'phases': phases_list,           # ['L1', 'L2', 'L3']
                'phaseNames': session_phase_names.get(sid, {ph: ph for ph in phases_list}),
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
            cur.execute("""
                SELECT timestamp, epoch, voltage, current, power, frequency, energy, power_factor, offline
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

if __name__ == '__main__':
    flask_host = os.environ.get("FLASK_HOST", "0.0.0.0")
    try:
        flask_port = int(os.environ.get("FLASK_PORT", "5000"))
    except ValueError:
        flask_port = 5000
    app.run(debug=True, host=flask_host, port=flask_port)
