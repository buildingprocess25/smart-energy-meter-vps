from __future__ import annotations
import os, re, threading, time, hashlib, hmac, json, logging, contextlib, csv, io, secrets, sqlite3
from collections import deque
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request, send_file, redirect, Response, stream_with_context, make_response
import requests as http_requests
from werkzeug.exceptions import HTTPException
import paho.mqtt.client as paho_mqtt
import psycopg2
from psycopg2 import pool

log = logging.getLogger('werkzeug')
# Load .env baik dari root directory maupun folder backend/
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
load_dotenv()

_WIB = timezone(timedelta(hours=7))

_mqtt_live_data = {}
_mqtt_last_seen = {}
_db_last_ping = {}
_buffer_sync_status = {}  # device_id -> {'status': str, 'total': int, 'replayed': int, 'downtime_sec': int, 'start_time': float, 'last_updated': float}
_device_last_uptime_sec = {}
_device_last_reboot_ts = {}
_device_online_since = {}
_device_recovered_outage = {}
_legacy_status_last_polled = {}


def _request_legacy_status_if_needed(device_id: str, min_interval: float = 45.0):
    if not device_id or device_id.upper().startswith(('CT-', 'PLUG-')):
        return
    now = time.time()
    if (now - _legacy_status_last_polled.get(device_id, 0.0)) < min_interval:
        return
    _legacy_status_last_polled[device_id] = now
    try:
        if _mqtt_client is not None:
            _mqtt_client.publish(f"AlfaEnergy/{device_id}/cmd/getStatus", "1")
            _mqtt_client.publish(f"energymeter/{device_id}/cmd/getStatus", "1")
    except Exception:
        pass


def _parse_uptime_seconds(raw_uptime) -> int | None:
    if raw_uptime is None:
        return None
    if isinstance(raw_uptime, (int, float)):
        return max(0, int(raw_uptime))
    s = str(raw_uptime).strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    # Tasmota format: "0T02:14:30" or "14T03:20:10"
    m = re.match(r'^(\d+)\s*[Td]\s*(\d{1,2}):(\d{2}):(\d{2})$', s)
    if m:
        return int(m.group(1)) * 86400 + int(m.group(2)) * 3600 + int(m.group(3)) * 60 + int(m.group(4))
    m2 = re.match(r'^(\d{1,2}):(\d{2}):(\d{2})$', s)
    if m2:
        return int(m2.group(1)) * 3600 + int(m2.group(2)) * 60 + int(m2.group(3))
    return None


def _update_device_uptime(device_id: str, raw_uptime) -> None:
    if raw_uptime is None:
        return
    if 'status' not in _mqtt_live_data[device_id]:
        _mqtt_live_data[device_id]['status'] = {}
    _mqtt_live_data[device_id]['status']['uptime'] = raw_uptime
    up_sec = _parse_uptime_seconds(raw_uptime)
    if up_sec is not None:
        _mqtt_live_data[device_id]['status']['uptime_sec'] = up_sec
        prev_sec = _device_last_uptime_sec.get(device_id)
        if prev_sec is not None and (up_sec + 15) < prev_sec:
            _device_last_reboot_ts[device_id] = int(time.time() * 1000)
        _device_last_uptime_sec[device_id] = up_sec

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

def _process_buffered_record(device_id: str, phase: str, rec_ts: str, rec_epoch: int,
                             slot_ts: str, slot_epoch: int, seq: int,
                             v: float, c: float, w: float, tot: float, pf: float, hz: float):
    """
    Menyimpan data offline buffer LittleFS dari Sparta CT / Sparta Plug:
    1. Menyimpan data detail per menit ke tabel buffered_telemetry.
    2. Melakukan Gap-Healing pada tabel telemetry (slot 15 menit), mengganti row offline (0.0) dengan data riil.
    """
    if not db_pool:
        return
    try:
        with get_db_cursor() as cur:
            # 1. Simpan rekaman detail ke tabel buffered_telemetry
            cur.execute("""
                INSERT INTO buffered_telemetry (
                    device_id, phase, timestamp, epoch, seq,
                    voltage, current, power, frequency, energy, power_factor, replayed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (device_id, phase, epoch) DO UPDATE SET
                    voltage = EXCLUDED.voltage,
                    current = EXCLUDED.current,
                    power = EXCLUDED.power,
                    energy = EXCLUDED.energy,
                    power_factor = EXCLUDED.power_factor,
                    replayed_at = CURRENT_TIMESTAMP;
            """, (device_id, phase, rec_ts, rec_epoch, seq, v, c, w, hz, tot, pf))

            # 2. Gap-Healing pada tabel telemetry: pulihkan slot 15-menit yang sebelumnya tercatat offline (0.0)
            cur.execute("""
                UPDATE telemetry
                SET voltage = %s, current = %s, power = %s, frequency = %s,
                    energy = %s, power_factor = %s, offline = FALSE
                WHERE device_id = %s AND phase = %s AND timestamp = %s AND offline = TRUE;
            """, (v, c, w, hz, tot, pf, device_id, phase, slot_ts))

            # Jika baris slot 15-menit ini belum ada sama sekali di tabel telemetry (misal VPS mati saat alat offline)
            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO telemetry (
                        device_id, phase, timestamp, epoch,
                        voltage, current, power, frequency, energy, power_factor, offline
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)
                    ON CONFLICT (device_id, phase, timestamp) DO NOTHING;
                """, (device_id, phase, slot_ts, slot_epoch, v, c, w, hz, tot, pf))
    except Exception as e:
        print(f"[Buffer Processing Error] device={device_id} seq={seq}: {e}")

def _on_message(client, userdata, msg):
    try:
        if msg.retain:
            return
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        parts = topic.split('/')
        if len(parts) >= 2 and parts[0] in ('energymeter', 'AlfaEnergy', 'SpartaEnergy'):
            # Format dinamis khusus CT & Plug: <TIPE>-<MAC> (contoh: CT-E62A34, Plug-B80E68)
            # Alat lama (EM-0001, MC3, dll.) tetap mempertahankan ID aslinya
            device_type = None
            if len(parts) >= 4 and parts[0] in ('AlfaEnergy', 'SpartaEnergy') and parts[1].upper() in ('CT', 'PLUG'):
                device_type = 'CT' if parts[1].upper() == 'CT' else 'Plug'
                raw_mac = parts[2].strip().rstrip(':').strip()
                device_id = f"{device_type}-{raw_mac}"
                sub_parts = parts[3:]
            else:
                device_id = parts[1].strip().rstrip(':').strip()
                sub_parts = parts[2:]

            if not device_id or device_id == 'all' or (device_type and raw_mac.lower() == 'all'):
                return
            _now_seen = time.time()
            _prev_seen = _mqtt_last_seen.get(device_id, 0)
            if device_id not in _device_online_since:
                _device_online_since[device_id] = _now_seen
            elif _prev_seen > 0 and (_now_seen - _prev_seen) > 120:
                _device_online_since[device_id] = _now_seen
                _device_recovered_outage[device_id] = _now_seen
            _mqtt_last_seen[device_id] = _now_seen
            if device_id not in _mqtt_live_data:
                _mqtt_live_data[device_id] = {}
            if device_type:
                if 'status' not in _mqtt_live_data[device_id]:
                    _mqtt_live_data[device_id]['status'] = {}
                _mqtt_live_data[device_id]['status']['device_type'] = device_type
                _mqtt_live_data[device_id]['status']['prefix'] = parts[0]

            # Case 1: Timestamp -> AlfaEnergy/EM-0001/Timestamp atau energymeter/MC3/Timestamp
            if len(sub_parts) == 1 and sub_parts[0] == 'Timestamp':
                _mqtt_live_data[device_id]['Timestamp'] = payload
                _request_legacy_status_if_needed(device_id, min_interval=45.0)

            # Case 2: Status -> AlfaEnergy/EM-0001/status/...
            elif len(sub_parts) >= 2 and sub_parts[0] == 'status':
                if 'status' not in _mqtt_live_data[device_id]:
                    _mqtt_live_data[device_id]['status'] = {}
                clean_p = payload.replace('\x00', '').strip()
                _mqtt_live_data[device_id]['status'][sub_parts[1]] = clean_p
                if sub_parts[1].lower() == 'uptime':
                    _update_device_uptime(device_id, clean_p)
                elif clean_p.startswith('{'):
                    try:
                        st_json = json.loads(clean_p)
                        if isinstance(st_json, dict):
                            up_val = st_json.get('uptime') if 'uptime' in st_json else st_json.get('Uptime')
                            if up_val is not None:
                                _update_device_uptime(device_id, up_val)
                    except Exception as ex:
                        _mqtt_live_data[device_id]['status']['parse_err'] = str(ex)

            # Case 2B: Tasmota tele/stat (AlfaEnergy CT & Plug) -> tele/SENSOR, tele/STATE, stat/STATE, stat/POWER, etc.
            elif len(sub_parts) >= 2 and sub_parts[0].lower() in ('tele', 'stat'):
                sub = sub_parts[1].upper()
                if sub == 'SENSOR':
                    try:
                        data = json.loads(payload)
                        # Timestamp dari alat (format ISO 8601: 2026-10-16T12:05:37)
                        time_str = data.get('Time')
                        if time_str:
                            try:
                                dt = datetime.fromisoformat(time_str)
                                _mqtt_live_data[device_id]['Timestamp'] = dt.strftime('%H:%M:%S %d/%m/%Y')
                            except Exception:
                                _mqtt_live_data[device_id]['Timestamp'] = str(time_str)

                        # Deteksi object energi di JSON Tasmota (ENERGY, ENERGY1, PZEM, dll)
                        energy_keys = [k for k in data if k == 'ENERGY' or (k.startswith('ENERGY') and k[6:].isdigit())]
                        if not energy_keys:
                            for k in ('PZEM004T', 'PZEM', 'CSE7766', 'BL0937', 'BL0942'):
                                if k in data and isinstance(data[k], dict):
                                    energy_keys = [k]
                                    break

                        def _get_metric(obj, key, idx=None):
                            if not isinstance(obj, dict):
                                return None
                            val = obj.get(key)
                            if val is None:
                                for k_lower, v_lower in obj.items():
                                    if k_lower.lower() == key.lower():
                                        val = v_lower
                                        break
                            if isinstance(val, list) and idx is not None:
                                val = val[idx] if idx < len(val) else None
                            try:
                                return float(val)
                            except (TypeError, ValueError):
                                return None

                        def _build_metrics(e_obj, idx=None):
                            v = _get_metric(e_obj, 'Voltage', idx)
                            c = _get_metric(e_obj, 'Current', idx)
                            w = _get_metric(e_obj, 'Power', idx)
                            hz = _get_metric(e_obj, 'Frequency', idx)
                            kwh = _get_metric(e_obj, 'Total', idx)
                            if kwh is None:
                                kwh = _get_metric(e_obj, 'Energy', idx)
                            pf = _get_metric(e_obj, 'Factor', idx)
                            if pf is None:
                                pf = _get_metric(e_obj, 'PowerFactor', idx)

                            metrics = {}
                            if v is not None: metrics['Voltage (V)'] = v
                            if c is not None: metrics['Current (A)'] = c
                            if w is not None: metrics['Power (W)'] = w
                            if hz is not None: metrics['Frequency (Hz)'] = hz
                            if kwh is not None: metrics['Active Energy (kWh)'] = kwh
                            if pf is not None: metrics['Power Factor'] = pf

                            va = _get_metric(e_obj, 'ApparentPower', idx)
                            if va is not None:
                                metrics['Apparent Power (kVA)'] = round(va / 1000.0, 4)
                            elif v is not None and c is not None:
                                metrics['Apparent Power (kVA)'] = round((v * c) / 1000.0, 4)

                            var = _get_metric(e_obj, 'ReactivePower', idx)
                            if var is not None:
                                metrics['Reactive Power (kVAR)'] = round(var / 1000.0, 4)

                            return metrics

                        # Parsing multi-channel atau single-channel (CT -> 'CT', Plug -> 'PG', lainnya -> 'L1')
                        if len(energy_keys) > 1:
                            for i, ekey in enumerate(sorted(energy_keys)):
                                ph = f"CT{i+1}" if device_type == 'CT' else (f"PG{i+1}" if device_type == 'Plug' else f"L{i+1}")
                                if ph not in _mqtt_live_data[device_id]:
                                    _mqtt_live_data[device_id][ph] = {}
                                _mqtt_live_data[device_id][ph].update(_build_metrics(data[ekey]))
                        elif len(energy_keys) == 1:
                            e_obj = data[energy_keys[0]]
                            array_lens = [len(e_obj[k]) for k in ('Voltage', 'Current', 'Power') if isinstance(e_obj.get(k), list)]
                            if array_lens:
                                num_ch = max(array_lens)
                                for ch in range(num_ch):
                                    ph = f"CT{ch+1}" if device_type == 'CT' else (f"PG{ch+1}" if device_type == 'Plug' else f"L{ch+1}")
                                    if ph not in _mqtt_live_data[device_id]:
                                        _mqtt_live_data[device_id][ph] = {}
                                    _mqtt_live_data[device_id][ph].update(_build_metrics(e_obj, ch))
                            else:
                                ph = "CT" if device_type == 'CT' else ("PG" if device_type == 'Plug' else "L1")
                                if ph not in _mqtt_live_data[device_id]:
                                    _mqtt_live_data[device_id][ph] = {}
                                _mqtt_live_data[device_id][ph].update(_build_metrics(e_obj))

                        # Simpan info ekstra Tasmota ke metadata status
                        if 'status' not in _mqtt_live_data[device_id]:
                            _mqtt_live_data[device_id]['status'] = {}
                        main_e = data.get(energy_keys[0]) if energy_keys else {}
                        if isinstance(main_e, dict):
                            if 'Today' in main_e:
                                _mqtt_live_data[device_id]['status']['today_kwh'] = main_e['Today']
                            if 'Yesterday' in main_e:
                                _mqtt_live_data[device_id]['status']['yesterday_kwh'] = main_e['Yesterday']
                    except Exception:
                        pass

                elif sub == 'STATE':
                    try:
                        s_data = json.loads(payload)
                        if 'status' not in _mqtt_live_data[device_id]:
                            _mqtt_live_data[device_id]['status'] = {}
                        if 'POWER' in s_data:
                            _mqtt_live_data[device_id]['status']['relay'] = s_data['POWER']
                        if 'Uptime' in s_data:
                            _update_device_uptime(device_id, s_data['Uptime'])
                        if 'Wifi' in s_data and isinstance(s_data['Wifi'], dict):
                            _mqtt_live_data[device_id]['status']['wifi'] = s_data['Wifi']
                        if 'Heap' in s_data:
                            _mqtt_live_data[device_id]['status']['heap'] = s_data['Heap']
                        if 'Time' in s_data and not _mqtt_live_data[device_id].get('Timestamp'):
                            try:
                                dt = datetime.fromisoformat(s_data['Time'])
                                _mqtt_live_data[device_id]['Timestamp'] = dt.strftime('%H:%M:%S %d/%m/%Y')
                            except Exception:
                                _mqtt_live_data[device_id]['Timestamp'] = str(s_data['Time'])
                    except Exception:
                        pass

                elif sub in ('POWER', 'RESULT'):
                    if 'status' not in _mqtt_live_data[device_id]:
                        _mqtt_live_data[device_id]['status'] = {}
                    try:
                        res_data = json.loads(payload)
                        if isinstance(res_data, dict) and 'POWER' in res_data:
                            _mqtt_live_data[device_id]['status']['relay'] = res_data['POWER']
                    except Exception:
                        if payload.strip().upper() in ('ON', 'OFF'):
                            _mqtt_live_data[device_id]['status']['relay'] = payload.strip().upper()

                elif sub == 'LWT':
                    if payload.strip().lower() == 'offline':
                        _mqtt_last_seen[device_id] = 0
                    elif payload.strip().lower() == 'online':
                        _mqtt_last_seen[device_id] = time.time()

                elif sub == 'EVENT':
                    # Notifikasi Event Lifecycle (InternetRestored, SyncComplete, dll) dari LittleFS buffer
                    try:
                        ev_data = json.loads(payload)
                        event_name = ev_data.get('Event', '')
                        if event_name == 'InternetRestored':
                            buffered_cnt = int(ev_data.get('BufferedRecords', 0))
                            downtime_sec = int(ev_data.get('DowntimeSec', 0))
                            _buffer_sync_status[device_id] = {
                                'status': 'SYNCING',
                                'total': buffered_cnt,
                                'replayed': 0,
                                'downtime_sec': downtime_sec,
                                'start_time': time.time(),
                                'last_updated': time.time()
                            }
                            if 'status' not in _mqtt_live_data[device_id]:
                                _mqtt_live_data[device_id]['status'] = {}
                            _mqtt_live_data[device_id]['status']['buffer_sync'] = _buffer_sync_status[device_id]
                            print(f"[Buffer EVENT] {device_id} Internet pulih! Downtime: {downtime_sec}s, buffered records: {buffered_cnt}")
                        elif event_name == 'SyncComplete':
                            replayed_cnt = int(ev_data.get('ReplayedRecords', 0))
                            if device_id in _buffer_sync_status:
                                _buffer_sync_status[device_id]['status'] = 'COMPLETED'
                                _buffer_sync_status[device_id]['replayed'] = replayed_cnt
                                _buffer_sync_status[device_id]['last_updated'] = time.time()
                            if 'status' not in _mqtt_live_data[device_id]:
                                _mqtt_live_data[device_id]['status'] = {}
                            _mqtt_live_data[device_id]['status']['buffer_sync'] = {
                                'status': 'COMPLETED',
                                'total': replayed_cnt,
                                'replayed': replayed_cnt,
                                'last_updated': time.time()
                            }
                            print(f"[Buffer EVENT] {device_id} Replay buffer LittleFS selesai! Total: {replayed_cnt} records.")
                    except Exception as ev_err:
                        print(f"[Buffer EVENT Error] {device_id}: {ev_err}")

                elif sub == 'HISTORY':
                    # Offline Data Buffer Replay Engine: memproses streaming history LittleFS secara terisolasi
                    try:
                        h_data = json.loads(payload)
                        rec_time_str = h_data.get('Time')
                        seq = int(h_data.get('Seq', 0))
                        e_obj = h_data.get('ENERGY') or {}

                        # Perbarui progress di tracker sync
                        if device_id in _buffer_sync_status:
                            _buffer_sync_status[device_id]['replayed'] = seq
                            _buffer_sync_status[device_id]['last_updated'] = time.time()
                            if 'status' in _mqtt_live_data.get(device_id, {}):
                                _mqtt_live_data[device_id]['status']['buffer_sync'] = _buffer_sync_status[device_id]

                        if rec_time_str and e_obj:
                            try:
                                dt_rec = datetime.fromisoformat(rec_time_str)
                            except Exception:
                                dt_rec = datetime.strptime(rec_time_str, '%Y-%m-%d %H:%M:%S')
                            if dt_rec.tzinfo is None:
                                dt_rec = dt_rec.replace(tzinfo=_WIB)

                            rec_epoch_ms = int(dt_rec.timestamp() * 1000)
                            rec_ts_str = dt_rec.strftime('%H:%M:%S %d/%m/%Y')

                            # Tentukan slot 15-menit
                            slot_m = (dt_rec.minute // 15) * 15
                            slot_dt = datetime(dt_rec.year, dt_rec.month, dt_rec.day, dt_rec.hour, slot_m, 0, tzinfo=_WIB)
                            slot_ts = f'{slot_dt.strftime("%H")}:{str(slot_m).zfill(2)} {slot_dt.strftime("%d/%m/%Y")}'
                            slot_epoch_ms = int(slot_dt.timestamp() * 1000)

                            ph = 'CT' if (device_type == 'CT' or device_id.upper().startswith('CT-')) else ('PG' if (device_type == 'Plug' or device_id.upper().startswith('PLUG-')) else 'L1')

                            v = float(e_obj.get('Voltage') or 0.0)
                            c = float(e_obj.get('Current') or 0.0)
                            w = float(e_obj.get('Power') or 0.0)
                            tot = float(e_obj.get('Total') or 0.0)
                            pf = float(e_obj.get('Factor', e_obj.get('PowerFactor', 0.0)) or 0.0)
                            hz = float(e_obj.get('Frequency') or 50.0)

                            _process_buffered_record(device_id, ph, rec_ts_str, rec_epoch_ms, slot_ts, slot_epoch_ms, seq, v, c, w, tot, pf, hz)
                    except Exception as h_err:
                        print(f"[Buffer HISTORY Error] {device_id}: {h_err}")
                    return  # TERISOLASI: Jangan biarkan menimpa _mqtt_live_data agar visual live tetap akurat!

            # Case 3A: AlfaEnergy v1.3.0 1P -> AlfaEnergy/<DEVICE_ID>/1P/<LOAD_TYPE>/<UNIT_NUMBER> (sub_parts: ['1P', ...])
            elif len(sub_parts) == 3 and sub_parts[0] == '1P':
                phase = f"{sub_parts[1]}_{sub_parts[2]}"
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

            # Case 3B: AlfaEnergy Mode 3P / 1P -> AlfaEnergy/EM-0001/3P/R, AlfaEnergy/EM-0001/3P/S, AlfaEnergy/EM-0001/1P/Lampu
            elif len(sub_parts) == 2 and sub_parts[0] in ('3P', '1P'):
                phase = sub_parts[1]
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

            # Case 4: Format baru legacy -> energymeter/alat1/L1
            elif len(sub_parts) == 1 and re.match(r'^L\d+$', sub_parts[0]):
                phase = sub_parts[0]
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

            # Case 5: Format lama -> energymeter/alat1/L1/Voltage_V → nilai float tunggal
            elif len(sub_parts) == 2 and re.match(r'^L\d+$', sub_parts[0]):
                phase, metric = sub_parts[0], sub_parts[1]
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
            c.subscribe("SpartaEnergy/#")
            c.subscribe("AlfaEnergy/#")
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

_MIN_TOKEN_LENGTH = 16

def _portal_token() -> str:
    token = os.environ.get('PORTAL_CONTROL_TOKEN', '').strip()
    return token if len(token) >= _MIN_TOKEN_LENGTH else ''

def _dashboard_ui_enabled() -> bool:
    return os.environ.get('DASHBOARD_UI_ENABLED', '').strip() == '1'

# Runs before every route, static files included (app.js carries broker credentials).
@app.before_request
def _guard_request():
    if request.endpoint == 'health':
        return None
    # The endpoint decides, not the path: '/api/../app.js' is served by the static route.
    if request.endpoint in ('static', 'index') or not request.path.startswith('/api/'):
        if _dashboard_ui_enabled():
            return None
        return 'Dashboard ini sudah pindah ke portal Smart Energy Monitoring.', 410

    if os.environ.get('PORTAL_CONTROL_TOKEN', '').strip() and not _portal_token():
        print(f"ERROR PORTAL_CONTROL_TOKEN is shorter than {_MIN_TOKEN_LENGTH} characters, API closed")
        return jsonify({'ok': False, 'error': 'PORTAL_CONTROL_TOKEN terlalu pendek'}), 503

    expected = _portal_token()
    is_write = request.method not in ('GET', 'HEAD', 'OPTIONS')
    if (request.path == '/api/spartaflash/register' or request.path.startswith('/api/spartaflash/download/')) and _dashboard_ui_enabled():
        return None
    if not expected:
        # Writes fail closed. Reads stay open so a missing env does not blind the portal.
        if is_write:
            return jsonify({'ok': False, 'error': 'PORTAL_CONTROL_TOKEN belum diisi'}), 503
        return None
    if not is_write and _dashboard_ui_enabled():
        return None
    received = request.headers.get('X-Portal-Token', '')
    if not hmac.compare_digest(received.encode('utf-8'), expected.encode('utf-8')):
        return jsonify({'ok': False, 'error': 'Token tidak valid'}), 401
    return None
_mqtt_client = _start_mqtt() if (not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true") else None

_PHASE_RE = re.compile(r'^L\d+$')
_PHASE_ORDER = {'R': 1, 'S': 2, 'T': 3}
_NON_PHASE_KEYS = {'Timestamp', 'timestamp', 'meta', 'offline', 'status', 'cmd', 'realtime', 'RealTime'}

def _get_default_sensor_display_name(phase: str) -> str:
    if not phase:
        return ''
    up = phase.upper()
    if up in ('R', 'S', 'T'):
        return f'Phase {up}'
    if up == 'CT':
        return 'CT'
    if up.startswith('CT') and up[2:].isdigit():
        return f'CT-{int(up[2:])}'
    if up == 'PG':
        return 'Plug'
    if up.startswith('PG') and up[2:].isdigit():
        return f'Plug-{int(up[2:])}'
    if _PHASE_RE.match(phase):
        return f'Sensor {phase[1:]}'
    m = re.match(r'^([A-Za-z]+)[_-]?(\d+)$', phase)
    if m:
        t, n = m.group(1).upper(), int(m.group(2))
        prefix_map = {
            'AC': 'AC', 'COOLER': 'Cooler', 'LAMP': 'Lamp', 'REFRIGERATOR': 'Refrigerator',
            'FREEZER': 'Freezer', 'PUMP': 'Pump', 'MAIN': 'Main Panel',
            'DISTRIBUTION': 'Distribution', 'PRODUCTION': 'Production', 'OTHER': 'Other'
        }
        clean_type = prefix_map.get(t, t.capitalize())
        return f'{clean_type}-{n}'
    return phase

def _phase_sort_key(p: str):
    if not p:
        return (99, '', 0, '')
    up = p.upper()
    if up in _PHASE_ORDER:
        return (0, '', _PHASE_ORDER[up], p)
    if _PHASE_RE.match(p):
        try:
            return (1, '', int(p[1:]), p)
        except ValueError:
            return (1, '', 999, p)
    m = re.match(r'^([A-Za-z]+)[_-]?(\d+)$', up)
    if m:
        return (2, m.group(1), int(m.group(2)), p)
    return (3, p, 0, p)

def _is_valid_phase_key(k: str) -> bool:
    if not k or not isinstance(k, str):
        return False
    if k in _NON_PHASE_KEYS:
        return False
    return bool(re.match(r'^[A-Za-z0-9_\-\s]{1,50}$', k.strip()))

def _detect_phases(device_data: dict) -> list[str]:
    keys: set[str] = set()
    for src in [device_data.get('RealTime') or {}, (device_data.get('meta') or {}).get('sensors') or {}]:
        if isinstance(src, dict):
            keys.update(k for k in src if _is_valid_phase_key(k))
    return sorted(keys, key=_phase_sort_key)

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
            pool = psycopg2.pool.ThreadedConnectionPool(
                2, 50, db_url,
                connect_timeout=8,
                # A dead connection is noticed within about a minute instead of hanging.
                keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3, tcp_user_timeout=20000,
                options='-c statement_timeout=15000'
            )
            db_pool = pool  # Langsung sediakan pool agar request API tidak terblokir
            print("PostgreSQL connection pool initialized successfully (min=2, max=50, timeout=8s).")

            # Inisialisasi DB Sparta juga jika tersedia
            init_sparta_db()

            # Jalankan schema & index check
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
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS capture_states (
                            device_id VARCHAR(50) PRIMARY KEY,
                            state JSONB NOT NULL,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS buffered_telemetry (
                            id SERIAL PRIMARY KEY,
                            device_id VARCHAR(50) NOT NULL,
                            phase VARCHAR(50) NOT NULL,
                            timestamp VARCHAR(50) NOT NULL,
                            epoch BIGINT NOT NULL,
                            seq INT DEFAULT 0,
                            voltage FLOAT DEFAULT 0.0,
                            current FLOAT DEFAULT 0.0,
                            power FLOAT DEFAULT 0.0,
                            frequency FLOAT DEFAULT 50.0,
                            energy FLOAT DEFAULT 0.0,
                            power_factor FLOAT DEFAULT 0.0,
                            replayed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS spartaflash_downloads (
                            id SERIAL PRIMARY KEY,
                            download_token VARCHAR(64) UNIQUE NOT NULL,
                            downloader_name VARCHAR(120) NOT NULL,
                            branch_division VARCHAR(120) NOT NULL,
                            role_title VARCHAR(80) DEFAULT '',
                            contact VARCHAR(120) NOT NULL,
                            notes TEXT DEFAULT '',
                            version VARCHAR(32) NOT NULL,
                            filename VARCHAR(255) NOT NULL,
                            source_mode VARCHAR(32) NOT NULL,
                            ip_address VARCHAR(64) DEFAULT '',
                            user_agent TEXT DEFAULT '',
                            status VARCHAR(32) DEFAULT 'registered',
                            download_hits INT DEFAULT 0,
                            registered_at VARCHAR(50) NOT NULL,
                            last_downloaded_at VARCHAR(50) DEFAULT NULL,
                            epoch BIGINT NOT NULL
                        );
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS app_settings (
                            key VARCHAR(80) PRIMARY KEY,
                            value JSONB NOT NULL,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    # Alter table add column if not exists
                    columns_to_add = [
                        ('history', 'phase_name', 'VARCHAR(100) DEFAULT NULL'),
                        ('history', 'device_name', 'TEXT DEFAULT NULL'),
                        ('history', 'store_id', 'VARCHAR(50) DEFAULT NULL'),
                        ('history', 'latitude', 'FLOAT DEFAULT NULL'),
                        ('history', 'longitude', 'FLOAT DEFAULT NULL'),
                        ('devices', 'store_id', 'VARCHAR(50) DEFAULT NULL'),
                        ('devices', 'latitude', 'FLOAT DEFAULT NULL'),
                        ('devices', 'longitude', 'FLOAT DEFAULT NULL'),
                    ]
                    for tbl, col, col_def in columns_to_add:
                        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {col} {col_def};")

                    try:
                        cur.execute("ALTER TABLE telemetry ALTER COLUMN phase TYPE VARCHAR(50);")
                        cur.execute("ALTER TABLE history ALTER COLUMN phase TYPE VARCHAR(50);")
                    except Exception as pe:
                        print(f"[init_db] Note altering phase columns: {pe}")

                    def create_index_safe(idx_name, create_sql):
                        try:
                            cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = %s", (idx_name,))
                            if not cur.fetchone():
                                cur.execute(create_sql)
                        except Exception as ie:
                            print(f"[init_db] Warning creating index {idx_name}: {ie}")

                    create_index_safe('idx_telemetry_device_epoch', "CREATE INDEX idx_telemetry_device_epoch ON telemetry(device_id, epoch);")
                    create_index_safe('idx_telemetry_device_phase', "CREATE INDEX idx_telemetry_device_phase ON telemetry(device_id, phase);")
                    create_index_safe('idx_history_session', "CREATE INDEX idx_history_session ON history(session_id);")
                    create_index_safe('idx_history_device_session', "CREATE INDEX idx_history_device_session ON history(device_id, session_id);")
                    create_index_safe('idx_history_session_epoch', "CREATE INDEX idx_history_session_epoch ON history(session_id, epoch);")
                    create_index_safe('idx_history_dev_sess_epoch', "CREATE INDEX idx_history_dev_sess_epoch ON history(device_id, session_id, epoch);")
                    create_index_safe('idx_buffered_dev_ph_epoch_uniq', "CREATE UNIQUE INDEX idx_buffered_dev_ph_epoch_uniq ON buffered_telemetry(device_id, phase, epoch);")
                    create_index_safe('idx_buffered_dev_epoch', "CREATE INDEX idx_buffered_dev_epoch ON buffered_telemetry(device_id, epoch);")
                    create_index_safe('idx_spartaflash_dl_epoch', "CREATE INDEX idx_spartaflash_dl_epoch ON spartaflash_downloads(epoch DESC);")

                    try:
                        cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = 'idx_telemetry_dev_ph_ts_uniq'")
                        if not cur.fetchone():
                            cur.execute("""
                                DELETE FROM telemetry a USING telemetry b
                                WHERE a.id < b.id
                                  AND a.device_id = b.device_id
                                  AND a.phase = b.phase
                                  AND a.timestamp = b.timestamp;
                            """)
                            cur.execute("CREATE UNIQUE INDEX idx_telemetry_dev_ph_ts_uniq ON telemetry(device_id, phase, timestamp);")
                    except Exception as ue:
                        print(f"[init_db] Note creating unique index for telemetry: {ue}")

                    conn.commit()
                    print("Database tables & indexes initialized successfully.")
            except Exception as e:
                conn.rollback()
                print(f"Error initializing database tables: {e}")
            finally:
                pool.putconn(conn)
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
        phases = sorted([k for k in raw if _is_valid_phase_key(k) and isinstance(raw.get(k), dict)], key=_phase_sort_key)
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
def validate_phase_key(phase: str) -> bool: return _is_valid_phase_key(phase)
_capture_lock = threading.RLock()
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

# Serializes snapshot, file and database, so an older snapshot cannot land after a newer one.
# Order: _state_io_lock, then _capture_lock. Never take it while holding _capture_lock.
_state_io_lock = threading.RLock()
# device -> number of its newest request. A finished write only clears the request it carried.
_pending_state_sync: dict[str, int] = {}
_state_request_counter = 0
_touched_devices: set[str] = set()     # started or stopped since boot
_state_load_complete = False
_STATE_LOCK_WAIT_S = 10

def _mark_state_pending(device_id: str) -> None:
    global _state_request_counter
    with _capture_lock:
        _state_request_counter += 1
        _pending_state_sync[device_id] = _state_request_counter

def _take_state_work() -> tuple[dict[str, dict], dict[str, int]]:
    """Snapshot and the requests it covers, taken together."""
    with _capture_lock:
        return _snapshot_active_states(), dict(_pending_state_sync)

def _snapshot_active_states() -> dict[str, dict]:
    with _capture_lock:
        return {
            did: {
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
                'time_offset_ms': state.get('time_offset_ms', 0),
                'store_id': state.get('store_id'),
                'latitude': state.get('latitude'),
                'longitude': state.get('longitude'),
            }
            for did, state in _capture_states.items() if state.get('active')
        }

def _write_state_file(states: dict[str, dict]) -> None:
    try:
        tmp = STATE_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(states, f)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        print(f"Error writing {STATE_FILE}: {e}")

def _sync_pending_states(states: dict[str, dict], requests: dict[str, int]) -> None:
    """Caller holds _state_io_lock. Touches only the rows of pending devices."""
    for did in sorted(requests):
        try:
            with get_db_cursor() as cur:
                if did in states:
                    payload = json.dumps(states[did])
                    cur.execute(
                        "INSERT INTO capture_states (device_id, state) VALUES (%s, %s) ON CONFLICT (device_id) DO UPDATE SET state = %s, updated_at = CURRENT_TIMESTAMP;",
                        (did, payload, payload)
                    )
                else:
                    cur.execute("DELETE FROM capture_states WHERE device_id = %s", (did,))
            with _capture_lock:
                if _pending_state_sync.get(did) == requests[did]:
                    del _pending_state_sync[did]
        except Exception as e:
            print(f"Capture state of {did} not stored, will retry: {e}")
            # Only an unreachable database ends the round; a bad row must not hold back the others.
            if not db_pool or isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError)):
                return

def _save_capture_states(device_id: str) -> bool:
    """Persist one device. False when the database still holds the old state."""
    _mark_state_pending(device_id)
    # A stuck connection must not tie up every request thread; the retry worker finishes the save.
    if not _state_io_lock.acquire(timeout=_STATE_LOCK_WAIT_S):
        print(f"Capture state of {device_id} not stored, another save is stuck")
        return False
    try:
        states, requests = _take_state_work()
        _write_state_file(states)
        _sync_pending_states(states, requests)
        with _capture_lock:
            return device_id not in _pending_state_sync
    finally:
        _state_io_lock.release()

def _retry_state_work() -> None:
    if not _state_load_complete:
        _load_capture_states()
    with _state_io_lock:
        states, requests = _take_state_work()
        if requests:
            _write_state_file(states)
            _sync_pending_states(states, requests)

def _state_retry_worker() -> None:
    while True:
        time.sleep(10)
        try:
            _retry_state_work()
        except Exception as e:
            print(f"Error in _state_retry_worker: {e}")

def _busy_response(message: str = 'Sesi masih merekam. Hentikan rekaman dulu.'):
    return jsonify({'ok': False, 'error': message}), 409

def _not_loaded_response():
    return jsonify({'ok': False, 'error': 'Status rekaman belum termuat dari database. Coba lagi sebentar.'}), 503

def _json_body() -> dict:
    body = request.get_json(silent=True)
    return body if isinstance(body, dict) else {}

def _is_phase_recording(device_id: str, phase: str) -> bool:
    with _capture_lock:
        s = _capture_states.get(device_id) or {}
        if not (s.get('active') or s.get('_finalizing')):
            return False
        phases = s.get('enabled_phases')
        return phases is None or phase.lower() in {p.lower() for p in phases}

def _is_session_busy(session_id: str) -> bool:
    """True while a session records or its worker is still writing its last rows."""
    with _capture_lock:
        return any(
            (s.get('active') and s.get('session_id') == session_id)
            or (s.get('_finalizing') and s.get('_finalizing_session_id') == session_id)
            for s in _capture_states.values()
        )

def _is_device_busy(device_id: str) -> bool:
    with _capture_lock:
        s = _capture_states.get(device_id) or {}
        return bool(s.get('active') or s.get('_finalizing'))

def _read_persisted_states() -> dict[str, dict]:
    saved = {}
    with get_db_cursor() as cur:
        cur.execute("SELECT device_id, state FROM capture_states")
        for r_did, r_state in cur.fetchall():
            st = r_state if isinstance(r_state, dict) else json.loads(r_state)
            if st and st.get('active'):
                saved[r_did] = st
    return saved

def _load_capture_states():
    """Resume recordings. Runs again until the database answered; never replaces newer states."""
    global _state_load_complete
    saved, from_db = {}, False
    if db_pool or os.environ.get('DATABASE_URL'):
        try:
            saved = _read_persisted_states()
            from_db = True
        except Exception as dbe:
            print(f"ERROR capture states not loaded, recordings may be paused until the retry succeeds: {dbe}")
    else:
        from_db = True

    if not saved and os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                saved = json.load(f)
        except Exception as fe:
            print(f"Error reading {STATE_FILE}: {fe}")

    counts = {}
    for did, state in saved.items():
        if from_db and state.get('session_id') and did not in _capture_states:
            try:
                with get_db_cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM history WHERE session_id = %s", (state['session_id'],))
                    row = cur.fetchone()
                    if row and row[0]:
                        counts[did] = row[0]
            except Exception:
                pass

    to_start = []
    with _capture_lock:
        for did, state in saved.items():
            known = _capture_states.get(did) or {}
            if not state.get('active') or did in _touched_devices or known.get('active') or known.get('_finalizing'):
                continue
            cstate = _get_device_capture_state(did)
            cstate.update(state)
            if did in counts:
                cstate['count'] = counts[did]
            to_start.append(did)
    for did in to_start:
        _start_thread(did)
    if to_start:
        print(f"[Auto-Resume] Resumed active capture threads for devices: {to_start}")
    _state_load_complete = from_db

def _data_hash(raw): return hashlib.md5(json.dumps(raw, sort_keys=True).encode()).hexdigest() if raw else None
_sessions_cache: dict[str, dict] = {}
_sessions_cache_lock = threading.RLock()

def _invalidate_sessions_cache():
    with _sessions_cache_lock:
        _sessions_cache.clear()

_hourly_stop = threading.Event()
_last_write_per_device: dict[str, str] = {}  # guard duplikat per device
_live_buffer_last_push: dict[str, float] = {}  # waktu terakhir data dipush ke live buffer per device
HEARTBEAT_INTERVAL = 30  # detik; push heartbeat ke live-buffer agar frontend tidak stale

def _get_telemetry_phases(device_id: str, raw: dict) -> list[str]:
    """Dapatkan daftar phase yang harus di-snapshot untuk device ini.
    Prioritas: sensor terdaftar di DB → MQTT live data → fallback R, S, T."""
    phases: set[str] = set()

    # 1. Phase dari MQTT live data (paling aktual)
    for k in raw:
        if _is_valid_phase_key(k) and isinstance(raw.get(k), dict):
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
                if _is_valid_phase_key(ph):
                    phases.add(ph)
    except Exception:
        pass

    # 3. Fallback minimal jika tidak ada data sama sekali
    if not phases:
        phases = {'R', 'S', 'T'}

    return sorted(phases, key=_phase_sort_key)

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
    last_db_check = 0
    while True:
        try:
            now_ms = int(time.time() * 1000)
            now = time.time()

            # --- Langkah 1: Simpan snapshot kontinu ke RAM buffer setiap 1 detik ---
            all_known_dids = set(_mqtt_live_data.keys())
            for did in all_known_dids:
                raw = _mqtt_live_data.get(did)
                last_seen = _mqtt_last_seen.get(did, 0)
                is_offline = (now - last_seen > 300) or not raw

                if did not in _device_live_buffer:
                    _device_live_buffer[did] = deque(maxlen=1200)

                if is_offline:
                    if not _device_is_offline.get(did, False):
                        _device_is_offline[did] = True
                        _device_live_buffer[did].append({'timestamp': now_ms, 'data': {"offline": True}})
                else:
                    _device_is_offline[did] = False
                    raw_copy = json.loads(json.dumps(raw)) if raw else {}
                    _device_live_buffer[did].append({'timestamp': now_ms, 'data': raw_copy})
                    _live_buffer_last_push[did] = now

            # --- Langkah 2: Database online/offline & ping sync (throttled setiap 5 detik) ---
            if db_pool and (now - last_db_check >= 5):
                last_db_check = now
                with get_db_cursor() as cur:
                    cur.execute("SELECT id, online, last_seen FROM devices")
                    db_devices = cur.fetchall()

                devices_meta = {row[0]: {'online': row[1], 'last_seen': row[2]} for row in db_devices}
                dids = set(devices_meta.keys()) | all_known_dids

                offline_updates = []    # device_id yang perlu di-set offline
                online_updates  = []    # (device_id, ts_str) yang perlu di-set online

                for did in dids:
                    raw = _mqtt_live_data.get(did)
                    last_seen = _mqtt_last_seen.get(did, 0)
                    is_offline = (now - last_seen > 300) or not raw
                    meta = devices_meta.get(did)

                    if is_offline:
                        if meta and meta['online'] != False:
                            offline_updates.append(did)
                    else:
                        status_changed = not meta or meta['online'] != True
                        time_for_ping = (now - _db_last_ping.get(did, 0) > 30)
                        if status_changed or time_for_ping:
                            _db_last_ping[did] = now
                            ts_str = (raw or {}).get('Timestamp') or _ts_now()
                            online_updates.append((did, ts_str))

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
        time.sleep(1)

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
                FROM (
                    SELECT phase, epoch, voltage, current, power, frequency, energy, power_factor
                    FROM telemetry
                    WHERE device_id = %s AND epoch >= %s
                    ORDER BY epoch DESC
                    LIMIT 2400
                ) sub
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

def _do_capture_io(device_id, session_id, sched_ts, interval, last_hash, last_change, enabled_phases, time_offset_ms, meta):
    try:
        raw = _mqtt_live_data.get(device_id)
        h   = _data_hash(raw); now = time.time()
        if h != last_hash[0]: last_hash[0] = h; last_change[0] = now
        stale   = (now - _mqtt_last_seen.get(device_id, 0))
        offline = raw is None or stale > 300
        sched_shifted = sched_ts + (time_offset_ms / 1000.0)
        ts  = datetime.fromtimestamp(sched_shifted, tz=_WIB).strftime('%H:%M:%S %d/%m/%Y')
        epoch_val = int(sched_shifted * 1000)
        
        sname = meta.get('session_name') or 'Rekaman'
        sensor_names = meta.get('sensor_names') or {}
        dev_name = meta.get('device_name') or device_id
        store_id = meta.get('store_id')

        if enabled_phases:
            phases = enabled_phases
        else:
            phases = sorted(
                [k for k in (raw or {}) if isinstance((raw or {}).get(k), dict) and k not in ('status', 'Timestamp', '_meta', 'cmd', 'realtime', 'RealTime')],
                key=lambda x: int(x[1:]) if _PHASE_RE.match(x) else 99
            )
        
        # Fallback kuat: jika MQTT live data saat restart belum ready, gunakan sensor terdaftar di DB
        if not phases:
            phases = _get_telemetry_phases(device_id, raw or {})
        if not phases:
            phases = ['R', 'S', 'T']
        
        insert_rows = []
        for ph in phases:
            pd = {} if offline else ((raw or {}).get(ph) if isinstance((raw or {}).get(ph), dict) else {})
            phase_offline = offline or not bool(pd)
            def f(k):
                try: return float((pd or {}).get(k) or 0)
                except: return 0.0
            insert_rows.append((
                device_id, ph, ts, epoch_val,
                f('Voltage (V)'), f('Current (A)'), f('Power (W)'),
                f('Frequency (Hz)'), f('Active Energy (kWh)'), f('Power Factor'),
                phase_offline, session_id, sname, sensor_names.get(ph, ph), dev_name, store_id
            ))

        if insert_rows:
            from psycopg2.extras import execute_values
            with get_db_cursor() as cur:
                execute_values(cur, """
                    INSERT INTO history (
                        device_id, phase, timestamp, epoch,
                        voltage, current, power, frequency, energy, power_factor,
                        offline, session_id, session_name, phase_name, device_name, store_id
                    ) VALUES %s
                """, insert_rows)
                
        with _capture_lock:
            cstate = _get_device_capture_state(device_id)
            if cstate.get('session_id') == session_id:
                cstate['count'] += len(phases)
    except Exception as e:
        print(f"Error in _do_capture_io ({device_id}): {e}")
        try:
            with open("capture_error.log", "a") as f:
                f.write(f"[{_ts_now()}] Error in _do_capture_io ({device_id}): {e}\n")
        except:
            pass

def _capture_worker(device_id: str, stop: threading.Event, wake: threading.Event) -> None:
    last_hash: list = [None]; last_change: list = [None]
    nxt = time.time() + 3
    while not stop.is_set():
        try:
            if stop.wait(timeout=min(max(0., nxt - time.time()), 0.2)): break
            if wake.is_set():
                wake.clear()
                with _capture_lock: nxt = time.time()
                continue
            if time.time() < nxt: continue
            with _capture_lock:
                cstate = _get_device_capture_state(device_id)
                if not cstate.get('active'): break
                sid = cstate.get('session_id')
                did = cstate.get('device_id', device_id)
                iv  = float(cstate.get('interval') or 15)
                ep  = cstate.get('enabled_phases')
                to_ms = cstate.get('time_offset_ms', 0)
                meta = {k: cstate.get(k) for k in ('session_name', 'sensor_names', 'device_name', 'store_id')}
            if not sid:
                break
            sched = nxt
            # Jika nxt tertinggal jauh lebih dari 10 detik (misal proses restart atau sleep), sinkronkan ke waktu sekarang
            if time.time() - nxt > 10:
                nxt = time.time()
            nxt += iv
            _do_capture_io(did, sid, sched, iv, last_hash, last_change, ep, to_ms, meta)
        except Exception as we:
            print(f"[Capture Worker Error] Device {device_id}: {we}")
            try:
                with open("capture_error.log", "a") as f:
                    f.write(f"[{_ts_now()}] [Capture Worker Exception] Device {device_id}: {we}\n")
            except:
                pass
            time.sleep(1)

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
        while th and th.is_alive():
            th.join(timeout=30)
            if th.is_alive():
                print(f"[Capture] Worker of {did} is still writing session {sid}")
    except Exception as e:
        print(f"Error in _finalize_bg ({did}): {e}")
    finally:
        with _capture_lock:
            if did in _capture_states:
                _capture_states[did]['_finalizing'] = False
                _capture_states[did]['_finalizing_session_id'] = None

def _finalize_in_background(*args) -> None:
    threading.Thread(target=_finalize_bg, args=args, daemon=True).start()

def _stop_device_capture(device_id: str) -> tuple[bool, bool]:
    """(stopped, persisted)"""
    with _capture_lock:
        cstate = _capture_states.get(device_id)
        if not cstate or not cstate.get('active'):
            return False, True
        _touched_devices.add(device_id)
        sid = cstate.get('session_id')
        cnt = cstate.get('count', 0)
        did = cstate.get('device_id', device_id)
        se  = cstate.get('_stop_event')
        th  = cstate.get('_thread')
        ep  = cstate.get('enabled_phases')
        to_ms = cstate.get('time_offset_ms', 0)
        cstate.update({
            'active': False, '_finalizing': True, '_finalizing_session_id': sid,
            'session_id': None, 'session_name': None,
            'count': 0, 'started_at': None, 'enabled_phases': None,
            '_thread': None, '_stop_event': None, '_wake_event': None,
        })
    if se: se.set()
    persisted = _save_capture_states(device_id)
    _finalize_in_background(sid, did, cnt, se, th, ep, to_ms)
    return True, persisted

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
@app.route('/spartaflash')
def index():
    return render_template('index.html', v=_APP_VERSION)

@app.route('/health', methods=['GET'])
def health(): return jsonify({"status": "ok", "message": "Service is alive"}), 200

@app.errorhandler(500)
def internal_error(error):
    if request.path.startswith('/api/'):
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500
    return 'Internal Server Error', 500

@app.errorhandler(404)
def not_found_error(error):
    if request.path.startswith('/api/'):
        return jsonify({'ok': False, 'error': f'Endpoint not found: {request.path}'}), 404
    return render_template('index.html', v=_APP_VERSION), 200

@app.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        if request.path.startswith('/api/'):
            return jsonify({'ok': False, 'error': e.name}), e.code
        return e
    print(f"[API Error] {request.path}: {e}")
    if request.path.startswith('/api/'):
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500
    return 'Internal Server Error', 500

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
                for ph in sorted([k for k in raw if _is_valid_phase_key(k) and isinstance(raw.get(k), dict)], key=_phase_sort_key):
                    disp_name = _get_default_sensor_display_name(ph)
                    phases.append({'phase': ph, 'name': disp_name, 'enabled': True})
            st = (raw or {}).get('status') if isinstance(raw, dict) else {}
            st = st if isinstance(st, dict) else {}
            up_raw = st.get('uptime')
            up_sec = st.get('uptime_sec') if st.get('uptime_sec') is not None else _parse_uptime_seconds(up_raw)
            if online and up_sec is None:
                _request_legacy_status_if_needed(did, min_interval=30.0)
            devices.append({
                'id': did,
                'name': did,
                'online': is_online,
                'lastSeen': (raw or {}).get('Timestamp') or '---',
                'phases': phases,
                'phaseCount': len(phases),
                'storeId': None,
                'bufferSync': _buffer_sync_status.get(did, None),
                'uptime': up_raw,
                'uptimeSec': up_sec,
                'lastRebootTs': _device_last_reboot_ts.get(did),
            })
        return jsonify(devices)

    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT id, name, online, last_seen, sensors, store_id, latitude, longitude FROM devices")
            rows = cur.fetchall()
        for row in rows:
            did, device_name, online, last_seen, sensors_json, store_id, lat, lng = row
            phases = []
            sensors_list = []
            if sensors_json:
                sensors_list = sensors_json if isinstance(sensors_json, list) else json.loads(sensors_json)

            # Auto-discover phase baru HANYA jika device baru dan belum memiliki sensor sama sekali di database
            if len(sensors_list) == 0:
                raw = _mqtt_live_data.get(did) or {}
                live_detected = [k for k in raw if _is_valid_phase_key(k) and isinstance(raw.get(k), dict)]
                existing_phases = {s.get('phase', '').lower() for s in sensors_list if isinstance(s, dict)}
                added = False
                for ph in live_detected:
                    if ph.lower() not in existing_phases:
                        disp_name = _get_default_sensor_display_name(ph)
                        sensors_list.append({
                            'phase': ph,
                            'name': disp_name,
                            'enabled': True,
                            'properties': []
                        })
                        existing_phases.add(ph.lower())
                        added = True

                if added:
                    try:
                        with get_db_cursor() as cur2:
                            cur2.execute("UPDATE devices SET sensors = %s WHERE id = %s AND (sensors IS NULL OR jsonb_array_length(sensors) = 0)", (json.dumps(sensors_list), did))
                    except Exception as ex:
                        print(f"Error auto-updating sensors for {did}: {ex}")

            for s in sorted(sensors_list, key=lambda item: _phase_sort_key(item.get('phase', ''))):
                raw_ph = s.get('phase')
                s_name = s.get('name')
                if did.upper().startswith('CT-') and (raw_ph == 'L1' or s_name in ('Sensor 1', 'L1', 'None', None, '')):
                    raw_ph = 'CT'
                    s_name = 'CT'
                elif did.lower().startswith('plug-') and (raw_ph == 'L1' or s_name in ('Sensor 1', 'L1', 'None', None, '')):
                    raw_ph = 'PG'
                    s_name = 'Plug'
                else:
                    s_name = s_name or _get_default_sensor_display_name(raw_ph)
                phases.append({
                    'phase': raw_ph,
                    'name': s_name,
                    'properties': s.get('properties', []),
                    'enabled': s.get('enabled', True),
                    'color': s.get('color', None)
                })
            raw_live = _mqtt_live_data.get(did) or {}
            st = raw_live.get('status') if isinstance(raw_live, dict) else {}
            st = st if isinstance(st, dict) else {}
            up_raw = st.get('uptime')
            up_sec = st.get('uptime_sec') if st.get('uptime_sec') is not None else _parse_uptime_seconds(up_raw)
            up_source = 'native' if up_sec is not None else None
            if online:
                _request_legacy_status_if_needed(did, min_interval=45.0)
            devices.append({
                'id': did,
                'name': device_name,
                'online': online,
                'lastSeen': last_seen,
                'phases': phases,
                'phaseCount': len(phases),
                'storeId': store_id,
                'latitude': lat,
                'longitude': lng,
                'bufferSync': _buffer_sync_status.get(did, None),
                'uptime': up_raw,
                'uptimeSec': up_sec,
                'uptimeSource': up_source,
                'statusKeys': list(st.keys()),
                'lastRebootTs': _device_last_reboot_ts.get(did),
            })
    except Exception as e:
        print(f"Error listing devices: {e}")
    return jsonify(sorted(devices, key=lambda d: d['id']))

@app.route('/api/devices/<device_id>/init-sensors', methods=['POST'])
def init_device_sensors(device_id: str):
    try:
        raw = _mqtt_live_data.get(device_id) or {}
        detected = sorted([k for k in raw if _is_valid_phase_key(k) and isinstance(raw.get(k), dict)], key=_phase_sort_key)
        
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s FOR UPDATE", (device_id,))
            row = cur.fetchone()
            
            current_sensors = []
            if row and row[0]:
                current_sensors = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            existing_phases = {s.get('phase') for s in current_sensors}
            added = 0
            for ph in detected:
                if ph not in existing_phases:
                    disp_name = _get_default_sensor_display_name(ph)
                    current_sensors.append({
                        'phase': ph,
                        'name': disp_name,
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
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/devices/<device_id>/rename', methods=['POST'])
def rename_device(device_id: str):
    data = _json_body()
    name = str(data.get('name') or '').strip()
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
    fields = {'device_name': name, 'store_id': store_id, 'latitude': lat, 'longitude': lng}
    previous, active_sid = {}, None
    with _capture_lock:
        cstate = _capture_states.get(device_id)
        if cstate and cstate.get('active'):
            active_sid = cstate.get('session_id')
            previous = {k: cstate.get(k) for k in fields}
            cstate.update(fields)

    def restore():
        with _capture_lock:
            cstate = _capture_states.get(device_id)
            if previous and cstate and cstate.get('session_id') == active_sid:
                cstate.update(previous)

    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT 1 FROM devices WHERE id = %s", (device_id,))
            found = cur.fetchone()
            if found:
                cur.execute(
                    "UPDATE devices SET name = %s, store_id = %s, latitude = %s, longitude = %s WHERE id = %s",
                    (name, store_id, lat, lng, device_id),
                )
                if active_sid:
                    cur.execute("UPDATE history SET device_name = %s, store_id = %s, latitude = %s, longitude = %s WHERE session_id = %s", (name, store_id, lat, lng, active_sid))
    except Exception as e:
        restore()
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500
    if not found:
        restore()
        return jsonify({'ok': False, 'error': 'Perangkat tidak ditemukan'}), 404
    persisted = _save_capture_states(device_id) if active_sid else True
    return jsonify({'ok': True, 'name': name, 'store_id': store_id, 'latitude': lat, 'longitude': lng, 'timestamp': int(time.time() * 1000), 'persisted': persisted})

@app.route('/api/devices/<device_id>', methods=['DELETE'])
def delete_device_permanently(device_id: str):
    if not _state_load_complete: return _not_loaded_response()
    if _is_device_busy(device_id): return _busy_response('Perangkat masih merekam. Hentikan rekaman dulu.')
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
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/devices/<device_id>/buffer-sync')
def get_device_buffer_sync(device_id: str):
    """Mengembalikan status progres sinkronisasi replay buffer LittleFS saat ini."""
    sync_info = _buffer_sync_status.get(device_id)
    if not sync_info:
        count = 0
        if db_pool:
            try:
                with get_db_cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM buffered_telemetry WHERE device_id = %s", (device_id,))
                    count = cur.fetchone()[0]
            except Exception:
                pass
        sync_info = {'status': 'IDLE', 'total': count, 'replayed': count, 'downtime_sec': 0}

    total = sync_info.get('total', 0)
    replayed = sync_info.get('replayed', 0)
    percent = round((replayed / total * 100.0), 1) if total > 0 else 100.0
    return jsonify({
        'deviceId': device_id,
        'status': sync_info.get('status', 'IDLE'),
        'total': total,
        'replayed': replayed,
        'percent': min(100.0, percent),
        'downtimeSec': sync_info.get('downtime_sec', 0),
        'lastUpdated': sync_info.get('last_updated', 0)
    })

@app.route('/api/devices/<device_id>/buffered-telemetry')
def get_device_buffered_telemetry(device_id: str):
    """Mengembalikan rekaman granular offline buffer per menit dari tabel buffered_telemetry."""
    if not db_pool:
        return jsonify({'error': 'Database tidak aktif'}), 503
    try:
        limit = min(max(1, int(request.args.get('limit', 500))), 5000)
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT phase, timestamp, epoch, seq, voltage, current, power, frequency, energy, power_factor, replayed_at
                FROM buffered_telemetry
                WHERE device_id = %s
                ORDER BY epoch DESC
                LIMIT %s
            """, (device_id, limit))
            rows = cur.fetchall()

        records = []
        for r in rows:
            records.append({
                'phase': r[0],
                'timestamp': r[1],
                'epoch': r[2],
                'seq': r[3],
                'voltage': r[4],
                'current': r[5],
                'power': r[6],
                'frequency': r[7],
                'energy': r[8],
                'powerFactor': r[9],
                'replayedAt': r[10].isoformat() if r[10] else None
            })
        return jsonify({
            'deviceId': device_id,
            'count': len(records),
            'records': records
        })
    except Exception as e:
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500

@app.route('/api/devices/<device_id>/buffer/clear', methods=['POST'])
def clear_device_buffer(device_id: str):
    """Mengirim perintah BufferClear ke perangkat untuk mengosongkan storage LittleFS."""
    global _mqtt_client
    if not _mqtt_client:
        return jsonify({'ok': False, 'error': 'MQTT client tidak aktif'}), 503

    topics = []
    if device_id.upper().startswith('CT-'):
        raw_mac = device_id[3:]
        topics = [f"SpartaEnergy/CT/{raw_mac}/cmnd/BufferClear", f"AlfaEnergy/CT/{raw_mac}/cmnd/BufferClear"]
    elif device_id.lower().startswith('plug-'):
        raw_mac = device_id[5:]
        topics = [f"SpartaEnergy/Plug/{raw_mac}/cmnd/BufferClear", f"AlfaEnergy/Plug/{raw_mac}/cmnd/BufferClear"]
    else:
        topics = [f"SpartaEnergy/{device_id}/cmnd/BufferClear", f"AlfaEnergy/{device_id}/cmnd/BufferClear"]

    try:
        for t in topics:
            _mqtt_client.publish(t, "1")
        if device_id in _buffer_sync_status:
            _buffer_sync_status[device_id] = {'status': 'CLEARED', 'total': 0, 'replayed': 0}
        return jsonify({'ok': True, 'deviceId': device_id, 'command': 'BufferClear'})
    except Exception as e:
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>/rename', methods=['POST'])
def rename_sensor(device_id: str, phase: str):
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    name = str(_json_body().get('name') or '').strip()
    ok, err = validate_device_name(name)
    if not ok: return jsonify({'ok': False, 'error': err}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s FOR UPDATE", (device_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({'ok': False, 'error': 'Perangkat tidak ditemukan'}), 404
            sensors_list = []
            if row and row[0]:
                sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            found = False
            for s in sensors_list:
                if s.get('phase', '').lower() == phase.lower():
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
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>/color', methods=['POST'])
def set_sensor_color(device_id: str, phase: str):
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    color = str(_json_body().get('color') or '').strip()
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s FOR UPDATE", (device_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({'ok': False, 'error': 'Perangkat tidak ditemukan'}), 404
            sensors_list = []
            if row and row[0]:
                sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            found = False
            for s in sensors_list:
                if s.get('phase', '').lower() == phase.lower():
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
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>/enabled', methods=['POST'])
def set_sensor_enabled(device_id: str, phase: str):
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    body = _json_body()
    enabled = body.get('enabled')
    if not isinstance(enabled, bool):
        return jsonify({'ok': False, 'error': 'enabled harus true atau false'}), 400
    if not enabled and _is_phase_recording(device_id, phase): return _busy_response('Sensor masih direkam. Hentikan rekaman dulu.')
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s FOR UPDATE", (device_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({'ok': False, 'error': 'Perangkat tidak ditemukan'}), 404
            sensors_list = []
            if row and row[0]:
                sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            found = False
            for s in sensors_list:
                if s.get('phase', '').lower() == phase.lower():
                    s['enabled'] = enabled
                    found = True
                    break
            if not found:
                sensors_list.append({
                    'phase': phase,
                    'name': f'Phase {phase}' if phase in ('R', 'S', 'T') else phase,
                    'enabled': enabled,
                    'properties': []
                })
                
            cur.execute("UPDATE devices SET sensors = %s WHERE id = %s", (json.dumps(sensors_list), device_id))

        if not enabled and device_id in _mqtt_live_data and isinstance(_mqtt_live_data[device_id], dict):
            keys_to_pop = [k for k in _mqtt_live_data[device_id] if k.lower() == phase.lower()]
            for k in keys_to_pop:
                _mqtt_live_data[device_id].pop(k, None)

        return jsonify({'ok': True, 'enabled': enabled, 'phase': phase, 'device_id': device_id})
    except Exception as e:
        print(f"Error in set_sensor_enabled: {e}")
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>', methods=['DELETE'])
def delete_sensor(device_id: str, phase: str):
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    if _is_phase_recording(device_id, phase): return _busy_response('Sensor masih direkam. Hentikan rekaman dulu.')
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s FOR UPDATE", (device_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({'ok': False, 'error': 'Perangkat tidak ditemukan'}), 404
            sensors_list = []
            if row and row[0]:
                sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                
            filtered_sensors = [s for s in sensors_list if s.get('phase', '').lower() != phase.lower()]
            cur.execute("UPDATE devices SET sensors = %s WHERE id = %s", (json.dumps(filtered_sensors), device_id))
            
        if device_id in _mqtt_live_data and isinstance(_mqtt_live_data[device_id], dict):
            keys_to_pop = [k for k in _mqtt_live_data[device_id] if k.lower() == phase.lower()]
            for k in keys_to_pop:
                _mqtt_live_data[device_id].pop(k, None)

        if device_id in _device_live_buffer:
            for item in _device_live_buffer[device_id]:
                if isinstance(item, dict) and isinstance(item.get('data'), dict):
                    keys_to_pop = [k for k in item['data'] if k.lower() == phase.lower()]
                    for k in keys_to_pop:
                        item['data'].pop(k, None)

        return jsonify({'ok': True, 'phase': phase, 'device_id': device_id})
    except Exception as e:
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/devices/<device_id>/sensors/<phase>/init', methods=['POST'])
def init_sensor(device_id: str, phase: str):
    phase = phase.upper()
    if not validate_phase_key(phase): return jsonify({'ok': False, 'error': f'Sensor tidak valid: {phase}.'}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT sensors FROM devices WHERE id = %s FOR UPDATE", (device_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({'ok': False, 'error': 'Perangkat tidak ditemukan'}), 404
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
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

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
    try:
        body  = _json_body()
        did   = str(body.get('deviceId')    or '').strip()
        dname = str(body.get('deviceName')  or '').strip()
        sname = str(body.get('sessionName') or '').strip() or f'Rekaman {_ts_now()}'
        try:
            iv = max(15, int(body.get('interval', 15)))
        except (TypeError, ValueError):
            return jsonify({'ok': False, 'error': 'interval harus angka'}), 400
        hints = sorted([p for p in (body.get('phases') or []) if _is_valid_phase_key(p)], key=_phase_sort_key)
        if not did: return jsonify({'ok': False, 'error': 'deviceId harus diisi'}), 400
        if not hints: return jsonify({'ok': False, 'error': 'Minimal 1 sensor harus diaktifkan'}), 400

        # Baca metadata sensor & toko dari DB DI LUAR LOCK (agar lock tidak menahan koneksi)
        sensor_names = {}
        store_id = None
        registered = True  # a database outage must not stop a recording from starting
        try:
            with get_db_cursor() as cur:
                cur.execute("SELECT sensors, store_id FROM devices WHERE id = %s", (did,))
                row = cur.fetchone()
                registered = row is not None
                if row:
                    if row[0]:
                        sensors_list = row[0] if isinstance(row[0], list) else json.loads(row[0])
                        for s in sensors_list:
                            ph = s.get('phase')
                            nm = s.get('name')
                            if ph and nm:
                                sensor_names[ph] = nm
                    store_id = row[1]
        except Exception as dbe:
            print(f"[capture_start] DB lookup warning: {dbe}")
        if not registered:
            return jsonify({'ok': False, 'error': 'Perangkat tidak ditemukan'}), 404

        sid    = f'session_{int(time.time() * 1000)}'
        now_s  = _ts_now()

        # Update in-memory state dengan lock secepat kilat (sub-milidetik)
        with _capture_lock:
            cstate = _get_device_capture_state(did)
            if cstate.get('active') or cstate.get('_finalizing'):
                return jsonify({'ok': False, 'error': f'Capture untuk perangkat {did} sudah berjalan'}), 409

            cstate.update({
                'active': True, 'device_id': did, 'device_name': dname or did,
                'session_id': sid, 'session_name': sname, 'interval': iv,
                'count': 0, 'started_at': now_s, 'enabled_phases': hints, 'time_offset_ms': 0,
                'sensor_names': sensor_names, 'store_id': store_id,
            })
            _touched_devices.add(did)
            _start_thread(did)

        # MQTT publish & save state file dijalankan DI LUAR LOCK
        if _mqtt_client:
            try:
                _mqtt_client.publish(f"SpartaEnergy/{did}/cmd/resetEnergy", "1")
                _mqtt_client.publish(f"AlfaEnergy/{did}/cmd/resetEnergy", "1")
                _mqtt_client.publish(f"energymeter/{did}/cmd/resetEnergy", "1")
                if did.lower().startswith('plug-'):
                    mac = did[5:]
                    _mqtt_client.publish(f"SpartaEnergy/Plug/{mac}/cmnd/EnergyReset", "0")
                    _mqtt_client.publish(f"AlfaEnergy/Plug/{mac}/cmnd/EnergyReset", "0")
                elif did.upper().startswith('CT-'):
                    mac = did[3:]
                    _mqtt_client.publish(f"SpartaEnergy/CT/{mac}/cmnd/EnergyReset", "0")
                    _mqtt_client.publish(f"AlfaEnergy/CT/{mac}/cmnd/EnergyReset", "0")
            except Exception:
                pass
        persisted = _save_capture_states(did)

        return jsonify({'ok': True, 'session_id': sid, 'session_name': sname, 'device_id': did, 'persisted': persisted})
    except Exception as e:
        print(f"Error in capture_start: {e}")
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/capture/stop', methods=['POST'])
def capture_stop():
    try:
        body = _json_body()
        did  = str(body.get('deviceId') or '').strip()
        if not did:
            return jsonify({'ok': False, 'error': 'deviceId harus diisi'}), 400

        stopped, persisted = _stop_device_capture(did)
        if not stopped:
            return jsonify({'ok': False, 'error': f'Tidak ada capture aktif untuk perangkat {did}'}), 400
        _invalidate_sessions_cache()
        return jsonify({'ok': True, 'device_id': did, 'persisted': persisted})
    except Exception as e:
        print(f"Error in capture_stop: {e}")
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/capture/interval', methods=['POST'])
def capture_interval():
    body = _json_body()
    did = str(body.get('deviceId') or '').strip()
    if not did:
        return jsonify({'ok': False, 'error': 'deviceId harus diisi'}), 400
    try:
        iv = max(15, int(body.get('interval', 15)))
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'interval harus angka'}), 400
    with _capture_lock:
        cstate = _capture_states.get(did)
        if not cstate or not cstate.get('active'):
            return jsonify({'ok': False, 'error': f'Tidak ada capture aktif untuk perangkat {did}'}), 400
        cstate['interval'] = iv
        ev = cstate.get('_wake_event')
        if ev: ev.set()
    persisted = _save_capture_states(did)
    return jsonify({'ok': True, 'interval': iv, 'persisted': persisted})

# Not used by the portal; each could damage recordings.
@app.route('/api/capture/shift_time', methods=['POST'])
@app.route('/api/capture/clear-all', methods=['POST'])
@app.route('/api/devices/<device_id>/relay', methods=['POST'])
@app.route('/api/devices/<device_id>/hourly-capture', methods=['POST'])
@app.route('/api/hourly-capture/trigger-all', methods=['POST'])
def removed_command(device_id: str = ''):
    return jsonify({'ok': False, 'error': 'Perintah ini sudah tidak tersedia'}), 410

@app.route('/api/sessions')
@app.route('/api/devices/<device_id>/sessions')
def get_sessions(device_id: str = 'all'):
    try:
        is_all = not device_id or device_id.lower() == 'all'
        cache_key = 'all' if is_all else device_id
        now_t = time.time()

        cached = None
        with _sessions_cache_lock:
            entry = _sessions_cache.get(cache_key)
            if entry and (now_t - entry.get('timestamp', 0) < 25):  # 25 detik cache
                cached = entry.get('data')

        if cached is None:
            with get_db_cursor() as cur:
                if is_all:
                    cur.execute("""
                        SELECT session_id, 
                               MAX(session_name) as session_name,
                               to_char(to_timestamp(MIN(epoch)/1000.0) AT TIME ZONE 'Asia/Jakarta', 'HH24:MI:SS DD/MM/YYYY') as start_time,
                               to_char(to_timestamp(MAX(epoch)/1000.0) AT TIME ZONE 'Asia/Jakarta', 'HH24:MI:SS DD/MM/YYYY') as end_time,
                               COUNT(*) as record_count,
                               MIN(epoch) as start_epoch,
                               ARRAY_AGG(DISTINCT phase) as phases,
                               MAX(device_name) as snapshot_device_name,
                               MAX(device_id) as device_id
                        FROM history
                        GROUP BY session_id
                        ORDER BY start_epoch DESC
                        LIMIT 100;
                    """)
                else:
                    cur.execute("""
                        SELECT session_id, 
                               MAX(session_name) as session_name,
                               to_char(to_timestamp(MIN(epoch)/1000.0) AT TIME ZONE 'Asia/Jakarta', 'HH24:MI:SS DD/MM/YYYY') as start_time,
                               to_char(to_timestamp(MAX(epoch)/1000.0) AT TIME ZONE 'Asia/Jakarta', 'HH24:MI:SS DD/MM/YYYY') as end_time,
                               COUNT(*) as record_count,
                               MIN(epoch) as start_epoch,
                               ARRAY_AGG(DISTINCT phase) as phases,
                               MAX(device_name) as snapshot_device_name,
                               MAX(device_id) as device_id
                        FROM history
                        WHERE device_id = %s
                        GROUP BY session_id
                        ORDER BY start_epoch DESC
                        LIMIT 100;
                    """, (device_id,))
                rows = cur.fetchall()

            device_names_map = {}
            try:
                with get_db_cursor() as cur:
                    cur.execute("SELECT id, name FROM devices")
                    for d_id, d_name in cur.fetchall():
                        if d_name:
                            device_names_map[d_id] = d_name
            except Exception:
                pass

            cached = []
            for row in rows:
                sid, sname, start_time, end_time, count, start_epoch, phases_arr, snapshot_dname, row_did = row
                phases_list = sorted(phases_arr or [], key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
                cur_dname = device_names_map.get(row_did, row_did)
                cached.append({
                    'id': sid,
                    'name': sname or 'Tanpa nama',
                    'startTime': start_time or '---',
                    'endTime': end_time or '---',
                    'recordCount': count or 0,
                    'startTimestamp': start_epoch or 0,
                    'deviceId': row_did,
                    'deviceName': snapshot_dname or cur_dname,
                    'phases': phases_list,
                    'phaseNames': {ph: ph for ph in phases_list},
                })

            with _sessions_cache_lock:
                _sessions_cache[cache_key] = {'data': cached, 'timestamp': now_t}

        sessions = [dict(s) for s in cached]

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
                        cur_dname = cstate.get('device_name') or active_did

                        existing = next((s for s in sessions if s['id'] == active_sid), None)
                        if existing:
                            existing['recordCount'] = max(existing.get('recordCount', 0), active_count)
                            existing['endTime'] = None
                            existing['isActive'] = True
                        else:
                            sessions.insert(0, {
                                'id': active_sid,
                                'name': active_sname,
                                'startTime': active_start,
                                'endTime': None,
                                'recordCount': active_count,
                                'startTimestamp': int(time.time() * 1000),
                                'deviceId': active_did,
                                'deviceName': cur_dname,
                                'phases': active_phases,
                                'phaseNames': active_names or {ph: ph for ph in active_phases},
                                'isActive': True,
                            })

        return jsonify(sessions)
    except Exception as e:
        print(f"Error getting sessions: {e}")
        return jsonify([]), 500


@app.route('/api/devices/<device_id>/history/<session_id>/<phase>')
def get_session_history_phase(device_id: str, session_id: str, phase: str):
    try:
        phase = phase.upper()
        is_all = not device_id or device_id.lower() == 'all'
        with get_db_cursor() as cur:
            if is_all:
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
        resolved_device_id = device_id
        for row in rows:
            ts, epoch_val, v, c, w, hz, kwh, pf, offline, row_did = row
            resolved_device_id = row_did or resolved_device_id
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
                    SELECT session_name, MIN(timestamp), MAX(timestamp), COUNT(*), MAX(device_id)
                    FROM history
                    WHERE session_id = %s
                    GROUP BY session_name
                """, (session_id,))
                m_row = cur.fetchone()
            if m_row:
                sname, start, end, count, meta_did = m_row
                history['_meta'] = {
                    'id': session_id,
                    'name': sname,
                    'deviceId': meta_did or resolved_device_id,
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
    body = _json_body()
    sid = body.get('sessionId')
    name = str(body.get('name') or '').strip()
    if not sid or not name: return jsonify({'ok': False, 'error': 'Invalid parameters'}), 400
    # Memory first: the worker's next rows must not bring the old name back.
    previous = {}
    with _capture_lock:
        for did, state in _capture_states.items():
            if state.get('active') and state.get('session_id') == sid:
                previous[did] = state.get('session_name')
                state['session_name'] = name
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                UPDATE history
                SET session_name = %s
                WHERE session_id = %s
            """, (name, sid))
    except Exception as e:
        with _capture_lock:
            for did, old in previous.items():
                if _capture_states[did].get('session_id') == sid:
                    _capture_states[did]['session_name'] = old
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500
    persisted = all([_save_capture_states(did) for did in previous])
    _invalidate_sessions_cache()
    return jsonify({'ok': True, 'persisted': persisted})

@app.route('/api/capture/rename-session-sensor', methods=['POST'])
def rename_session_sensor():
    body = _json_body()
    sid = body.get('sessionId')
    phase = body.get('phase')
    name = str(body.get('name') or '').strip()
    if not sid or not phase or not name:
        return jsonify({'ok': False, 'error': 'Invalid parameters'}), 400
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                UPDATE history
                SET phase_name = %s
                WHERE session_id = %s AND LOWER(phase) = LOWER(%s)
            """, (name, sid, phase))
        _invalidate_sessions_cache()
        return jsonify({'ok': True})
    except Exception as e:
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/capture/delete-session', methods=['POST'])
def delete_session():
    body = _json_body()
    sid = body.get('sessionId')
    if not sid: return jsonify({'ok': False, 'error': 'Invalid parameters'}), 400
    if not _state_load_complete: return _not_loaded_response()
    if _is_session_busy(sid): return _busy_response()
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                DELETE FROM history
                WHERE session_id = %s
            """, (sid,))
        _invalidate_sessions_cache()
        return jsonify({'ok': True})
    except Exception as e:
        print(f"[API Error] {request.path}: {e}")
        return jsonify({'ok': False, 'error': 'Internal Server Error'}), 500

@app.route('/api/devices/<device_id>/chart-data')
def get_device_chart_data(device_id: str):
    try:
        date_str = request.args.get('date')
        range_type = request.args.get('range', 'day')
        
        if not date_str or date_str in ('null', 'undefined', ''):
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

# ── SpartaFlash Installer Download & Telemetry Tracker ───────────────────────
_SPARTAFLASH_DOWNLOADS_DIR = os.environ.get(
    "SPARTAFLASH_INSTALLER_DIR",
    os.path.join(os.path.dirname(__file__), "downloads"),
)
_SPARTAFLASH_SQLITE_FALLBACK = os.path.join(_SPARTAFLASH_DOWNLOADS_DIR, "spartaflash_fallback.sqlite3")
_spartaflash_sqlite_lock = threading.Lock()
_spartaflash_gh_cache = {"key": None, "expires": 0.0, "asset": None, "error": ""}


def _spartaflash_cookie_sig(token: str) -> str:
    secret = (
        os.environ.get("PORTAL_CONTROL_TOKEN")
        or os.environ.get("SPARTAFLASH_GITHUB_TOKEN")
        or "spartaflash-dl-bind-key"
    ).encode("utf-8")
    return hmac.new(secret, token.encode("utf-8"), hashlib.sha256).hexdigest()


def _get_spartaflash_github_token() -> str:
    return (os.environ.get("SPARTAFLASH_GITHUB_TOKEN") or os.environ.get("GITHUB_TOKEN") or "").strip()


def _parse_github_release_url(url: str) -> tuple[str, str, str]:
    m = re.match(r"^https?://github\.com/([^/]+/[^/]+)/releases/download/([^/]+)/([^/?#]+)", (url or "").strip(), re.I)
    if m:
        return m.group(1), m.group(2), m.group(3)
    return "", "", ""


def _ensure_spartaflash_sqlite() -> sqlite3.Connection:
    os.makedirs(_SPARTAFLASH_DOWNLOADS_DIR, exist_ok=True)
    conn = sqlite3.connect(_SPARTAFLASH_SQLITE_FALLBACK, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS spartaflash_downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            download_token TEXT UNIQUE NOT NULL,
            downloader_name TEXT NOT NULL,
            branch_division TEXT NOT NULL,
            role_title TEXT DEFAULT '',
            contact TEXT NOT NULL,
            notes TEXT DEFAULT '',
            version TEXT NOT NULL,
            filename TEXT NOT NULL,
            source_mode TEXT NOT NULL,
            ip_address TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            status TEXT DEFAULT 'registered',
            download_hits INTEGER DEFAULT 0,
            registered_at TEXT NOT NULL,
            last_downloaded_at TEXT DEFAULT NULL,
            epoch INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


def _has_pg_pool() -> bool:
    global db_pool
    if db_pool is not None:
        return True
    db_url = os.environ.get("DATABASE_URL") or ""
    if not (db_url.startswith("postgres://") or db_url.startswith("postgresql://")):
        return False
    init_db()
    return db_pool is not None


def _get_client_ip() -> str:
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()[:64]
    xri = (request.headers.get("X-Real-IP") or "").strip()
    if xri:
        return xri[:64]
    return (request.remote_addr or "")[:64]


def _default_spartaflash_config() -> dict:
    return {
        "mode": (os.environ.get("SPARTAFLASH_DOWNLOAD_MODE") or "auto").strip().lower(),
        "version": (os.environ.get("SPARTAFLASH_VERSION") or "v2.1.6").strip(),
        "filename": os.path.basename((os.environ.get("SPARTAFLASH_INSTALLER_FILE") or "SpartaFlash_Setup_v2.1.6_Windows_x64.exe").strip()),
        "github_repo": (os.environ.get("SPARTAFLASH_GITHUB_REPO") or "buildingprocess25/SpartaFlash").strip(),
        "github_tag": (os.environ.get("SPARTAFLASH_GITHUB_TAG") or "latest").strip(),
        "github_url": (os.environ.get("SPARTAFLASH_GITHUB_URL") or "https://github.com/buildingprocess25/SpartaFlash/releases/download/v2.1.6/SpartaFlash_Setup_v2.1.6_Windows_x64.exe").strip(),
        "sha256": (os.environ.get("SPARTAFLASH_SHA256") or "8563333199744e1fe2bf0b8ecc892e700564868d0a01475c539b35dd2cd9fbdc").strip(),
    }


def _resolve_github_private_asset(cfg: dict, force_refresh: bool = False) -> tuple[dict | None, str]:
    token = _get_spartaflash_github_token()
    if not token:
        return None, "SPARTAFLASH_GITHUB_TOKEN belum dikonfigurasi di environment server."

    repo = (cfg.get("github_repo") or "").strip().strip("/")
    tag = (cfg.get("github_tag") or "latest").strip()
    want_name = os.path.basename(cfg.get("filename") or "")

    if not repo and cfg.get("github_url"):
        parsed_repo, parsed_tag, parsed_name = _parse_github_release_url(cfg["github_url"])
        repo = parsed_repo or repo
        tag = parsed_tag or tag
        want_name = parsed_name or want_name

    if not repo or "/" not in repo:
        return None, "Format GitHub repository tidak valid (contoh: buildingprocess25/SpartaFlash)."

    cache_key = f"{repo}|{tag}|{want_name}|{hashlib.sha256(token.encode('utf-8')).hexdigest()[:8]}"
    now = time.time()
    if not force_refresh and _spartaflash_gh_cache["key"] == cache_key and _spartaflash_gh_cache["expires"] > now:
        return _spartaflash_gh_cache["asset"], _spartaflash_gh_cache["error"]

    api_url = (
        f"https://api.github.com/repos/{repo}/releases/latest"
        if tag.lower() == "latest"
        else f"https://api.github.com/repos/{repo}/releases/tags/{tag}"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "SpartaEnergyPortal/1.0",
    }
    try:
        resp = http_requests.get(api_url, headers=headers, timeout=10)
        if resp.status_code != 200:
            err = f"GitHub API HTTP {resp.status_code} saat mengakses {repo} ({tag})."
            _spartaflash_gh_cache.update({"key": cache_key, "expires": now + 20, "asset": None, "error": err})
            return None, err

        rel = resp.json()
        assets = rel.get("assets") or []
        chosen = None
        if want_name and tag.lower() != "latest":
            chosen = next((a for a in assets if a.get("name") == want_name), None)
        if chosen is None and want_name:
            chosen = next((a for a in assets if a.get("name") == want_name), None)
        if chosen is None:
            chosen = next((a for a in assets if str(a.get("name") or "").lower().endswith(".exe")), None)
        if chosen is None:
            err = f"Asset installer (.exe) tidak ditemukan pada release {rel.get('tag_name') or tag} di {repo}."
            _spartaflash_gh_cache.update({"key": cache_key, "expires": now + 20, "asset": None, "error": err})
            return None, err

        digest = str(chosen.get("digest") or "")
        if digest.startswith("sha256:"):
            digest = digest.split(":", 1)[1]
        asset_info = {
            "id": chosen.get("id"),
            "url": chosen.get("url"),
            "name": os.path.basename(chosen.get("name") or want_name),
            "size": int(chosen.get("size") or 0),
            "digest": digest,
            "tag_name": str(rel.get("tag_name") or tag),
            "repo": repo,
        }
        _spartaflash_gh_cache.update({"key": cache_key, "expires": now + 60, "asset": asset_info, "error": ""})
        return asset_info, ""
    except Exception as e:
        err = f"Gagal menghubungi GitHub API: {e}"
        _spartaflash_gh_cache.update({"key": cache_key, "expires": now + 15, "asset": None, "error": err})
        return None, err


def _get_spartaflash_config() -> dict:
    cfg = _default_spartaflash_config()
    saved = None
    try:
        if _has_pg_pool():
            with get_db_cursor() as cur:
                cur.execute("SELECT value FROM app_settings WHERE key = %s", ("spartaflash_config",))
                row = cur.fetchone()
                if row and row[0]:
                    saved = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        else:
            with _spartaflash_sqlite_lock:
                conn = _ensure_spartaflash_sqlite()
                try:
                    row = conn.execute("SELECT value FROM app_settings WHERE key = ?", ("spartaflash_config",)).fetchone()
                    if row and row["value"]:
                        saved = json.loads(row["value"])
                finally:
                    conn.close()
    except Exception as e:
        print(f"[SpartaFlash] Failed reading config: {e}")

    if isinstance(saved, dict):
        for k in ("mode", "version", "filename", "github_repo", "github_tag", "github_url", "sha256"):
            val = str(saved.get(k) or "").strip()
            if val:
                cfg[k] = os.path.basename(val) if k == "filename" else val

    if cfg["mode"] == "github":
        cfg["mode"] = "github_private"
    if cfg["mode"] not in ("auto", "local", "github_private", "github_redirect"):
        cfg["mode"] = "auto"

    safe_filename = os.path.basename(cfg["filename"]) or "SpartaFlash_Setup_v2.1.6_Windows_x64.exe"
    cfg["filename"] = safe_filename
    local_path = os.path.join(_SPARTAFLASH_DOWNLOADS_DIR, safe_filename)
    local_available = os.path.isfile(local_path)
    local_size_bytes = os.path.getsize(local_path) if local_available else 0
    token_configured = bool(_get_spartaflash_github_token())

    if cfg["mode"] == "github_redirect":
        effective_source = "github_redirect"
    elif cfg["mode"] == "github_private":
        effective_source = "github_private" if token_configured else "missing_github_token"
    elif cfg["mode"] == "local":
        effective_source = "local" if local_available else "missing_local"
    else:
        if local_available:
            effective_source = "local"
        elif token_configured:
            effective_source = "github_private"
        else:
            effective_source = "missing_github_token"

    remote_asset = None
    remote_error = ""
    if token_configured and effective_source in ("github_private", "local"):
        remote_asset, remote_error = _resolve_github_private_asset(cfg)
        if effective_source == "github_private" and remote_asset:
            cfg["version"] = remote_asset["tag_name"] or cfg["version"]
            cfg["filename"] = remote_asset["name"] or cfg["filename"]
            if remote_asset.get("digest"):
                cfg["sha256"] = remote_asset["digest"]

    cfg["local_path"] = local_path
    cfg["local_available"] = local_available
    cfg["local_size_bytes"] = local_size_bytes
    cfg["github_token_configured"] = token_configured
    cfg["remote_available"] = bool(remote_asset)
    cfg["remote_size_bytes"] = remote_asset["size"] if remote_asset else 0
    cfg["remote_error"] = remote_error
    cfg["effective_source"] = effective_source
    return cfg


def _save_spartaflash_config(new_cfg: dict) -> dict:
    payload = {
        "mode": new_cfg["mode"],
        "version": new_cfg["version"],
        "filename": os.path.basename(new_cfg["filename"]),
        "github_repo": new_cfg["github_repo"],
        "github_tag": new_cfg["github_tag"],
        "github_url": new_cfg["github_url"],
        "sha256": new_cfg["sha256"],
    }
    raw_json = json.dumps(payload)
    if _has_pg_pool():
        with get_db_cursor() as cur:
            cur.execute("""
                INSERT INTO app_settings (key, value, updated_at)
                VALUES (%s, %s::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
            """, ("spartaflash_config", raw_json))
    else:
        with _spartaflash_sqlite_lock:
            conn = _ensure_spartaflash_sqlite()
            try:
                conn.execute("""
                    INSERT INTO app_settings (key, value, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
                """, ("spartaflash_config", raw_json))
                conn.commit()
            finally:
                conn.close()
    _spartaflash_gh_cache["expires"] = 0.0
    return _get_spartaflash_config()


def _query_spartaflash_downloads(q: str = "", limit: int = 200) -> tuple[list[dict], dict]:
    q_norm = (q or "").strip().lower()
    limit_val = max(1, min(int(limit or 200), 1000))
    rows_out = []
    stats = {"total_registrations": 0, "total_downloads": 0, "today_downloads": 0, "unique_branches": 0}
    today_suffix = datetime.now(_WIB).strftime("%d/%m/%Y")

    cols = (
        "id, download_token, downloader_name, branch_division, role_title, contact, "
        "notes, version, filename, source_mode, ip_address, user_agent, status, "
        "download_hits, registered_at, last_downloaded_at, epoch"
    )

    if _has_pg_pool():
        with get_db_cursor() as cur:
            cur.execute(f"SELECT {cols} FROM spartaflash_downloads ORDER BY epoch DESC, id DESC LIMIT %s", (limit_val,))
            raw_rows = cur.fetchall()
            cur.execute("""
                SELECT
                    COUNT(*) AS total_reg,
                    COALESCE(SUM(CASE WHEN download_hits > 0 THEN 1 ELSE 0 END), 0) AS total_dl,
                    COUNT(DISTINCT LOWER(TRIM(branch_division))) AS uniq_branches
                FROM spartaflash_downloads
            """)
            agg = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM spartaflash_downloads WHERE RIGHT(registered_at, 10) = %s", (today_suffix,))
            today_row = cur.fetchone()
        if agg:
            stats["total_registrations"] = int(agg[0] or 0)
            stats["total_downloads"] = int(agg[1] or 0)
            stats["unique_branches"] = int(agg[2] or 0)
        stats["today_downloads"] = int(today_row[0] or 0) if today_row else 0
    else:
        with _spartaflash_sqlite_lock:
            conn = _ensure_spartaflash_sqlite()
            try:
                raw_rows = conn.execute(
                    f"SELECT {cols} FROM spartaflash_downloads ORDER BY epoch DESC, id DESC LIMIT ?",
                    (limit_val,),
                ).fetchall()
                agg = conn.execute("""
                    SELECT
                        COUNT(*) AS total_reg,
                        COALESCE(SUM(CASE WHEN download_hits > 0 THEN 1 ELSE 0 END), 0) AS total_dl,
                        COUNT(DISTINCT LOWER(TRIM(branch_division))) AS uniq_branches
                    FROM spartaflash_downloads
                """).fetchone()
                today_row = conn.execute(
                    "SELECT COUNT(*) FROM spartaflash_downloads WHERE substr(registered_at, -10) = ?",
                    (today_suffix,),
                ).fetchone()
            finally:
                conn.close()
        if agg:
            stats["total_registrations"] = int(agg[0] or 0)
            stats["total_downloads"] = int(agg[1] or 0)
            stats["unique_branches"] = int(agg[2] or 0)
        stats["today_downloads"] = int(today_row[0] or 0) if today_row else 0

    for r in raw_rows:
        item = {
            "id": r[0],
            "downloader_name": r[2],
            "branch_division": r[3],
            "role_title": r[4] or "",
            "contact": r[5],
            "notes": r[6] or "",
            "version": r[7],
            "filename": r[8],
            "source_mode": r[9],
            "ip_address": r[10] or "",
            "user_agent": r[11] or "",
            "status": r[12] or "registered",
            "download_hits": int(r[13] or 0),
            "registered_at": r[14],
            "last_downloaded_at": r[15] or "",
            "epoch": int(r[16] or 0),
        }
        if q_norm:
            haystack = " ".join([
                item["downloader_name"], item["branch_division"], item["role_title"],
                item["contact"], item["notes"], item["ip_address"], item["version"],
            ]).lower()
            if q_norm not in haystack:
                continue
        rows_out.append(item)
    return rows_out, stats


@app.route('/api/spartaflash/info', methods=['GET'])
def spartaflash_info():
    try:
        cfg = _get_spartaflash_config()
        _, stats = _query_spartaflash_downloads(limit=1)
        public_cfg = {k: v for k, v in cfg.items() if k != "local_path"}
        return jsonify({"ok": True, "config": public_cfg, "stats": stats})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/spartaflash/config', methods=['POST'])
def spartaflash_update_config():
    try:
        body = request.get_json(silent=True) or {}
        cur_cfg = _get_spartaflash_config()
        mode = (body.get("mode") or cur_cfg["mode"]).strip().lower()
        if mode == "github":
            mode = "github_private"
        version = (body.get("version") or cur_cfg["version"]).strip()
        filename = os.path.basename((body.get("filename") or cur_cfg["filename"]).strip())
        github_repo = (body.get("github_repo") or cur_cfg["github_repo"]).strip().strip("/")
        github_tag = (body.get("github_tag") or cur_cfg["github_tag"]).strip()
        github_url = (body.get("github_url") or cur_cfg["github_url"]).strip()
        sha256 = (body.get("sha256") or cur_cfg["sha256"]).strip()

        if mode not in ("auto", "local", "github_private", "github_redirect"):
            return jsonify({"ok": False, "error": "Mode harus 'auto', 'local', 'github_private', atau 'github_redirect'."}), 400
        if github_url and not (github_url.startswith("https://") or github_url.startswith("http://")):
            return jsonify({"ok": False, "error": "GitHub URL harus diawali https:// atau http://."}), 400
        if not version or not filename:
            return jsonify({"ok": False, "error": "Version dan filename wajib diisi."}), 400
        if not github_repo or "/" not in github_repo:
            return jsonify({"ok": False, "error": "GitHub Repo harus format owner/repo."}), 400

        updated = _save_spartaflash_config({
            "mode": mode, "version": version, "filename": filename,
            "github_repo": github_repo, "github_tag": github_tag or "latest",
            "github_url": github_url, "sha256": sha256,
        })
        public_cfg = {k: v for k, v in updated.items() if k != "local_path"}
        return jsonify({"ok": True, "config": public_cfg})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/spartaflash/register', methods=['POST'])
def spartaflash_register_download():
    try:
        body = request.get_json(silent=True) or {}
        name = (body.get("downloader_name") or "").strip()
        branch = (body.get("branch_division") or "").strip()
        role_title = (body.get("role_title") or "").strip()
        contact = (body.get("contact") or "").strip()
        notes = (body.get("notes") or "").strip()

        if len(name) < 2:
            return jsonify({"ok": False, "error": "Nama lengkap / PIC wajib diisi (minimal 2 karakter)."}), 400
        if len(branch) < 2:
            return jsonify({"ok": False, "error": "Cabang / Toko / Divisi wajib diisi."}), 400
        if len(contact) < 5:
            return jsonify({"ok": False, "error": "No. WhatsApp atau Email wajib diisi (minimal 5 karakter)."}), 400

        cfg = _get_spartaflash_config()
        if cfg["effective_source"] == "missing_local":
            return jsonify({
                "ok": False,
                "error": f"File installer '{cfg['filename']}' belum tersedia di server (backend/downloads/). Upload file atau ubah sumber ke GitHub Private Proxy.",
            }), 404
        if cfg["effective_source"] == "missing_github_token":
            return jsonify({
                "ok": False,
                "error": "Akses GitHub Private belum aktif (SPARTAFLASH_GITHUB_TOKEN belum diset di environment server).",
            }), 503
        if cfg["effective_source"] == "github_private" and not cfg.get("remote_available"):
            return jsonify({
                "ok": False,
                "error": cfg.get("remote_error") or "Gagal memverifikasi file release di repository private GitHub.",
            }), 502

        token = secrets.token_urlsafe(24)
        now_dt = datetime.now(_WIB)
        now_ts = now_dt.strftime("%H:%M:%S %d/%m/%Y")
        now_epoch = int(now_dt.timestamp() * 1000)
        ip_addr = _get_client_ip()
        user_agent = (request.headers.get("User-Agent") or "")[:300]

        if _has_pg_pool():
            with get_db_cursor() as cur:
                cur.execute("""
                    INSERT INTO spartaflash_downloads (
                        download_token, downloader_name, branch_division, role_title, contact,
                        notes, version, filename, source_mode, ip_address, user_agent,
                        status, download_hits, registered_at, epoch
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'registered', 0, %s, %s)
                    RETURNING id
                """, (
                    token, name[:120], branch[:120], role_title[:80], contact[:120],
                    notes[:500], cfg["version"][:32], cfg["filename"][:255],
                    cfg["effective_source"][:32], ip_addr, user_agent, now_ts, now_epoch,
                ))
                rec_id = cur.fetchone()[0]
        else:
            with _spartaflash_sqlite_lock:
                conn = _ensure_spartaflash_sqlite()
                try:
                    cur = conn.execute("""
                        INSERT INTO spartaflash_downloads (
                            download_token, downloader_name, branch_division, role_title, contact,
                            notes, version, filename, source_mode, ip_address, user_agent,
                            status, download_hits, registered_at, epoch
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'registered', 0, ?, ?)
                    """, (
                        token, name[:120], branch[:120], role_title[:80], contact[:120],
                        notes[:500], cfg["version"][:32], cfg["filename"][:255],
                        cfg["effective_source"][:32], ip_addr, user_agent, now_ts, now_epoch,
                    ))
                    rec_id = cur.lastrowid
                    conn.commit()
                finally:
                    conn.close()

        resp = make_response(jsonify({
            "ok": True,
            "id": rec_id,
            "token": token,
            "download_url": f"/api/spartaflash/download/{token}",
            "version": cfg["version"],
            "filename": cfg["filename"],
            "sha256": cfg["sha256"],
            "source_mode": cfg["effective_source"],
            "registered_at": now_ts,
        }))
        is_https = request.is_secure or request.headers.get("X-Forwarded-Proto", "").lower() == "https"
        resp.set_cookie(
            "sf_dl_bind",
            _spartaflash_cookie_sig(token),
            max_age=300,
            path="/",
            httponly=True,
            samesite="Lax",
            secure=is_https,
        )
        return resp
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/spartaflash/download/<token>', methods=['POST', 'GET'])
def spartaflash_execute_download(token: str):
    try:
        clean_token = (token or "").strip()
        if not clean_token:
            return jsonify({"ok": False, "error": "Token unduhan tidak valid."}), 400

        now_dt = datetime.now(_WIB)
        now_ts = now_dt.strftime("%H:%M:%S %d/%m/%Y")
        now_epoch = int(now_dt.timestamp() * 1000)

        rec_epoch = None
        rec_hits = 0
        rec_ip = ""
        if _has_pg_pool():
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT id, epoch, download_hits, ip_address FROM spartaflash_downloads WHERE download_token = %s",
                    (clean_token,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"ok": False, "error": "Token unduhan tidak ditemukan. Silakan isi form pendataan terlebih dahulu."}), 404
                rec_epoch = int(row[1] or 0)
                rec_hits = int(row[2] or 0)
                rec_ip = str(row[3] or "")
        else:
            with _spartaflash_sqlite_lock:
                conn = _ensure_spartaflash_sqlite()
                try:
                    row = conn.execute(
                        "SELECT id, epoch, download_hits, ip_address FROM spartaflash_downloads WHERE download_token = ?",
                        (clean_token,),
                    ).fetchone()
                    if not row:
                        return jsonify({"ok": False, "error": "Token unduhan tidak ditemukan. Silakan isi form pendataan terlebih dahulu."}), 404
                    rec_epoch = int(row["epoch"] or 0)
                    rec_hits = int(row["download_hits"] or 0)
                    rec_ip = str(row["ip_address"] or "")
                finally:
                    conn.close()

        # Kunci token ke HttpOnly browser cookie pengisi form (tahan terhadap pergantian IPv4/IPv6 proxy)
        got_sig = (request.cookies.get("sf_dl_bind") or "").strip()
        expected_sig = _spartaflash_cookie_sig(clean_token)
        if not got_sig or not hmac.compare_digest(got_sig, expected_sig):
            return jsonify({
                "ok": False,
                "error": "Akses ditolak: link unduhan hanya berlaku di browser yang mengisi form. Silakan isi form pendataan terlebih dahulu.",
            }), 403

        # Token berlaku 5 menit & maksimal 3x unduhan (1x otomatis + retry)
        if rec_epoch and (now_epoch - rec_epoch) > (5 * 60 * 1000):
            return jsonify({"ok": False, "error": "Sesi unduhan sudah kedaluwarsa (>5 menit). Silakan isi ulang form pendataan."}), 410
        if rec_hits >= 3:
            return jsonify({"ok": False, "error": "Batas unduhan maksimal (3x) untuk sesi ini sudah tercapai. Silakan isi ulang form pendataan."}), 429

        cfg = _get_spartaflash_config()
        if cfg["effective_source"] == "missing_local":
            return jsonify({
                "ok": False,
                "error": f"File installer '{cfg['filename']}' belum tersedia di server.",
            }), 404
        if cfg["effective_source"] == "missing_github_token":
            return jsonify({
                "ok": False,
                "error": "SPARTAFLASH_GITHUB_TOKEN belum dikonfigurasi di server.",
            }), 503

        effective_source = cfg["effective_source"]
        if _has_pg_pool():
            with get_db_cursor() as cur:
                cur.execute("""
                    UPDATE spartaflash_downloads
                    SET status = 'downloaded',
                        download_hits = download_hits + 1,
                        last_downloaded_at = %s,
                        source_mode = %s
                    WHERE download_token = %s
                """, (now_ts, effective_source, clean_token))
        else:
            with _spartaflash_sqlite_lock:
                conn = _ensure_spartaflash_sqlite()
                try:
                    conn.execute("""
                        UPDATE spartaflash_downloads
                        SET status = 'downloaded',
                            download_hits = download_hits + 1,
                            last_downloaded_at = ?,
                            source_mode = ?
                        WHERE download_token = ?
                    """, (now_ts, effective_source, clean_token))
                    conn.commit()
                finally:
                    conn.close()

        if effective_source == "local" and cfg["local_available"]:
            return send_file(
                cfg["local_path"],
                as_attachment=True,
                download_name=cfg["filename"],
                conditional=True,
            )

        if effective_source == "github_private":
            asset_info, err = _resolve_github_private_asset(cfg)
            if not asset_info:
                return jsonify({"ok": False, "error": err or "Asset release GitHub private tidak tersedia."}), 502

            gh_token = _get_spartaflash_github_token()
            api_headers = {
                "Authorization": f"Bearer {gh_token}",
                "Accept": "application/octet-stream",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "SpartaEnergyPortal/1.0",
            }
            init_resp = http_requests.get(
                asset_info["url"],
                headers=api_headers,
                stream=True,
                allow_redirects=False,
                timeout=15,
            )
            if init_resp.status_code in (301, 302, 303, 307, 308):
                signed_url = init_resp.headers.get("Location")
                init_resp.close()
                if not signed_url:
                    return jsonify({"ok": False, "error": "URL asset sementara dari GitHub kosong."}), 502
                upstream = http_requests.get(signed_url, stream=True, timeout=(15, 120))
            elif init_resp.status_code == 200:
                upstream = init_resp
            else:
                code = init_resp.status_code
                init_resp.close()
                return jsonify({"ok": False, "error": f"Gagal mengambil installer dari GitHub API (HTTP {code})."}), 502

            if upstream.status_code != 200:
                code = upstream.status_code
                upstream.close()
                return jsonify({"ok": False, "error": f"Gagal mengunduh stream installer (HTTP {code})."}), 502

            safe_name = asset_info["name"] or cfg["filename"]
            content_len = upstream.headers.get("Content-Length") or (str(asset_info["size"]) if asset_info.get("size") else "")

            def generate():
                try:
                    for chunk in upstream.iter_content(chunk_size=262144):
                        if chunk:
                            yield chunk
                finally:
                    upstream.close()

            resp_headers = {
                "Content-Disposition": f'attachment; filename="{safe_name}"',
                "Cache-Control": "no-store",
            }
            if content_len:
                resp_headers["Content-Length"] = str(content_len)
            return Response(stream_with_context(generate()), mimetype="application/octet-stream", headers=resp_headers)

        return redirect(cfg["github_url"], code=302)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/spartaflash/downloads', methods=['GET'])
def spartaflash_list_downloads():
    try:
        q = request.args.get("q", "")
        limit = request.args.get("limit", "200")
        rows, stats = _query_spartaflash_downloads(q=q, limit=int(limit))
        return jsonify({"ok": True, "downloads": rows, "stats": stats})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/spartaflash/downloads/export', methods=['GET'])
def spartaflash_export_downloads_csv():
    try:
        q = request.args.get("q", "")
        rows, _ = _query_spartaflash_downloads(q=q, limit=1000)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "ID", "Waktu Daftar (WIB)", "Terakhir Unduh (WIB)", "Nama Pengunduh",
            "Cabang / Divisi", "Jabatan / Peran", "Kontak", "Keperluan / Catatan",
            "Versi", "Nama File", "Sumber", "Status", "Jumlah Hits", "IP Address",
        ])
        for item in rows:
            writer.writerow([
                item["id"], item["registered_at"], item["last_downloaded_at"],
                item["downloader_name"], item["branch_division"], item["role_title"],
                item["contact"], item["notes"], item["version"], item["filename"],
                item["source_mode"], item["status"], item["download_hits"], item["ip_address"],
            ])
        csv_bytes = ("\ufeff" + buf.getvalue()).encode("utf-8")
        fname = f"spartaflash_downloads_{datetime.now(_WIB).strftime('%Y%m%d_%H%M%S')}.csv"
        return Response(
            csv_bytes,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'},
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/spartaflash/downloads/<int:record_id>', methods=['DELETE'])
def spartaflash_delete_download(record_id: int):
    try:
        deleted = 0
        if _has_pg_pool():
            with get_db_cursor() as cur:
                cur.execute("DELETE FROM spartaflash_downloads WHERE id = %s", (record_id,))
                deleted = cur.rowcount
        else:
            with _spartaflash_sqlite_lock:
                conn = _ensure_spartaflash_sqlite()
                try:
                    cur = conn.execute("DELETE FROM spartaflash_downloads WHERE id = ?", (record_id,))
                    deleted = cur.rowcount
                    conn.commit()
                finally:
                    conn.close()
        if not deleted:
            return jsonify({"ok": False, "error": "Data pengunduh tidak ditemukan."}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# Last in the module: resuming a recording needs every function above to exist.
if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    init_db()
    try:
        _load_capture_states()
    except Exception as e:
        # The retry worker tries again; a failed resume must not keep the server down.
        print(f"ERROR resuming recordings at boot: {e}")
    threading.Thread(target=_live_buffer_worker, daemon=True).start()
    threading.Thread(target=_state_retry_worker, daemon=True).start()

if __name__ == '__main__':
    flask_host = os.environ.get("FLASK_HOST", "0.0.0.0")
    try:
        flask_port = int(os.environ.get("FLASK_PORT", "5000"))
    except ValueError:
        flask_port = 5000
    
    app.run(debug=True, host=flask_host, port=flask_port)
