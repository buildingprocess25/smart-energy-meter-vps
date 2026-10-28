# Spesifikasi & Dokumentasi Firmware AlfaEnergy (v1.3.0)

Dokumen ini mencatat seluruh konfigurasi, arsitektur topik MQTT, payload schema, command, pinout hardware, dan OTA firmware ESP32 **AlfaEnergy v1.3.0** sebagai referensi pengembang.

---

## 1. Identitas & Versi Firmware
- **Versi Firmware**: `1.3.0`
- **Prefix Root MQTT**: `AlfaEnergy`
- **Format Device ID**: Alphanumeric + tanda hubung (contoh: `EM-0001`, `MC3`, `ESP32-01`).
- **OTA Hostname**: `EnergyMeter-<DEVICE_ID>`
- **Local OTA Password**: `AlfaEnergy`

---

## 2. GitHub OTA URL
- **Version Check URL**:
  `https://raw.githubusercontent.com/rohidfkra777/AlfaEnergy/refs/heads/main/version.txt`
- **Binary Firmware URL**:
  `https://github.com/rohidfkra777/AlfaEnergy/releases/download/Firmware/AlfaEnergy.ino.bin`
- **Mekanisme**: OTA hanya dijalankan ketika menerima command MQTT (`/cmd/ota` atau `/all/cmd/ota`), tidak auto-check di background.

---

## 3. Konfigurasi Broker MQTT Default
- **Host**: `103.127.99.241` (VPS Mosquitto)
- **Port**: `1883`
- **Username**: `sparta-ganteng`
- **Password**: `4fa9aafd748a1e0e4360e0035082fd00ad247983dded761b3fa2d5b566d605f6`
- **KeepAlive**: `120` detik
- **Buffer Size**: `1024` bytes

---

## 4. Hardware Pinout & Spesifikasi
- **PZEM-004T Serial**:
  - **RX**: GPIO `16`
  - **TX**: GPIO `17`
  - **Baudrate**: `9600` (8N1)
  - **Jumlah PZEM Didukung**: Hingga `20` modul (Address `0x01` – `0x14`)
  - **Max Channel Aktif per Device**: `5` channel
- **Tombol Fisik Reset Channel**:
  - **Pin**: GPIO `4` (Active LOW / `INPUT_PULLUP` ke GND)
  - **Hold Duration**: `5000ms` (5 detik) untuk mencegah reset tidak sengaja.
  - **Aksi**: Menghapus konfigurasi channel + WiFi, namun tetap mempertahankan Device ID.

---

## 5. Threshold Pengiriman Data (Delta Filter)
Data sensor dikirim jika ada perubahan nilai melebihi ambang batas berikut:
- **Interval Kirim**: `1000ms` (1 detik)
- **Interval Reconnect**: `5000ms`
- **Delta Tegangan ($\Delta V$)**: `0.5 V`
- **Delta Arus ($\Delta A$)**: `0.01 A`
- **Delta Daya ($\Delta W$)**: `1.0 W`
- **Delta Frekuensi ($\Delta Hz$)**: `0.1 Hz`
- **Delta Energi ($\Delta kWh$)**: `0.001 kWh`
- **Delta Power Factor ($\Delta pf$)**: `0.01`

---

## 6. Struktur Topik MQTT

### A. Data Telemetri Sensor
Format topik:
```text
AlfaEnergy/<DEVICE_ID>/<MODE>/...
```

1. **Mode 3 Phase (3P)** *(4 Segmen)*:
   - `AlfaEnergy/<DEVICE_ID>/3P/R`
   - `AlfaEnergy/<DEVICE_ID>/3P/S`
   - `AlfaEnergy/<DEVICE_ID>/3P/T`
2. **Mode 1 Phase (1P)** *(5 Segmen - v1.3.0)*:
   - `AlfaEnergy/<DEVICE_ID>/1P/<LOAD_TYPE>/<UNIT_NUMBER>`
   - Contoh: `AlfaEnergy/EM-0001/1P/LAMP/01`, `AlfaEnergy/EM-0001/1P/AC/01`, `AlfaEnergy/EM-0001/1P/REFRIGERATOR/02`
   - **Load Types**: `AC`, `COOLER`, `LAMP`, `REFRIGERATOR`, `FREEZER`, `PUMP`, `MAIN`, `DISTRIBUTION`, `PRODUCTION`, `OTHER`
   - **Unit Number**: `01` s/d `10`
3. **Format Payload JSON**:
   ```json
   {"V":230.5,"A":1.427,"W":218.9,"Hz":50.00,"kWh":0.332,"pf":0.670}
   ```

### B. Timestamp
- **Topik**: `AlfaEnergy/<DEVICE_ID>/Timestamp`
- **Payload**: `HH:MM:SS DD/MM/YYYY` (contoh: `10:37:19 28/08/2026`)

### C. Status Perangkat (Published Retained)
- `AlfaEnergy/<DEVICE_ID>/status/online` &rarr; `true`
- `AlfaEnergy/<DEVICE_ID>/status/deviceID` &rarr; `<DEVICE_ID>`
- `AlfaEnergy/<DEVICE_ID>/status/version` &rarr; `1.3.0`
- `AlfaEnergy/<DEVICE_ID>/status/ip` &rarr; IP lokal ESP32 (misal `192.168.1.50`)
- `AlfaEnergy/<DEVICE_ID>/status/activeChannels` &rarr; Jumlah channel aktif (misal `3`)
- `AlfaEnergy/<DEVICE_ID>/status/channel` &rarr; `resetting` (saat reset dipicu)
- `AlfaEnergy/<DEVICE_ID>/status/resetEnergy` &rarr; `completed` / `invalid_payload`

### D. Command (Subscribed oleh ESP32)
1. **Reset Energi ($kWh \rightarrow 0$)**:
   - Topik: `AlfaEnergy/<DEVICE_ID>/cmd/resetEnergy`
   - Payload: `1`
2. **Reset Konfigurasi Channel**:
   - Topik: `AlfaEnergy/<DEVICE_ID>/cmd/resetChannel`
   - Payload: `1`
3. **Ubah Device ID**:
   - Topik: `AlfaEnergy/<DEVICE_ID>/cmd/setDeviceID`
   - Payload: `<ID_BARU>` (string 2-20 karakter)
4. **Reset WiFi**:
   - Topik: `AlfaEnergy/<DEVICE_ID>/cmd/resetWiFi`
   - Payload: `1`
5. **Ganti Kredensial WiFi**:
   - Topik: `AlfaEnergy/<DEVICE_ID>/cmd/setWiFi`
   - Payload JSON: `{"ssid": "NamaWiFi", "password": "PasswordWiFi"}`
6. **Trigger OTA per Device**:
   - Topik: `AlfaEnergy/<DEVICE_ID>/cmd/ota`
   - Payload: `1`
7. **Trigger Global OTA (Semua Device)**:
   - Topik: `AlfaEnergy/all/cmd/ota`
   - Payload: `1`

---

## 7. Storage NVS (Preferences ESP32)
- **Namespace `"device"`**:
  - Key `"id"`: Menyimpan Device ID.
- **Namespace `"metercfg"`**:
  - Key `"count"`: Jumlah channel terkonfigurasi.
  - Key `"c0_en"`, `"c0_addr"`, `"c0_mode"`, `"c0_phase"`, `"c0_load"`, `"c0_unit"` (per channel $0..4$).

---

## 8. Spesifikasi Firmware AlfaEnergy CT & Plug (Tasmota-Based)
Perangkat yang menggunakan firmware di folder `AlfaEnergy/firmware/` (menggantikan `AlfaEnergy/CTandPlug/`):
- `AlfaEnergy_CT.bin` (Tasmota32 ESP32)
- `AlfaEnergy_Plug.bin` (Tasmota32C3 ESP32-C3)

### A. Struktur Topik MQTT Baru (v2.0 Firmware)
Format topik: `AlfaEnergy/<TYPE>/<DEVICE_ID>/<prefix>/...` di mana `<TYPE>` adalah `CT` atau `Plug`, dan `<DEVICE_ID>` adalah 6 digit MAC ESP (contoh: `E62A34`, `B80E68`).

**Konvensi Penamaan Dinamis di Dashboard:**
- Khusus perangkat `CT` & `Plug`, ID dan penamaan default di dashboard dibentuk secara dinamis:
  - Format Device ID: `<TIPE>-<MAC>` (contoh: `CT-E62A34`, `Plug-B80E68`, `CT-XXXXXX`, `Plug-YYYYYY`).
  - Berapapun jumlah device baru yang ditambahkan, sistem otomatis mengenali tipe dan MAC-nya.
- **Inisial (Badge) & Nama Sensor:**
  - Perangkat **CT**: Inisial badge = **`CT`**, Nama sensor default = **`CT`** (bukan `L1` / `Sensor 1`).
  - Perangkat **Plug**: Inisial badge = **`PG`**, Nama sensor default = **`Plug`** (bukan `L1` / `Sensor 1`).
- Perangkat lama (seperti `EM-0001`, `EM-0002`, `MC3`, dll.) tetap mempertahankan ID (`EM-...`) dan inisial sensor aslinya (`L1`, `L2`, dst.) tanpa perubahan.

1. **Telemetri Sensor**:
   - CT: `AlfaEnergy/CT/<DEVICE_ID>/tele/SENSOR` $\rightarrow$ Device: `CT-<DEVICE_ID>`, Sensor: `CT`
   - Plug: `AlfaEnergy/Plug/<DEVICE_ID>/tele/SENSOR` $\rightarrow$ Device: `Plug-<DEVICE_ID>`, Sensor: `PG` (`Plug`)
   - Payload JSON berisi objek `"ENERGY"` (`Voltage`, `Current`, `Power`, `Frequency`, `Total`, `Factor`, `ApparentPower`, `ReactivePower`) dan `"Time"`.
2. **Status Perangkat**:
   - `AlfaEnergy/<CT|Plug>/<DEVICE_ID>/stat/STATE` dan `AlfaEnergy/<CT|Plug>/<DEVICE_ID>/tele/STATE`
   - Payload JSON berisi `POWER` (relay), `Uptime`, `Heap`, `Wifi` (SSID, RSSI, Signal).
3. **Status Relay Smart Plug**:
   - `AlfaEnergy/Plug/<DEVICE_ID>/stat/POWER` / `stat/RESULT`
4. **Kontrol Relay Smart Plug**:
   - `AlfaEnergy/Plug/<DEVICE_ID>/cmnd/POWER` (Payload: `ON`, `OFF`, `TOGGLE`).

---

## 9. Catatan Temuan & Rencana Pengembangan Masa Depan (Backlog / Future Improvements)
*Bagian ini mencatat parameter dan kapabilitas tambahan dari firmware baru untuk pengembangan lanjutan:*

1. **Parameter Tambahan pada Objek `ENERGY`**:
   - **`ApparentPower` (VA)** & **`ReactivePower` (VAR)**:
     - Sudah diekstrak di backend ke `Apparent Power (kVA)` dan `Reactive Power (kVAR)`.
     - *Rencana Masa Depan:* Tambahkan visualisasi grafik kVA / kVAR pada UI dashboard jika dibutuhkan analisis faktor daya mendalam.
   - **`TodaySumImport`**, **`TodaySumExport`**, **`ExportActive`** (Khusus CT):
     - Fitur bidirectional metering dari Tasmota untuk mengukur ekspor energi (misal integrasi PLTS / Solar Rooftop di toko).
     - *Rencana Masa Depan:* Tambahkan tabel/kartu monitoring ekspor energi harian jika ada toko yang memasang solar panel.
2. **Kontrol Relay Smart Plug**:
   - Firmware baru menggunakan topik `AlfaEnergy/Plug/<DEVICE_ID>/cmnd/POWER`.
   - *Rencana Masa Depan:* Tambahkan tombol toggle relay interaktif di detail card device Smart Plug pada frontend web UI.
3. **Penanda Tipe Perangkat (Device Model Detection)**:
   - Metadata `device_type: 'CT' | 'Plug'` telah disimpan di status device `_mqtt_live_data[device_id]['status']['device_type']`.
   - *Rencana Masa Depan:* Gunakan badge/ikon pembeda di daftar perangkat (ikon CT clamp vs stopkontak Smart Plug).


