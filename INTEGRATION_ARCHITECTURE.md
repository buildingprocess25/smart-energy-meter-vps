# 📐 Arsitektur Integrasi Ekosistem Energi (3-Pilar)

Dokumen ini mendokumentasikan spesifikasi arsitektur integrasi antara **Smart Energy Meter VPS**, **Sparta Energy**, dan **Smart Energy Monitoring**.

---

## 🏗️ 1. Struktur Ekosistem & Pembagian Domain

Ekosistem pengelolaan energi terdiri dari 3 proyek yang saling terintegrasi:

| Proyek | Domain & Peran Utama | Tech Stack | Database Target |
| :--- | :--- | :--- | :--- |
| **`sparta-energy`** | **Master Toko & Audit Manual**<br>Manajemen toko/cabang, audit manual peralatan, standar PLN, rekomendasi AI. | Next.js 16, Prisma ORM, Better-Auth, PostgreSQL | **DB 1: `sparta_energy`** *(Master Toko, User, Audits)* |
| **`smart-energy-meter-vps`** | **IoT Ingestion Engine & Tool Operasional**<br>Penerimaan data MQTT ESP32, penyimpanan log telemetri, manajemen penugasan toko per device, rekaman sesi (*Capture*). | Python Flask, PostgreSQL, Paho MQTT, Chart.js, HTML5 | **DB 2: `energy_meter`** *(Devices, Telemetry, History)*<br>+ Read-only ke **DB 1** |
| **`smart-energy-monitoring`** | **Portal Visualisasi & Dashboard Monitoring**<br>Visualisasi live gauge 3-fase, peta interaktif sebaran alat IoT (Live/Historical), grafik tren daya & kWh per toko. | Next.js 16, React 19, Tailwind CSS 4, Recharts, Leaflet/Map | Mengonsumsi **DB 1** (Toko & Peta) & **DB 2** (Telemetri IoT) |

---

## 🗄️ 2. Arsitektur Dual Database (Isolasi & Reliabilitas)

Dua database dipisahkan untuk menjaga integritas data bisnis dari beban jutaan data log IoT mentah:

```
┌──────────────────────────────────────────────┐       ┌──────────────────────────────────────────────┐
│        🏢 DATABASE 1: `sparta_energy`        │       │        ⚡ DATABASE 2: `energy_meter`         │
│  - Tabel `stores`                            │       │  - Tabel `devices`                           │
│    (id, code, name, branch,                  │       │    (id, name, online, last_seen,             │
│     latitude, longitude, daya_va, area)      │       │     store_id, store_code, lat, lng)          │
│  - Tabel `users`, `accounts`, `sessions`     │       │  - Tabel `telemetry` (snapshot 15 menit)     │
│  - Tabel `audits`, `audit_items`             │       │  - Tabel `history` (rekaman sesi capture)    │
└──────────────────────┬───────────────────────┘       └──────────────────────┬───────────────────────┘
                       │                                                      │
         (1) Query Master Toko (Read-Only)                       (2) Simpan Data IoT + Metadata Toko
                       │                                                      │
                       ▼                                                      ▼
              ┌────────────────────────────────────────────────────────────────────────┐
              │                     ⚙️ `smart-energy-meter-vps`                        │
              │  - Tab Settings ➔ Manage Devices: Dropdown cari Toko dari DB 1         │
              │  - Modal Start Capture: Terikat ke ID Toko & Koordinat GPS             │
              └───────────────────────────────────┬────────────────────────────────────┘
                                                  │
                                                  ▼
              ┌────────────────────────────────────────────────────────────────────────┐
              │                     📊 `smart-energy-monitoring`                       │
              │  - Sinkronisasi Peta & Status Toko:                                    │
              │    🟢 LIVE : Perangkat IoT aktif merekam di toko tersebut              │
              │    🔵 HISTORICAL : Toko memiliki riwayat rekaman audit IoT             │
              │    ⚪ UNASSIGNED : Toko belum pernah dipasang IoT                      │
              │  - Klik Pin Toko di Peta ➔ Langsung buka grafik daya & riwayat audit   │
              └────────────────────────────────────────────────────────────────────────┘
```

---

## ⚙️ 3. Implementasi Teknis di `smart-energy-meter-vps`

### A. Environment Variables (`.env`)
```env
# Database Utama IoT (Tulis & Baca Log Sensor)
DATABASE_URL="postgresql://username:password@localhost:5432/energy_meter"

# Database Sparta Energy (Hanya Baca Master Data Toko)
SPARTA_DATABASE_URL="postgresql://username:password@localhost:5432/sparta_energy"
```

### B. Backend Flask (`app.py`)
1. **Connection Pool Sparta (Read-Only):**
   Membuat pool koneksi sekunder ke `SPARTA_DATABASE_URL`.
2. **Endpoint API Master Toko:**
   - `GET /api/stores?q=...`
   - Mengambil `id`, `code`, `name`, `branch`, `latitude`, `longitude` dari tabel `stores` di DB Sparta untuk autocomplete dropdown.
3. **Penyimpanan Metadata Toko di Tabel `devices` & `history`:**
   Menambahkan kolom:
   - `store_id (UUID/VARCHAR)`
   - `store_code (VARCHAR)`
   - `store_name (VARCHAR)`
   - `latitude (FLOAT)`
   - `longitude (FLOAT)`

### C. Frontend UI (`frontend/index.html` & `frontend/app.js`)
1. **Tab Settings ➔ Manage Devices:**
   - Pada input edit nama device (yang sebelumnya teks bebas placeholder `"Nama Lokasi / Toko..."`), diganti menjadi **Searchable Dropdown (Combobox Toko)**.
   - User mengetik nama/kode toko ➔ memilih toko dari list ➔ device langsung terasosiasi dengan `store_id` dan koordinat toko tersebut.
2. **Modal Start Capture (Mulai Rekam):**
   - Otomatis mengambil identitas toko yang sedang aktif pada device terpilih.
   - Sesi rekaman di tabel `history` otomatis tersimpan dengan ID toko dan koordinatnya.

---

## 🎯 4. Dampak ke `smart-energy-monitoring`

1. **Sinkronisasi Peta (Leaflet Map):**
   - Menampilkan seluruh toko dari DB Sparta berdasarkan koordinat `latitude` & `longitude`.
   - Memberikan badge status:
     - 🟢 **LIVE:** Jika device IoT sedang aktif merekam di toko tersebut (`online = true` dan ada sesi rekaman berjalan).
     - 🔵 **HISTORICAL:** Jika toko memiliki riwayat sesi rekaman di tabel `history`.
     - ⚪ **UNASSIGNED:** Belum pernah diaudit IoT.
2. **Detail Monitoring Per Toko (`/monitoring/[storeId]`):**
   - Menampilkan Circular Gauge daya 3-fase (L1, L2, L3) secara live.
   - Menampilkan riwayat sesi audit IoT lampau untuk perbandingan efisiensi beban listrik toko.
