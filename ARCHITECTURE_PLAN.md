# 📐 Rencana Arsitektur Sistem Monitoring & Audit Energi (3-Pilar)

Dokumen ini mencatat hasil diskusi dan keputusan arsitektur untuk pengembangan ekosistem **Smart Energy & Audit System**.

---

## 🏗️ 1. Struktur Ekosistem (3 Proyek Terpisah)

Sistem akan terdiri dari 3 repositori/proyek independen dengan peran dan domain fungsi yang terpisah:

### 📌 Proyek 1: `sparta-energy` (Portal Audit & Strategi Energi)
* **Fungsi Utama:** Audit manual peralatan toko, manajemen cabang/toko, perbandingan standar PLN, laporan manajemen, dan AI Strategic Recommendations.
* **Tech Stack:** Next.js 16 (App Router), React 19, Tailwind CSS 4, Prisma ORM, PostgreSQL, better-auth, Recharts, Google Gemini API.
* **Target User:** Auditor, Management, Exec, Admin Operasional.

### ⚙️ Proyek 2: `Smart-Energy-Meter-Vps` (IoT Ingestion Engine & Service)
* **Fungsi Utama:** Layanan backend 24/7 untuk menerima data telemetri dari hardware sensor (ESP32) via MQTT broker, menyimpan ke PostgreSQL, dan menyediakan REST API / WebSocket data. Juga memiliki UI fallback darurat.
* **Tech Stack:** Python Flask, PostgreSQL, Paho MQTT, Docker, Render.
* **Target User:** Backend Data Service (Machine-to-Machine) & Fallback Emergency UI.

### ⚡ Proyek 3: `myeco-energy-portal` *(Akan Di-build)* (Portal Real-Time IoT ala myECO)
* **Fungsi Utama:** Dashboard operasional real-time ala myECO. Menampilkan live gauge (Voltage, Current, Power W, Power Factor), estimasi rupiah tagihan listrik live, grafik historis interaktif, sistem alert overvoltage/overcurrent, dan otomasi sakelar/relay.
* **Tech Stack (Rencana):** Next.js 16 / Vite React, Tailwind CSS, shadcn/ui, Recharts.
* **Target User:** Manajer Operasional Toko, Teknisi Lapangan, Supervisor Energi.

---

## 🔗 2. Strategi Keterkaitan Database & SSO

Meskipun fungsi frontend dipisah, data di tingkat database dan autentikasi saling terintegrasi:

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                            SHARED DATABASE LAYER                                 │
│                                                                                  │
│   ┌───────────────────────────┐                  ┌───────────────────────────┐   │
│   │    Database Telemetri     │  device_id /     │    Database Management    │   │
│   │  (Smart-Energy-Meter-Vps) │ ◄──────────────► │      (sparta-energy)      │   │
│   │  - raw_telemetry          │   store_id       │  - User & Auth (SSO)      │   │
│   │  - device_sessions        │                  │  - Stores & Branches      │   │
│   └─────────────┬─────────────┘                  │  - Audits & Equipment     │   │
│                 │                                └─────────────┬─────────────┘   │
└─────────────────┼──────────────────────────────────────────────┼─────────────────┘
                  │                                              │
                  ▼                                              ▼
┌───────────────────────────────────┐          ┌───────────────────────────────────┐
│ ⚡ PROYEK 3: Web Portal myECO      │          │ 📊 PROYEK 1: sparta-energy        │
│    (Real-Time IoT & Control)      │          │    (Audit & Energy Strategy)     │
└───────────────────────────────────┘          └───────────────────────────────────┘
```

1. **Mapping Identitas Perangkat (`device_id` ↔ `store_id`):**
   - Setiap `Store` di `sparta-energy` terhubung dengan satu atau beberapa `device_id` sensor di `Smart-Energy-Meter-Vps`.
2. **Single Sign-On (SSO):**
   - Session/Token Auth digunakan lintas portal sehingga pengguna tidak perlu login 2x saat berpindah dari Portal Audit ke Portal Real-Time Monitoring.
3. **App Switcher Navigation:**
   - Navigasi cepat di header (Quick Switcher) untuk berpindah antara *Sparta Audit Platform* dan *myECO Live Monitoring*.

---

## 📋 3. Rencana Langkah Selanjutnya (Next Session)

Saat sesi berikutnya dimulai, kita dapat langsung melangkah ke:
1. Inisialisasi folder/proyek ke-3 (misal: `myeco-energy-portal`).
2. Menentukan skema relasi `device_id` dengan `store_id` di Prisma/PostgreSQL.
3. Membuat komponen UI awal untuk dashboard real-time ala myECO.
