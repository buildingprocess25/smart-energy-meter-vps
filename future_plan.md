# 📋 Future Plan & Roadmap: Smart Energy Meter

Dokumen ini berisi rencana pengembangan jangka menengah dan panjang untuk sistem **Smart Energy Meter**. Rencana ini dirancang untuk mengatasi potensi pembengkakan database, mempermudah manajemen arsip data, dan memberikan wawasan analitik yang lebih mendalam bagi pengguna.

---

## 🚀 Rencana Fitur & Inovasi

### 1. Ekspor Data Fleksibel (JSON & CSV Backup)
Menambahkan opsi ekspor data tambahan di tab **History** selain format Excel (.xlsx) saat ini:
- **Tujuan:** Menyediakan berkas data mentah yang ringan, terstandarisasi, dan mudah dibaca oleh mesin/aplikasi lain.
- **Implementasi:**
  - Tombol **"Download JSON Backup"** untuk mencadangkan seluruh data telemetri sesi terpilih ke dalam satu berkas teks JSON terstruktur.
  - Tombol **"Download Excel"** dipertahankan khusus untuk kebutuhan pembuatan laporan manusia.

---

### 2. Integrasi Backup Google Drive (Tanpa Hapus Otomatis)
Menghubungkan Flask backend dengan Google Drive API untuk mengarsipkan data sesi yang sudah lama secara eksternal.
- **Tujuan:** Menyelamatkan penyimpanan database PostgreSQL lokal/produksi dari kepenuhan.
- **Implementasi:**
  - Integrasi API menggunakan Service Account / OAuth Google Cloud.
  - Sesi yang dipilih akan dikirim ke folder Google Drive khusus berupa berkas JSON/CSV terkompresi.
  - **Prinsip Keamanan:** Proses backup **tidak akan menghapus data lokal secara otomatis**. Setelah backup dinyatakan sukses oleh Google API, pengguna akan diberikan tombol manual **"Hapus Data Lokal"** dengan konfirmasi ganda untuk keamanan data optimal.

---

### 3. Visualisasi Offline (Offline Session Viewer)
Membuat fitur pengunggah berkas cadangan (*Uploader*) pada tab **History**.
- **Tujuan:** Pengguna dapat melihat grafik visualisasi interaktif dari data lama yang sudah dihapus di database lokal hanya dengan mengunggah kembali file JSON/CSV cadangan.
- **Implementasi:**
  - Menyediakan area drag-and-drop di browser.
  - JavaScript di sisi klien akan membaca file backup secara lokal (menggunakan HTML5 File API).
  - Data langsung diumpankan ke Chart.js dan tabel parameter seketika itu juga tanpa menulis apa pun ke database server.

---

### 4. Analisis Data & Wawasan Listrik (Advanced Analytics)
Menambahkan modul kalkulasi statistik pintar yang dijalankan di backend/visualizer:
- **Voltage Drop & Surge Alert:** Menandai waktu-waktu di mana tegangan listrik tidak stabil (misalnya di bawah 200V atau di atas 240V).
- **Peak Load Analysis:** Menganalisis waktu penggunaan daya maksimum (*Power Peak*) beserta rata-ratanya untuk menentukan beban puncak operasional.
- **Cost Estimation:** Menghitung perkiraan rupiah tagihan listrik berdasarkan akumulasi kWh dikalikan tarif PLN yang berlaku di lokasi tersebut.

---

## 🛠️ Pilihan Garapan Sekarang

Berikut adalah opsi fitur yang siap dikerjakan sekarang berdasarkan kesiapan sistem:

### Opsi A: Visualisasi Offline & Uploader (Rekomendasi Utama) - [SELESAI]
- **Detail:** Membuat halaman/tab "Tools" khusus dengan drag-and-drop uploader untuk berkas Excel (.xlsx) multisheet dan JSON cadangan, mem-parse berkas di sisi klien via SheetJS, dan merender grafik multi-sensor perbandingan parameter.

### Opsi B: JSON Backup & Tombol Manual Clean - [SELESAI]
- **Detail:** Menyediakan menu "Backup JSON" pada dropdown tindakan sesi dan memperjelas tombol ekspor laporan Excel utama.

### Opsi C: Google Drive API Setup
- **Detail:** Mulai memasang library python google client pada backend Flask dan mempersiapkan endpoint `/api/backup/gdrive` untuk mengunggah data.


