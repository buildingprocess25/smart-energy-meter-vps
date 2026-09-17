import pandas as pd
import numpy as np
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

def build_comparison():
    pqi_path = r'd:\Coding\Smart-Energy-Meter-Vps\3P410001.XLS'
    pzem_path = r'd:\Coding\Smart-Energy-Meter-Vps\testing comparation.xlsx'
    output_path = r'd:\Coding\Smart-Energy-Meter-Vps\Perbandingan_PQI_vs_PZEM.xlsx'

    print("Loading PQI file...")
    df_pqi_raw = pd.read_csv(pqi_path, sep='\t', encoding='latin1', skiprows=1, low_memory=False)
    df_pqi = df_pqi_raw[df_pqi_raw['Date'] != 'Date'].dropna(subset=['Date', 'Time']).copy()
    df_pqi['dt'] = pd.to_datetime(df_pqi['Date'].str.strip() + ' ' + df_pqi['Time'].str.strip(), format='%Y/%m/%d %H:%M:%S')
    df_pqi = df_pqi[df_pqi['dt'] >= '2026-09-01 16:15:00'].sort_values('dt').reset_index(drop=True)

    # Convert numeric fields
    pqi_num_cols = ['V1', 'V2', 'V3', 'V12', 'V23', 'V31', 'A1', 'A2', 'A3', 'P1', 'P2', 'P3', 'P(SUM)', 
                    'S1', 'S2', 'S3', 'S(SUM)', 'Q1', 'Q2', 'Q3', 'Q(SUM)', 'PF1', 'PF2', 'PF3', 'PF(SUM)', 
                    'FREQ', 'WH', 'SH', 'QH']
    for col in pqi_num_cols:
        if col in df_pqi.columns:
            df_pqi[col] = pd.to_numeric(df_pqi[col], errors='coerce')

    print("Loading PZEM file sheets (Phase R, S, T)...")
    pzem_sheets = {}
    for ph in ['Phase R', 'Phase S', 'Phase T']:
        df = pd.read_excel(pzem_path, sheet_name=ph)
        df['dt'] = pd.to_datetime(df['Timestamp'], format='%H:%M:%S %d/%m/%Y').sort_values().reset_index(drop=True)
        pzem_sheets[ph] = df

    # Merge each phase with PQI using merge_asof (nearest within 35s)
    matched_phases = {}
    for ph, v_col, a_col, p_col, pf_col in [
        ('Phase R', 'V1', 'A1', 'P1', 'PF1'),
        ('Phase S', 'V2', 'A2', 'P2', 'PF2'),
        ('Phase T', 'V3', 'A3', 'P3', 'PF3')
    ]:
        m = pd.merge_asof(
            pzem_sheets[ph].sort_values('dt'),
            df_pqi.sort_values('dt'),
            on='dt',
            direction='nearest',
            tolerance=pd.Timedelta('35s'),
            suffixes=('_pzem', '_pqi')
        )
        # Filter only matched records
        m = m.dropna(subset=[v_col]).copy().reset_index(drop=True)
        m['PQI_Power_W'] = m[p_col] * 1000.0  # PQI P is in kW -> convert to W
        m['Delta_V'] = m['Voltage (V)'] - m[v_col]
        m['Error_V_pct'] = (m['Delta_V'] / m[v_col]) * 100.0
        m['Delta_A'] = m['Current (A)'] - m[a_col]
        m['Error_A_pct'] = np.where(m[a_col] > 0.05, (m['Delta_A'] / m[a_col]) * 100.0, np.nan)
        m['Delta_P'] = m['Power (W)'] - m['PQI_Power_W']
        m['Error_P_pct'] = np.where(m['PQI_Power_W'] > 5.0, (m['Delta_P'] / m['PQI_Power_W']) * 100.0, np.nan)
        m['Delta_PF'] = m['Power Factor'] - m[pf_col]
        m['Delta_Freq'] = m['Frequency (Hz)'] - m['FREQ']
        matched_phases[ph] = m

    print(f"Matched rows count: Phase R={len(matched_phases['Phase R'])}, Phase S={len(matched_phases['Phase S'])}, Phase T={len(matched_phases['Phase T'])}")

    # Create combined 3-phase DataFrame
    df_r = matched_phases['Phase R']
    df_s = matched_phases['Phase S']
    df_t = matched_phases['Phase T']

    df_combined = pd.DataFrame({
        'No': range(1, len(df_r) + 1),
        'Timestamp_PZEM': df_r['Timestamp'],
        'Timestamp_PQI': df_r['Date'].str.strip() + ' ' + df_r['Time'].str.strip(),
        # Voltage R, S, T
        'PZEM_V_R': df_r['Voltage (V)'],
        'PQI_V_R': df_r['V1'],
        'Delta_V_R': df_r['Delta_V'],
        'PZEM_V_S': df_s['Voltage (V)'],
        'PQI_V_S': df_s['V2'],
        'Delta_V_S': df_s['Delta_V'],
        'PZEM_V_T': df_t['Voltage (V)'],
        'PQI_V_T': df_t['V3'],
        'Delta_V_T': df_t['Delta_V'],
        # Current R, S, T
        'PZEM_I_R': df_r['Current (A)'],
        'PQI_I_R': df_r['A1'],
        'Delta_I_R': df_r['Delta_A'],
        'PZEM_I_S': df_s['Current (A)'],
        'PQI_I_S': df_s['A2'],
        'Delta_I_S': df_s['Delta_A'],
        'PZEM_I_T': df_t['Current (A)'],
        'PQI_I_T': df_t['A3'],
        'Delta_I_T': df_t['Delta_A'],
        # Active Power R, S, T
        'PZEM_P_R': df_r['Power (W)'],
        'PQI_P_R': df_r['PQI_Power_W'],
        'Delta_P_R': df_r['Delta_P'],
        'PZEM_P_S': df_s['Power (W)'],
        'PQI_P_S': df_s['PQI_Power_W'],
        'Delta_P_S': df_s['Delta_P'],
        'PZEM_P_T': df_t['Power (W)'],
        'PQI_P_T': df_t['PQI_Power_W'],
        'Delta_P_T': df_t['Delta_P'],
        # Total Power (W)
        'PZEM_Total_P': df_r['Power (W)'] + df_s['Power (W)'] + df_t['Power (W)'],
        'PQI_Total_P': df_r['P(SUM)'] * 1000.0,
        'Delta_Total_P': (df_r['Power (W)'] + df_s['Power (W)'] + df_t['Power (W)']) - (df_r['P(SUM)'] * 1000.0),
        # Power Factor
        'PZEM_PF_R': df_r['Power Factor'],
        'PQI_PF_R': df_r['PF1'],
        'PZEM_PF_S': df_s['Power Factor'],
        'PQI_PF_S': df_s['PF2'],
        'PZEM_PF_T': df_t['Power Factor'],
        'PQI_PF_T': df_t['PF3'],
        'PQI_PF_SUM': df_r['PF(SUM)'],
        # Frequency
        'PZEM_Freq': df_r['Frequency (Hz)'],
        'PQI_Freq': df_r['FREQ'],
        # Cumulative Energy (kWh)
        'PZEM_Energy_R_kWh': df_r['Active Energy (kWh)'],
        'PZEM_Energy_S_kWh': df_s['Active Energy (kWh)'],
        'PZEM_Energy_T_kWh': df_t['Active Energy (kWh)'],
        'PZEM_Total_Energy_kWh': df_r['Active Energy (kWh)'] + df_s['Active Energy (kWh)'] + df_t['Active Energy (kWh)'],
        'PQI_WH_kWh': df_r['WH']
    })

    # Create Workbook
    wb = openpyxl.Workbook()
    # default sheet
    ws_summary = wb.active
    ws_summary.title = "Ringkasan & Statistik"

    # Styles
    font_title = Font(name="Segoe UI", size=16, bold=True, color="1E3A8A")
    font_subtitle = Font(name="Segoe UI", size=11, italic=True, color="475569")
    font_section = Font(name="Segoe UI", size=12, bold=True, color="0F172A")
    font_header = Font(name="Segoe UI", size=10, bold=True, color="FFFFFF")
    font_subheader = Font(name="Segoe UI", size=10, bold=True, color="1E293B")
    font_body = Font(name="Segoe UI", size=10, color="1E293B")
    font_bold = Font(name="Segoe UI", size=10, bold=True, color="1E293B")

    fill_navy = PatternFill(start_color="1E3A8A", end_color="1E3A8A", fill_type="solid")
    fill_blue_header = PatternFill(start_color="2563EB", end_color="2563EB", fill_type="solid")
    fill_sub_blue = PatternFill(start_color="DBEAFE", end_color="DBEAFE", fill_type="solid")
    fill_sub_slate = PatternFill(start_color="F1F5F9", end_color="F1F5F9", fill_type="solid")
    fill_zebra = PatternFill(start_color="F8FAFC", end_color="F8FAFC", fill_type="solid")

    thin_border = Border(
        left=Side(style='thin', color='CBD5E1'),
        right=Side(style='thin', color='CBD5E1'),
        top=Side(style='thin', color='CBD5E1'),
        bottom=Side(style='thin', color='CBD5E1')
    )

    # ----------------------------------------------------
    # 1. SUMMARY SHEET
    # ----------------------------------------------------
    ws_summary.views.sheetView[0].showGridLines = True
    
    ws_summary['B2'] = "LAPORAN PERBANDINGAN SENSOR PZEM IOT VS DATA LOGGER PQI"
    ws_summary['B2'].font = font_title
    ws_summary['B3'] = "Pengujian Parameter 3-Fasa (R, S, T) dengan Interval Waktu & Jumlah Data Seimbang"
    ws_summary['B3'].font = font_subtitle

    # Metadata Table
    metadata = [
        ("Parameter Pengujian", "Informasi"),
        ("Sumber Data 1 (Reference)", "Data Logger PQI (Power Quality Instrument) - File: 3P410001.XLS"),
        ("Sumber Data 2 (Under Test)", "Sensor PZEM IoT - File: testing comparation.xlsx"),
        ("Periode Sinkronisasi", f"{df_combined['Timestamp_PZEM'].iloc[0]}  s/d  {df_combined['Timestamp_PZEM'].iloc[-1]}"),
        ("Interval Pengambilan Data", "1 Menit (Relative Time Diff ~5 Detik sinkron 1:1)"),
        ("Jumlah Data Teruji (N)", f"{len(df_combined):,} baris data seimbang (100% matched)"),
        ("Fasa yang Diuji", "Fasa R, Fasa S, Fasa T, dan Total 3-Fasa"),
    ]

    for r_idx, (k, v) in enumerate(metadata, start=5):
        ws_summary.cell(row=r_idx, column=2, value=k).font = font_bold if r_idx == 5 else font_body
        ws_summary.cell(row=r_idx, column=3, value=v).font = font_bold if r_idx == 5 else font_body
        if r_idx == 5:
            ws_summary.cell(row=r_idx, column=2).fill = fill_sub_blue
            ws_summary.cell(row=r_idx, column=3).fill = fill_sub_blue
        else:
            ws_summary.cell(row=r_idx, column=2).fill = fill_sub_slate
        ws_summary.cell(row=r_idx, column=2).border = thin_border
        ws_summary.cell(row=r_idx, column=3).border = thin_border

    # Statistics Summary Table
    ws_summary['B14'] = "RINGKASAN STATISTIK & AKURASI PARAMETER PER FASA"
    ws_summary['B14'].font = font_section

    stats_headers = [
        "Fasa", "Parameter", "Satuan",
        "PZEM (Rata-rata)", "PQI (Rata-rata)", "Selisih Rata-rata (PZEM-PQI)",
        "MAE (Selisih Mutlak)", "MAPE / Rata-rata Error (%)",
        "PZEM Min", "PZEM Max", "PQI Min", "PQI Max", "Korelasi (R²)"
    ]

    for col_idx, h in enumerate(stats_headers, start=2):
        cell = ws_summary.cell(row=16, column=col_idx, value=h)
        cell.font = font_header
        cell.fill = fill_blue_header
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = thin_border
    ws_summary.row_dimensions[16].height = 28

    # Calculate stats for all parameters
    def calc_stats(series_pzem, series_pqi, min_valid_ref=0.05):
        mask = series_pzem.notna() & series_pqi.notna()
        pzem = series_pzem[mask]
        pqi = series_pqi[mask]
        mean_pzem = pzem.mean()
        mean_pqi = pqi.mean()
        diff = (pzem - pqi).mean()
        mae = (pzem - pqi).abs().mean()
        
        # MAPE calculated on non-idle / active readings
        valid_mask = pqi.abs() >= min_valid_ref
        if valid_mask.sum() > 0:
            mape = (np.abs((pzem[valid_mask] - pqi[valid_mask]) / pqi[valid_mask]) * 100.0).mean()
        else:
            mape = 0.0

        min_pzem = pzem.min()
        max_pzem = pzem.max()
        min_pqi = pqi.min()
        max_pqi = pqi.max()
        corr = np.corrcoef(pzem, pqi)[0, 1] ** 2 if len(pzem) > 1 and pzem.std() > 0 and pqi.std() > 0 else np.nan
        return mean_pzem, mean_pqi, diff, mae, mape, min_pzem, max_pzem, min_pqi, max_pqi, corr

    stats_rows = [
        # Phase R
        ("Phase R", "Tegangan (Voltage)", "V", *calc_stats(df_combined['PZEM_V_R'], df_combined['PQI_V_R'], min_valid_ref=50.0)),
        ("Phase R", "Arus (Current)", "A", *calc_stats(df_combined['PZEM_I_R'], df_combined['PQI_I_R'], min_valid_ref=0.1)),
        ("Phase R", "Daya Aktif (Active Power)", "W", *calc_stats(df_combined['PZEM_P_R'], df_combined['PQI_P_R'], min_valid_ref=5.0)),
        ("Phase R", "Power Factor", "-", *calc_stats(df_combined['PZEM_PF_R'], df_combined['PQI_PF_R'], min_valid_ref=0.1)),
        ("Phase R", "Frekuensi", "Hz", *calc_stats(df_combined['PZEM_Freq'], df_combined['PQI_Freq'], min_valid_ref=10.0)),
        # Phase S
        ("Phase S", "Tegangan (Voltage)", "V", *calc_stats(df_combined['PZEM_V_S'], df_combined['PQI_V_S'], min_valid_ref=50.0)),
        ("Phase S", "Arus (Current)", "A", *calc_stats(df_combined['PZEM_I_S'], df_combined['PQI_I_S'], min_valid_ref=0.1)),
        ("Phase S", "Daya Aktif (Active Power)", "W", *calc_stats(df_combined['PZEM_P_S'], df_combined['PQI_P_S'], min_valid_ref=5.0)),
        ("Phase S", "Power Factor", "-", *calc_stats(df_combined['PZEM_PF_S'], df_combined['PQI_PF_S'], min_valid_ref=0.1)),
        ("Phase S", "Frekuensi", "Hz", *calc_stats(df_combined['PZEM_Freq'], df_combined['PQI_Freq'], min_valid_ref=10.0)),
        # Phase T
        ("Phase T", "Tegangan (Voltage)", "V", *calc_stats(df_combined['PZEM_V_T'], df_combined['PQI_V_T'], min_valid_ref=50.0)),
        ("Phase T", "Arus (Current)", "A", *calc_stats(df_combined['PZEM_I_T'], df_combined['PQI_I_T'], min_valid_ref=0.1)),
        ("Phase T", "Daya Aktif (Active Power)", "W", *calc_stats(df_combined['PZEM_P_T'], df_combined['PQI_P_T'], min_valid_ref=5.0)),
        ("Phase T", "Power Factor", "-", *calc_stats(df_combined['PZEM_PF_T'], df_combined['PQI_PF_T'], min_valid_ref=0.1)),
        ("Phase T", "Frekuensi", "Hz", *calc_stats(df_combined['PZEM_Freq'], df_combined['PQI_Freq'], min_valid_ref=10.0)),
        # Total
        ("Total 3P", "Total Daya Aktif (P SUM)", "W", *calc_stats(df_combined['PZEM_Total_P'], df_combined['PQI_Total_P'], min_valid_ref=10.0)),
    ]

    for r_idx, row_data in enumerate(stats_rows, start=17):
        for c_idx, val in enumerate(row_data, start=2):
            cell = ws_summary.cell(row=r_idx, column=c_idx)
            cell.border = thin_border
            cell.font = font_body
            if r_idx % 2 == 0:
                cell.fill = fill_zebra
            
            # Format numbers
            if c_idx in [2, 3]:  # Text
                cell.value = val
                cell.alignment = Alignment(horizontal="left", vertical="center")
            elif c_idx == 4:  # Unit
                cell.value = val
                cell.alignment = Alignment(horizontal="center", vertical="center")
            elif c_idx in [5, 6, 7, 8, 10, 11, 12, 13]:  # Numbers
                cell.value = round(val, 3) if isinstance(val, (int, float, np.number)) else val
                cell.alignment = Alignment(horizontal="right", vertical="center")
                cell.number_format = "#,##0.00"
            elif c_idx == 9:  # MAPE %
                cell.value = round(val / 100.0, 4) if isinstance(val, (int, float, np.number)) else val
                cell.alignment = Alignment(horizontal="right", vertical="center")
                cell.number_format = "0.00%"
            elif c_idx == 14:  # R²
                cell.value = round(val, 4) if isinstance(val, (int, float, np.number)) else val
                cell.alignment = Alignment(horizontal="right", vertical="center")
                cell.number_format = "0.0000"

    # Energy Summary Box
    e_start_row = 17 + len(stats_rows) + 2
    ws_summary.cell(row=e_start_row, column=2, value="PERBANDINGAN KONSUMSI ENERGI (ACTIVE ENERGY)").font = font_section

    energy_data = [
        ("Metrik Energi", "PZEM (kWh)", "PQI (kWh)", "Selisih (kWh)", "Deviasi (%)"),
        ("Energi Terakumulasi Fasa R", df_r['Active Energy (kWh)'].iloc[-1] - df_r['Active Energy (kWh)'].iloc[0], "-", "-", "-"),
        ("Energi Terakumulasi Fasa S", df_s['Active Energy (kWh)'].iloc[-1] - df_s['Active Energy (kWh)'].iloc[0], "-", "-", "-"),
        ("Energi Terakumulasi Fasa T", df_t['Active Energy (kWh)'].iloc[-1] - df_t['Active Energy (kWh)'].iloc[0], "-", "-", "-"),
        ("Total Energi 3-Fasa (WH)", 
         (df_r['Active Energy (kWh)'].iloc[-1] - df_r['Active Energy (kWh)'].iloc[0]) + 
         (df_s['Active Energy (kWh)'].iloc[-1] - df_s['Active Energy (kWh)'].iloc[0]) + 
         (df_t['Active Energy (kWh)'].iloc[-1] - df_t['Active Energy (kWh)'].iloc[0]),
         df_r['WH'].iloc[-1] - df_r['WH'].iloc[0],
         ((df_r['Active Energy (kWh)'].iloc[-1] - df_r['Active Energy (kWh)'].iloc[0]) + 
          (df_s['Active Energy (kWh)'].iloc[-1] - df_s['Active Energy (kWh)'].iloc[0]) + 
          (df_t['Active Energy (kWh)'].iloc[-1] - df_t['Active Energy (kWh)'].iloc[0])) - (df_r['WH'].iloc[-1] - df_r['WH'].iloc[0]),
         (((df_r['Active Energy (kWh)'].iloc[-1] - df_r['Active Energy (kWh)'].iloc[0]) + 
           (df_s['Active Energy (kWh)'].iloc[-1] - df_s['Active Energy (kWh)'].iloc[0]) + 
           (df_t['Active Energy (kWh)'].iloc[-1] - df_t['Active Energy (kWh)'].iloc[0]) - (df_r['WH'].iloc[-1] - df_r['WH'].iloc[0])) / (df_r['WH'].iloc[-1] - df_r['WH'].iloc[0])) * 100.0
        )
    ]

    for r_offset, row_d in enumerate(energy_data):
        curr_row = e_start_row + 2 + r_offset
        for c_offset, val in enumerate(row_d):
            cell = ws_summary.cell(row=curr_row, column=2 + c_offset)
            cell.border = thin_border
            if r_offset == 0:
                cell.value = val
                cell.font = font_header
                cell.fill = fill_blue_header
                cell.alignment = Alignment(horizontal="center", vertical="center")
            else:
                cell.font = font_bold if r_offset == 4 else font_body
                if r_offset == 4:
                    cell.fill = fill_sub_blue
                elif r_offset % 2 == 1:
                    cell.fill = fill_zebra
                
                if isinstance(val, (int, float, np.number)):
                    if c_offset == 4:
                        cell.value = round(val / 100.0, 4)
                        cell.number_format = "0.00%"
                    else:
                        cell.value = round(val, 3)
                        cell.number_format = "#,##0.000"
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                else:
                    cell.value = val
                    cell.alignment = Alignment(horizontal="left" if c_offset == 0 else "center", vertical="center")

    # ----------------------------------------------------
    # 2. HELPER TO POPULATE INDIVIDUAL PHASE SHEETS
    # ----------------------------------------------------
    def create_phase_sheet(ws, phase_name, df_matched, v_key, a_key, p_key, pf_key):
        ws.views.sheetView[0].showGridLines = True
        
        # Title
        ws['A1'] = f"DATA PERBANDINGAN SENSOR PZEM VS DATA LOGGER PQI - {phase_name.upper()}"
        ws['A1'].font = font_title
        ws['A2'] = f"Interval 1 Menit | Total {len(df_matched):,} Data Synchronized Side-by-Side"
        ws['A2'].font = font_subtitle

        # Super Headers (Grouped)
        super_headers = [
            ("Waktu / Timestamp", 1, 3, fill_navy),
            ("Tegangan / Voltage (V)", 4, 7, fill_blue_header),
            ("Arus / Current (A)", 8, 11, fill_navy),
            ("Daya Aktif / Power (W)", 12, 15, fill_blue_header),
            ("Power Factor (PF)", 16, 18, fill_navy),
            ("Frekuensi (Hz)", 19, 21, fill_blue_header),
            ("Energi Aktif (kWh)", 22, 22, fill_navy)
        ]

        for title, start_c, end_c, fill in super_headers:
            if start_c != end_c:
                ws.merge_cells(start_row=4, start_column=start_c, end_row=4, end_column=end_c)
            cell = ws.cell(row=4, column=start_c, value=title)
            cell.font = font_header
            cell.fill = fill
            cell.alignment = Alignment(horizontal="center", vertical="center")
            for c in range(start_c, end_c + 1):
                ws.cell(row=4, column=c).border = thin_border
                ws.cell(row=4, column=c).fill = fill

        headers = [
            "No", "Timestamp PZEM", "Timestamp PQI",
            "PZEM (V)", f"PQI {v_key} (V)", "Selisih (V)", "Error (%)",
            "PZEM (A)", f"PQI {a_key} (A)", "Selisih (A)", "Error (%)",
            "PZEM (W)", f"PQI {p_key} (W)", "Selisih (W)", "Error (%)",
            "PZEM PF", f"PQI {pf_key}", "Selisih PF",
            "PZEM (Hz)", "PQI FREQ (Hz)", "Selisih (Hz)",
            "PZEM Energy (kWh)"
        ]

        ws.row_dimensions[5].height = 24
        for c_idx, h in enumerate(headers, start=1):
            cell = ws.cell(row=5, column=c_idx, value=h)
            cell.font = font_subheader
            cell.fill = fill_sub_blue
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = thin_border

        # Populate Data
        for r_idx, row in df_matched.iterrows():
            row_num = 6 + r_idx
            pqi_ts_str = f"{str(row['Date']).strip()} {str(row['Time']).strip()}"
            
            v_pzem = row['Voltage (V)']
            v_pqi = row[v_key]
            d_v = row['Delta_V']
            err_v = row['Error_V_pct'] / 100.0 if pd.notna(row['Error_V_pct']) else None

            a_pzem = row['Current (A)']
            a_pqi = row[a_key]
            d_a = row['Delta_A']
            err_a = row['Error_A_pct'] / 100.0 if pd.notna(row['Error_A_pct']) else None

            p_pzem = row['Power (W)']
            p_pqi = row['PQI_Power_W']
            d_p = row['Delta_P']
            err_p = row['Error_P_pct'] / 100.0 if pd.notna(row['Error_P_pct']) else None

            pf_pzem = row['Power Factor']
            pf_pqi = row[pf_key]
            d_pf = row['Delta_PF']

            freq_pzem = row['Frequency (Hz)']
            freq_pqi = row['FREQ']
            d_freq = row['Delta_Freq']

            e_pzem = row['Active Energy (kWh)']

            row_values = [
                r_idx + 1, row['Timestamp'], pqi_ts_str,
                v_pzem, v_pqi, d_v, err_v,
                a_pzem, a_pqi, d_a, err_a,
                p_pzem, p_pqi, d_p, err_p,
                pf_pzem, pf_pqi, d_pf,
                freq_pzem, freq_pqi, d_freq,
                e_pzem
            ]

            for c_idx, val in enumerate(row_values, start=1):
                cell = ws.cell(row=row_num, column=c_idx, value=val)
                cell.font = font_body
                cell.border = thin_border
                if r_idx % 2 == 1:
                    cell.fill = fill_zebra

                # Format specific columns
                if c_idx == 1:
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                elif c_idx in [2, 3]:
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                elif c_idx in [4, 5, 6]:  # Voltage
                    cell.number_format = "#,##0.0"
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                elif c_idx in [7, 11, 15]:  # Percentage error
                    cell.number_format = "0.00%"
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                elif c_idx in [8, 9, 10]:  # Current
                    cell.number_format = "#,##0.000"
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                elif c_idx in [12, 13, 14]:  # Power
                    cell.number_format = "#,##0.0"
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                elif c_idx in [16, 17, 18]:  # PF
                    cell.number_format = "0.00"
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                elif c_idx in [19, 20, 21]:  # Frequency
                    cell.number_format = "#,##0.0"
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                elif c_idx == 22:  # Energy
                    cell.number_format = "#,##0.000"
                    cell.alignment = Alignment(horizontal="right", vertical="center")

        ws.freeze_panes = "D6"

    # Create Phase R Sheet
    ws_r = wb.create_sheet(title="Phase R")
    create_phase_sheet(ws_r, "Phase R", matched_phases['Phase R'], 'V1', 'A1', 'P1', 'PF1')

    # Create Phase S Sheet
    ws_s = wb.create_sheet(title="Phase S")
    create_phase_sheet(ws_s, "Phase S", matched_phases['Phase S'], 'V2', 'A2', 'P2', 'PF2')

    # Create Phase T Sheet
    ws_t = wb.create_sheet(title="Phase T")
    create_phase_sheet(ws_t, "Phase T", matched_phases['Phase T'], 'V3', 'A3', 'P3', 'PF3')

    # ----------------------------------------------------
    # 5. COMBINED 3-PHASE SHEET
    # ----------------------------------------------------
    ws_all = wb.create_sheet(title="Semua Fasa (3-Phase Combined)")
    ws_all.views.sheetView[0].showGridLines = True
    
    ws_all['A1'] = "DATA PERBANDINGAN LENGKAP 3-FASA (PZEM IOT VS PQI DATA LOGGER)"
    ws_all['A1'].font = font_title
    ws_all['A2'] = "Tabel Gabungan Semua Fasa R, S, T, Total Daya, Power Factor, dan Energi Aktif"
    ws_all['A2'].font = font_subtitle

    # Super Headers
    all_super_headers = [
        ("Timestamp", 1, 3, fill_navy),
        ("Tegangan Fasa R (V)", 4, 6, fill_blue_header),
        ("Tegangan Fasa S (V)", 7, 9, fill_navy),
        ("Tegangan Fasa T (V)", 10, 12, fill_blue_header),
        ("Arus Fasa R (A)", 13, 15, fill_navy),
        ("Arus Fasa S (A)", 16, 18, fill_blue_header),
        ("Arus Fasa T (A)", 19, 21, fill_navy),
        ("Daya Fasa R (W)", 22, 24, fill_blue_header),
        ("Daya Fasa S (W)", 25, 27, fill_navy),
        ("Daya Fasa T (W)", 28, 30, fill_blue_header),
        ("Total Daya 3-Fasa (W)", 31, 33, fill_navy),
        ("Power Factor R/S/T", 34, 40, fill_blue_header),
        ("Frekuensi (Hz)", 41, 42, fill_navy),
        ("Energi Aktif Kumulatif (kWh)", 43, 47, fill_blue_header)
    ]

    for title, start_c, end_c, fill in all_super_headers:
        if start_c != end_c:
            ws_all.merge_cells(start_row=4, start_column=start_c, end_row=4, end_column=end_c)
        cell = ws_all.cell(row=4, column=start_c, value=title)
        cell.font = font_header
        cell.fill = fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
        for c in range(start_c, end_c + 1):
            ws_all.cell(row=4, column=c).border = thin_border
            ws_all.cell(row=4, column=c).fill = fill

    all_cols = list(df_combined.columns)
    ws_all.row_dimensions[5].height = 24
    for c_idx, col_name in enumerate(all_cols, start=1):
        cell = ws_all.cell(row=5, column=c_idx, value=col_name)
        cell.font = font_subheader
        cell.fill = fill_sub_blue
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = thin_border

    # Populate df_combined
    for r_idx, row in df_combined.iterrows():
        row_num = 6 + r_idx
        for c_idx, col_name in enumerate(all_cols, start=1):
            val = row[col_name]
            cell = ws_all.cell(row=row_num, column=c_idx, value=val)
            cell.font = font_body
            cell.border = thin_border
            if r_idx % 2 == 1:
                cell.fill = fill_zebra

            if c_idx == 1:
                cell.alignment = Alignment(horizontal="center", vertical="center")
            elif c_idx in [2, 3]:
                cell.alignment = Alignment(horizontal="center", vertical="center")
            elif isinstance(val, (int, float, np.number)):
                cell.alignment = Alignment(horizontal="right", vertical="center")
                if 'Energy' in col_name or 'WH' in col_name or 'I_' in col_name:
                    cell.number_format = "#,##0.000"
                elif 'PF' in col_name:
                    cell.number_format = "0.00"
                else:
                    cell.number_format = "#,##0.0"

    ws_all.freeze_panes = "D6"

    # Auto-adjust column widths for all sheets
    for ws in [ws_summary, ws_r, ws_s, ws_t, ws_all]:
        for col in ws.columns:
            max_len = 0
            col_letter = get_column_letter(col[0].column)
            # Sample first 40 rows for speed
            for cell in col[:40]:
                if cell.value:
                    lines = str(cell.value).split('\n')
                    for line in lines:
                        if len(str(line)) > max_len:
                            max_len = len(str(line))
            ws.column_dimensions[col_letter].width = max(max_len + 3, 10)

    # Specific padding for summary
    ws_summary.column_dimensions['B'].width = 30
    ws_summary.column_dimensions['C'].width = 32
    for c in range(4, 15):
        ws_summary.column_dimensions[get_column_letter(c)].width = 16

    print(f"Saving workbook to {output_path}...")
    wb.save(output_path)
    print("Successfully generated comparison Excel file!")

if __name__ == '__main__':
    build_comparison()
