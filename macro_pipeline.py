import pandas as pd
import yfinance as yf
import requests
import os
from datetime import datetime, timedelta

# =====================================
# 1. CONFIGURACIÓN
# =====================================
BMX_TOKEN = "8c96d3ef981e1ca6847ad7c17802fb95f732b3625457f93932f62133b538b658"
FECHA_INICIO = "2025-01-01" 
FECHA_FIN = datetime.today().strftime("%Y-%m-%d")

bmx_series = {
    "SF43718": "TC_Dolar_FIX",
    "SF46410": "TC_Euro",
    "SP68257": "Inflacion_Mensual_Pct"
}

yahoo_tickers = {
    "CL=F": "Mezcla_Mex_Petroleo",
    "HG=F": "Cobre_USD_ton",
    "HRC=F": "Acero_HRC_ton"
}

# =====================================
# 2. FUNCIÓN MACRO
# =====================================
def generar_reporte_lorex_bi_v8_2():
    print("📊 Descargando datos macro desde Banxico y Yahoo Finance...")
    rango = pd.date_range(start=FECHA_INICIO, end=FECHA_FIN)
    df_final = pd.DataFrame({'Fecha': rango})
    headers = {"Bmx-Token": BMX_TOKEN}

    # --- BANXICO ---
    for sid, nombre in bmx_series.items():
        url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{sid}/datos/{FECHA_INICIO}/{FECHA_FIN}"
        try:
            res = requests.get(url, headers=headers)
            datos = res.json()['bmx']['series'][0]['datos']
            if len(datos) > 0:
                df_temp = pd.DataFrame(datos)
                df_temp['dato'] = df_temp['dato'].str.replace(',', '').astype(float)
                df_temp['fecha_dt'] = pd.to_datetime(df_temp['fecha'], format='%d/%m/%Y')
                df_final = pd.merge(df_final, df_temp[['fecha_dt', 'dato']], left_on='Fecha', right_on='fecha_dt', how='left').drop(columns=['fecha_dt']).rename(columns={'dato': nombre})
        except Exception as e:
            print(f"❌ Error en Banxico ({nombre}): {e}")

    # --- YAHOO ---
    for ticker, nombre in yahoo_tickers.items():
        try:
            tk = yf.Ticker(ticker)
            hist = tk.history(start=FECHA_INICIO, end=FECHA_FIN)
            if not hist.empty:
                hist.index = hist.index.tz_localize(None)
                if ticker == "HG=F":
                    hist['Close'] *= 2204.62
                df_final = pd.merge(df_final, hist[['Close']], left_on='Fecha', right_index=True, how='left').rename(columns={'Close': nombre})
        except Exception as e:
            print(f"❌ Error en Yahoo ({nombre}): {e}")

    df_final = df_final.ffill()

    # --- VARIACIONES ---
    cols_macro = [c for c in df_final.columns if c != 'Fecha']
    for col in cols_macro:
        df_final[f'Var_Diaria_{col}'] = df_final[col].pct_change() * 100

    # Convertir fecha a texto para el merge final
    df_final['Fecha_STR'] = df_final['Fecha'].dt.strftime('%d/%m/%Y')
    return df_final

# =====================================
# 3. FUNCIÓN FBX (Con motor openpyxl)
# =====================================
def get_fbx():
    print("🚢 Cargando FBX desde el repositorio...")
    ruta_fbx = "FBX.xlsx" 
    
    if os.path.exists(ruta_fbx):
        try:
            # Agregamos el engine='openpyxl' para evitar errores en GitHub
            df_fbx = pd.read_excel(ruta_fbx, engine='openpyxl')
            df_fbx.columns = df_fbx.columns.str.strip()
            df_fbx = df_fbx.rename(columns={
                "FBX: Global Ocean Freight Cotainer Pricing Index (USD)": "FBX_Global",
                "FBX03:Global Ocean Freight Container Pricing Index | North America East Coast to China/East Asia (USD)": "FBX03",
                "FBX22:Global Ocean Freight Container Pricing Index | North Europe to North American East Coast (USD)": "FBX22"
            })
            df_fbx['Fecha_STR'] = pd.to_datetime(df_fbx['Fecha']).dt.strftime('%d/%m/%Y')
            return df_fbx.drop(columns=['Fecha'], errors='ignore')
        except Exception as e:
            print(f"❌ Error al leer FBX.xlsx: {e}")
            return pd.DataFrame(columns=["Fecha_STR"])
    else:
        print("⚠️ FBX.xlsx no encontrado. Se generará el reporte sin fletes.")
        return pd.DataFrame(columns=["Fecha_STR"])

# =====================================
# 4. PIPELINE MASTER
# =====================================
def pipeline_master():
    print("🚀 Iniciando Pipeline Maestro Acumulativo...")
    archivo_master = "Macro_Master_Historico.xlsx"

    df_macro = generar_reporte_lorex_bi_v8_2()
    df_fbx = get_fbx()

    # Unión de datos
    df_total = df_macro.merge(df_fbx, on="Fecha_STR", how="left")
    
    # Rellenar huecos de FBX
    cols_fbx = [c for c in ["FBX_Global", "FBX03", "FBX22"] if c in df_total.columns]
    if cols_fbx:
        df_total[cols_fbx] = df_total[cols_fbx].ffill()

    # --- LÓGICA ACUMULATIVA ---
    if os.path.exists(archivo_master):
        try:
            df_viejo = pd.read_excel(archivo_master, engine='openpyxl')
            df_total = pd.concat([df_viejo, df_total]).drop_duplicates(subset=["Fecha_STR"], keep="last")
            print("📂 Datos nuevos integrados al histórico existente.")
        except:
            print("⚠️ El histórico actual no se pudo leer, creando uno nuevo.")

    # Limpieza final de columnas
    if 'Fecha' in df_total.columns:
        df_total = df_total.drop(columns=['Fecha'])
    df_total = df_total.rename(columns={'Fecha_STR': 'Fecha'})

    # --- GUARDAR CON MOTOR EXPLÍCITO ---
    df_total.to_excel(archivo_master, index=False, engine='openpyxl')
    
    print("\n" + "="*60)
    print(f"✅ EXCEL ACTUALIZADO EXITOSAMENTE: {archivo_master}")
    print("="*60)

if __name__ == "__main__":
    pipeline_master()
