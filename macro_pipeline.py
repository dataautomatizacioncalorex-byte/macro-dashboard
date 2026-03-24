import pandas as pd
import yfinance as yf
import requests
import os
from datetime import datetime, timedelta

# =====================================
# 1. CONFIGURACIÓN (TUS LLAVES Y FECHAS)
# =====================================
BMX_TOKEN = "8c96d3ef981e1ca6847ad7c17802fb95f732b3625457f93932f62133b538b658"
FECHA_INICIO = "2025-01-01"  # Empezamos desde el inicio de este año
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
# 2. FUNCIÓN PARA DATOS MACRO (BANXICO Y YAHOO)
# =====================================
def generar_reporte_lorex_bi_v8_2():
    print("📊 Paso 1: Descargando datos de Banxico y Yahoo Finance...")
    rango = pd.date_range(start=FECHA_INICIO, end=FECHA_FIN)
    df_final = pd.DataFrame({'Fecha': rango})
    headers = {"Bmx-Token": BMX_TOKEN}

    # Banxico
    for sid, nombre in bmx_series.items():
        try:
            url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{sid}/datos/{FECHA_INICIO}/{FECHA_FIN}"
            res = requests.get(url, headers=headers)
            datos = res.json()['bmx']['series'][0]['datos']
            if len(datos) > 0:
                df_temp = pd.DataFrame(datos)
                df_temp['dato'] = df_temp['dato'].str.replace(',', '').astype(float)
                df_temp['fecha_dt'] = pd.to_datetime(df_temp['fecha'], format='%d/%m/%Y')
                df_final = pd.merge(df_final, df_temp[['fecha_dt', 'dato']], left_on='Fecha', right_on='fecha_dt', how='left').drop(columns=['fecha_dt']).rename(columns={'dato': nombre})
        except: print(f"❌ Error en Banxico: {nombre}")

    # Yahoo
    for ticker, nombre in yahoo_tickers.items():
        try:
            tk = yf.Ticker(ticker)
            hist = tk.history(start=FECHA_INICIO, end=FECHA_FIN)
            if not hist.empty:
                hist.index = hist.index.tz_localize(None)
                if ticker == "HG=F": hist['Close'] *= 2204.62
                df_final = pd.merge(df_final, hist[['Close']], left_on='Fecha', right_index=True, how='left').rename(columns={'Close': nombre})
        except: print(f"❌ Error en Yahoo: {nombre}")

    df_final = df_final.ffill() # Rellenamos fines de semana

    # Variaciones
    cols_macro = [c for c in df_final.columns if c != 'Fecha']
    for col in cols_macro:
        df_final[f'Var_Diaria_{col}'] = df_final[col].pct_change() * 100

    # Guardamos la fecha como texto para que el merge sea fácil
    df_final['Fecha_STR'] = df_final['Fecha'].dt.strftime('%d/%m/%Y')
    return df_final

# =====================================
# 3. FUNCIÓN PARA FBX (MARÍTIMO)
# =====================================
def get_fbx():
    print("🚢 Paso 2: Buscando archivo FBX.xlsx...")
    ruta_fbx = "FBX.xlsx" 
    if os.path.exists(ruta_fbx):
        try:
            df_fbx = pd.read_excel(ruta_fbx, engine='openpyxl')
            df_fbx.columns = df_fbx.columns.str.strip()
            df_fbx = df_fbx.rename(columns={
                "FBX: Global Ocean Freight Cotainer Pricing Index (USD)": "FBX_Global",
                "FBX03:Global Ocean Freight Container Pricing Index | North America East Coast to China/East Asia (USD)": "FBX03",
                "FBX22:Global Ocean Freight Container Pricing Index | North Europe to North American East Coast (USD)": "FBX22"
            })
            df_fbx['Fecha_STR'] = pd.to_datetime(df_fbx['Fecha']).dt.strftime('%d/%m/%Y')
            return df_fbx.drop(columns=['Fecha'], errors='ignore')
        except:
            return pd.DataFrame(columns=["Fecha_STR"])
    return pd.DataFrame(columns=["Fecha_STR"])

# =====================================
# 4. PASO MAESTRO: SUMAR DATOS (ACUMULATIVO)
# =====================================
def pipeline_master():
    print("🚀 Paso 3: Iniciando unión de datos...")
    archivo_master = "Macro_Master_Historico.xlsx"

    # Datos descargados hoy
    df_actual = generar_reporte_lorex_bi_v8_2()
    df_fbx = get_fbx()
    df_combinado = df_actual.merge(df_fbx, on="Fecha_STR", how="left")
    
    # Rellenar fletes marítimos
    cols_fbx = [c for c in ["FBX_Global", "FBX03", "FBX22"] if c in df_combinado.columns]
    if cols_fbx: df_combinado[cols_fbx] = df_combinado[cols_fbx].ffill()

    # --- AQUÍ SE AGREGAN LOS DATOS AL EXCEL EXISTENTE ---
    if os.path.exists(archivo_master):
        print("📂 Leyendo histórico previo para sumar datos...")
        try:
            df_historico_previo = pd.read_excel(archivo_master, engine='openpyxl')
            # Pegamos lo que ya teníamos con lo nuevo
            df_final = pd.concat([df_historico_previo, df_combinado])
            # Eliminamos duplicados por si acaso corrió dos veces el mismo día
            df_final = df_final.drop_duplicates(subset=["Fecha_STR"], keep="last")
        except:
            print("⚠️ Error leyendo histórico, se creará uno nuevo.")
            df_final = df_combinado
    else:
        print("🆕 No hay histórico, creando archivo inicial.")
        df_final = df_combinado

    # Limpieza final de nombres
    if 'Fecha' in df_final.columns: df_final = df_final.drop(columns=['Fecha'])
    df_final = df_final.rename(columns={'Fecha_STR': 'Fecha'})

    # Guardar el resultado final
    df_final.to_excel(archivo_master, index=False, engine='openpyxl')
    print(f"✅ EXCEL ACTUALIZADO: {archivo_master}")

if __name__ == "__main__":
    pipeline_master()
