import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, timedelta

# =====================================
# 1. CONFIGURACIÓN
# =====================================
BMX_TOKEN = "8c96d3ef981e1ca6847ad7c17802fb95f732b3625457f93932f62133b538b658"

# 🔥 HISTÓRICO DESDE ESTA FECHA
FECHA_INICIO = "2025-12-01"

# 🔥 SE ACTUALIZA SOLO
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

    print("📊 Descargando datos macro...")

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

                df_final = pd.merge(
                    df_final,
                    df_temp[['fecha_dt', 'dato']],
                    left_on='Fecha',
                    right_on='fecha_dt',
                    how='left'
                )

                df_final = df_final.rename(columns={'dato': nombre}).drop(columns=['fecha_dt'])

        except Exception as e:
            print(f"❌ Error en {nombre}: {e}")

    # --- YAHOO ---
    for ticker, nombre in yahoo_tickers.items():
        try:
            hist = yf.download(ticker, start=FECHA_INICIO, end=FECHA_FIN)

            if not hist.empty:
                hist.index = hist.index.tz_localize(None)

                if ticker == "HG=F":
                    hist['Close'] *= 2204.62

                df_final = pd.merge(
                    df_final,
                    hist[['Close']],
                    left_on='Fecha',
                    right_index=True,
                    how='left'
                )

                df_final = df_final.rename(columns={'Close': nombre})

        except Exception as e:
            print(f"❌ Error en Yahoo {nombre}: {e}")

    # --- RELLENO ---
    df_final = df_final.ffill()

    # --- VARIACIONES ---
    cols_macro = [c for c in df_final.columns if c != 'Fecha']

    for col in cols_macro:
        df_final[f'Var_Diaria_{col}'] = df_final[col].pct_change() * 100

        def calc_var_quincenal(row):
            dia = row['Fecha'].day

            if dia <= 15:
                fecha_ref = row['Fecha'].replace(day=1) - timedelta(days=1)
            else:
                fecha_ref = row['Fecha'].replace(day=15)

            try:
                valor_inicio = df_final.loc[df_final['Fecha'] <= fecha_ref, col].iloc[-1]
                return ((row[col] / valor_inicio) - 1) * 100
            except:
                return 0

        df_final[f'Var_Quincenal_{col}'] = df_final.apply(calc_var_quincenal, axis=1)

    df_final['Fecha'] = df_final['Fecha'].dt.strftime('%d/%m/%Y')

    return df_final


# =====================================
# 3. FUNCIÓN FBX
# =====================================
def get_fbx():

    print("🚢 Cargando FBX...")

    ruta_fbx = "FBX.xlsx"  # 🔥 debe estar en GitHub
    df_fbx = pd.read_excel(ruta_fbx)

    df_fbx.columns = df_fbx.columns.str.strip()

    df_fbx = df_fbx.rename(columns={
        "FBX: Global Ocean Freight Cotainer Pricing Index (USD)": "FBX_Global",
        "FBX03:Global Ocean Freight Container Pricing Index | North America East Coast to China/East Asia (USD)": "FBX03",
        "FBX22:Global Ocean Freight Container Pricing Index | North Europe to North American East Coast (USD)": "FBX22"
    })

    df_fbx['Fecha'] = pd.to_datetime(df_fbx['Fecha']).dt.strftime('%d/%m/%Y')

    return df_fbx


# =====================================
# 4. PIPELINE MASTER (HISTÓRICO)
# =====================================
def pipeline_master():

    print("🚀 Ejecutando pipeline histórico...")

    archivo = "Macro_Master_Historico.xlsx"

    df_macro = generar_reporte_lorex_bi_v8_2()
    df_fbx = get_fbx()

    df_nuevo = df_macro.merge(df_fbx, on="Fecha", how="left")

    # --- RELLENO FBX ---
    cols_fbx = ["FBX_Global", "FBX03", "FBX22"]
    df_nuevo[cols_fbx] = df_nuevo[cols_fbx].ffill()

    # =====================================
    # 🔥 HISTÓRICO INTELIGENTE
    # =====================================
    try:
        df_hist = pd.read_excel(archivo)
        print("📂 Histórico encontrado, actualizando...")

        df_total = pd.concat([df_hist, df_nuevo])

        # 🔥 eliminar duplicados por fecha
        df_total = df_total.drop_duplicates(subset=["Fecha"], keep="last")

        df_total = df_total.sort_values("Fecha")

    except:
        print("🆕 Creando histórico nuevo...")
        df_total = df_nuevo

    # --- GUARDAR ---
    df_total.to_excel(archivo, index=False)

    print("\n" + "="*60)
    print("✅ HISTÓRICO ACTUALIZADO")
    print(f"📁 Archivo generado: {archivo}")
    print("="*60)


# =====================================
# 5. EJECUCIÓN
# =====================================
if __name__ == "__main__":
    pipeline_master()
