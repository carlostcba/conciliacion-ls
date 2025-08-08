import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from difflib import SequenceMatcher
import openpyxl
from openpyxl.styles import PatternFill

# Verificar e instalar dependencias necesarias
def verificar_dependencias():
    """Verifica e informa sobre dependencias faltantes"""
    dependencias_faltantes = []
    
    try:
        import xlrd
    except ImportError:
        dependencias_faltantes.append('xlrd>=2.0.1')
    
    if dependencias_faltantes:
        print("⚠️  DEPENDENCIAS FALTANTES:")
        print("Ejecuta estos comandos para instalar:")
        for dep in dependencias_faltantes:
            print(f"   pip install {dep}")
        print()
        return False
    return True

class ConciliadorBancario:
    def __init__(self):
        self.tolerancia_monto_pct = 0.02  # 2% de tolerancia para montos
        self.tolerancia_fecha_dias = 1    # 1 dia de tolerancia general
        
        # Mapeo de conceptos comunes observados en los datos
        self.mapeo_conceptos = {
            'transferencia': ['transferencia', 'transf', 'credito inmediato', 'debin'],
            'retencion': ['retencion', 'ret'],
            'debito': ['debito', 'deb'],
            'credito': ['credito', 'cred', 'acreditacion'],
            'iva': ['iva', 'impuesto'],
            'cheque': ['cheque', 'ch'],
            'mercado_pago': ['mercado pago', 'mp', 'mercadopago']
        }
    
    def cargar_archivos(self, archivo_cont, archivo_bco):
        """Carga y limpia los archivos contable y bancario"""
        
        print("Cargando archivo contable...")
        
        try:
            # Intentar cargar con engine openpyxl primero
            df_cont_raw = pd.read_excel(archivo_cont, header=None, engine='openpyxl')
        except Exception as e1:
            try:
                # Si falla, intentar con xlrd
                df_cont_raw = pd.read_excel(archivo_cont, header=None, engine='xlrd')
            except Exception as e2:
                # Como ultimo recurso, convertir a xlsx y cargar
                print(f"⚠️  No se pudo cargar {archivo_cont} directamente.")
                print("Intenta convertir el archivo .xls a .xlsx manualmente y vuelve a ejecutar.")
                print(f"Error con openpyxl: {e1}")
                print(f"Error con xlrd: {e2}")
                raise e2
        
        # Los headers estan en fila 1 (indice 1)
        headers_cont = df_cont_raw.iloc[1].tolist()
        
        # Limpiar datos contables (desde fila 2)
        cont_data = []
        for i in range(2, len(df_cont_raw)):
            row = df_cont_raw.iloc[i]
            if pd.notna(row.iloc[4]):  # Si tiene fecha
                transaccion = {
                    'fecha_original': row.iloc[4],
                    'concepto': str(row.iloc[5]) if pd.notna(row.iloc[5]) else '',
                    'comprobante': row.iloc[6] if pd.notna(row.iloc[6]) else '',
                    'debe': float(row.iloc[9]) if pd.notna(row.iloc[9]) else 0,
                    'haber': float(row.iloc[11]) if pd.notna(row.iloc[11]) else 0,
                    'saldo': float(row.iloc[13]) if pd.notna(row.iloc[13]) else 0,
                    'indice_original': i
                }
                cont_data.append(transaccion)
        
        self.df_cont = pd.DataFrame(cont_data)
        self.df_cont['fecha'] = self.df_cont['fecha_original'].apply(self.convertir_fecha_excel)
        self.df_cont['monto'] = self.df_cont.apply(lambda x: x['debe'] if x['debe'] > 0 else x['haber'], axis=1)
        self.df_cont['tipo'] = self.df_cont.apply(lambda x: 'DEBE' if x['debe'] > 0 else 'HABER', axis=1)
        
        print(f"Archivo contable cargado: {len(self.df_cont)} transacciones")
        
        print("Cargando archivo bancario...")
        
        try:
            # Intentar cargar archivo bancario
            df_bco_raw = pd.read_excel(archivo_bco, header=None, engine='openpyxl')
        except Exception as e1:
            try:
                df_bco_raw = pd.read_excel(archivo_bco, header=None, engine='xlrd')
            except Exception as e2:
                print(f"⚠️  No se pudo cargar {archivo_bco} directamente.")
                print("Intenta convertir el archivo .xls a .xlsx manualmente y vuelve a ejecutar.")
                print(f"Error con openpyxl: {e1}")
                print(f"Error con xlrd: {e2}")
                raise e2
        
        # Limpiar datos bancarios (headers en fila 0, datos desde fila 1)
        bco_data = []
        for i in range(1, len(df_bco_raw)):
            row = df_bco_raw.iloc[i]
            if pd.notna(row.iloc[1]):  # Si tiene fecha
                transaccion = {
                    'fecha': self.convertir_fecha_bancaria(str(row.iloc[1])),
                    'concepto': str(row.iloc[2]) if pd.notna(row.iloc[2]) else '',
                    'nro_comprobante': row.iloc[3] if pd.notna(row.iloc[3]) else '',
                    'debito': float(row.iloc[4]) if pd.notna(row.iloc[4]) else 0,
                    'credito': float(row.iloc[5]) if pd.notna(row.iloc[5]) else 0,
                    'saldo': float(row.iloc[6]) if pd.notna(row.iloc[6]) else 0,
                    'codigo': row.iloc[7] if pd.notna(row.iloc[7]) else '',
                    'indice_original': i
                }
                bco_data.append(transaccion)
        
        self.df_bco = pd.DataFrame(bco_data)
        self.df_bco['monto'] = self.df_bco.apply(lambda x: x['debito'] if x['debito'] > 0 else x['credito'], axis=1)
        self.df_bco['tipo'] = self.df_bco.apply(lambda x: 'DEBITO' if x['debito'] > 0 else 'CREDITO', axis=1)
        
        print(f"Archivo bancario cargado: {len(self.df_bco)} transacciones")
    
    def convertir_fecha_excel(self, fecha_excel):
        """Convierte fecha de Excel (numero de dias desde 1900) a datetime"""
        try:
            if isinstance(fecha_excel, (int, float)):
                # Excel cuenta desde 1900-01-01, pero tiene error de año bisiesto
                base_date = datetime(1899, 12, 30)
                return base_date + timedelta(days=fecha_excel)
            return pd.to_datetime(fecha_excel)
        except:
            return None
    
    def convertir_fecha_bancaria(self, fecha_str):
        """Convierte fecha bancaria string a datetime"""
        try:
            return pd.to_datetime(fecha_str, format='%d/%m/%Y')
        except:
            try:
                return pd.to_datetime(fecha_str)
            except:
                return None
    
    def normalizar_concepto(self, concepto):
        """Normaliza conceptos para comparacion"""
        concepto = str(concepto).lower().strip()
        # Eliminar caracteres especiales y espacios extras
        concepto = re.sub(r'[^\w\s]', ' ', concepto)
        concepto = re.sub(r'\s+', ' ', concepto)
        return concepto
    
    def calcular_similitud_concepto(self, concepto1, concepto2):
        """Calcula similitud entre conceptos usando mapeo y similitud de texto"""
        c1_norm = self.normalizar_concepto(concepto1)
        c2_norm = self.normalizar_concepto(concepto2)
        
        # Similitud directa
        similitud_directa = SequenceMatcher(None, c1_norm, c2_norm).ratio()
        
        # Buscar en mapeos de conceptos
        similitud_mapeo = 0
        for categoria, keywords in self.mapeo_conceptos.items():
            c1_match = any(kw in c1_norm for kw in keywords)
            c2_match = any(kw in c2_norm for kw in keywords)
            if c1_match and c2_match:
                similitud_mapeo = max(similitud_mapeo, 0.8)
        
        return max(similitud_directa, similitud_mapeo)
    
    def es_monto_similar(self, monto1, monto2, tolerancia_pct=None):
        """Verifica si dos montos son similares dentro de la tolerancia"""
        if tolerancia_pct is None:
            tolerancia_pct = self.tolerancia_monto_pct
        
        if monto1 == 0 or monto2 == 0:
            return monto1 == monto2
        
        diferencia_pct = abs(monto1 - monto2) / max(monto1, monto2)
        return diferencia_pct <= tolerancia_pct
    
    def es_fecha_similar(self, fecha1, fecha2, tolerancia_dias=None):
        """Verifica si dos fechas son similares dentro de la tolerancia"""
        if tolerancia_dias is None:
            tolerancia_dias = self.tolerancia_fecha_dias
        
        if fecha1 is None or fecha2 is None:
            return False
        
        diferencia = abs((fecha1 - fecha2).days)
        return diferencia <= tolerancia_dias
    
    def buscar_coincidencias(self):
        """Busca coincidencias entre transacciones contables y bancarias"""
        
        coincidencias = []
        cont_procesados = set()
        bco_procesados = set()
        
        print("Buscando coincidencias...")
        
        # Nivel 1: Coincidencia exacta (fecha + monto + concepto similar)
        print("Nivel 1: Coincidencias exactas")
        for i, row_cont in self.df_cont.iterrows():
            if i in cont_procesados:
                continue
                
            for j, row_bco in self.df_bco.iterrows():
                if j in bco_procesados:
                    continue
                
                # Verificar correspondencia de tipos (debe_cont = credito_bco, haber_cont = debito_bco)
                tipos_coinciden = (
                    (row_cont['tipo'] == 'DEBE' and row_bco['tipo'] == 'CREDITO') or
                    (row_cont['tipo'] == 'HABER' and row_bco['tipo'] == 'DEBITO')
                )
                
                if (tipos_coinciden and 
                    self.es_fecha_similar(row_cont['fecha'], row_bco['fecha'], 0) and  # Fecha exacta
                    abs(row_cont['monto'] - row_bco['monto']) < 0.01 and  # Monto exacto
                    self.calcular_similitud_concepto(row_cont['concepto'], row_bco['concepto']) > 0.6):
                    
                    coincidencias.append({
                        'cont_index': i,
                        'bco_index': j,
                        'nivel': 1,
                        'descripcion': 'Coincidencia exacta',
                        'similitud_concepto': self.calcular_similitud_concepto(row_cont['concepto'], row_bco['concepto']),
                        'diferencia_monto': abs(row_cont['monto'] - row_bco['monto']),
                        'diferencia_fecha': abs((row_cont['fecha'] - row_bco['fecha']).days)
                    })
                    
                    cont_procesados.add(i)
                    bco_procesados.add(j)
                    break
        
        print(f"Nivel 1 completado: {len(coincidencias)} coincidencias exactas")
        
        # Nivel 2: Coincidencia por fecha y monto (concepto no importa)
        print("Nivel 2: Coincidencias por fecha y monto")
        for i, row_cont in self.df_cont.iterrows():
            if i in cont_procesados:
                continue
                
            for j, row_bco in self.df_bco.iterrows():
                if j in bco_procesados:
                    continue
                
                tipos_coinciden = (
                    (row_cont['tipo'] == 'DEBE' and row_bco['tipo'] == 'CREDITO') or
                    (row_cont['tipo'] == 'HABER' and row_bco['tipo'] == 'DEBITO')
                )
                
                if (tipos_coinciden and 
                    self.es_fecha_similar(row_cont['fecha'], row_bco['fecha'], 0) and
                    abs(row_cont['monto'] - row_bco['monto']) < 0.01):
                    
                    coincidencias.append({
                        'cont_index': i,
                        'bco_index': j,
                        'nivel': 2,
                        'descripcion': 'Coincidencia fecha y monto',
                        'similitud_concepto': self.calcular_similitud_concepto(row_cont['concepto'], row_bco['concepto']),
                        'diferencia_monto': abs(row_cont['monto'] - row_bco['monto']),
                        'diferencia_fecha': abs((row_cont['fecha'] - row_bco['fecha']).days)
                    })
                    
                    cont_procesados.add(i)
                    bco_procesados.add(j)
                    break
        
        print(f"Nivel 2 completado: {len(coincidencias) - len([c for c in coincidencias if c['nivel'] == 1])} nuevas coincidencias")
        
        # Nivel 3: Coincidencia aproximada por monto (fecha exacta, monto con tolerancia)
        print("Nivel 3: Coincidencias aproximadas por monto")
        for i, row_cont in self.df_cont.iterrows():
            if i in cont_procesados:
                continue
                
            for j, row_bco in self.df_bco.iterrows():
                if j in bco_procesados:
                    continue
                
                tipos_coinciden = (
                    (row_cont['tipo'] == 'DEBE' and row_bco['tipo'] == 'CREDITO') or
                    (row_cont['tipo'] == 'HABER' and row_bco['tipo'] == 'DEBITO')
                )
                
                if (tipos_coinciden and 
                    self.es_fecha_similar(row_cont['fecha'], row_bco['fecha'], 0) and
                    self.es_monto_similar(row_cont['monto'], row_bco['monto'])):
                    
                    coincidencias.append({
                        'cont_index': i,
                        'bco_index': j,
                        'nivel': 3,
                        'descripcion': 'Coincidencia aproximada por monto',
                        'similitud_concepto': self.calcular_similitud_concepto(row_cont['concepto'], row_bco['concepto']),
                        'diferencia_monto': abs(row_cont['monto'] - row_bco['monto']),
                        'diferencia_fecha': abs((row_cont['fecha'] - row_bco['fecha']).days)
                    })
                    
                    cont_procesados.add(i)
                    bco_procesados.add(j)
                    break
        
        print(f"Nivel 3 completado: {len(coincidencias) - len([c for c in coincidencias if c['nivel'] <= 2])} nuevas coincidencias")
        
        # Nivel 4: Tolerancia de fecha (monto exacto, fecha ±1 dia)
        print("Nivel 4: Coincidencias con tolerancia de fecha")
        for i, row_cont in self.df_cont.iterrows():
            if i in cont_procesados:
                continue
                
            for j, row_bco in self.df_bco.iterrows():
                if j in bco_procesados:
                    continue
                
                tipos_coinciden = (
                    (row_cont['tipo'] == 'DEBE' and row_bco['tipo'] == 'CREDITO') or
                    (row_cont['tipo'] == 'HABER' and row_bco['tipo'] == 'DEBITO')
                )
                
                if (tipos_coinciden and 
                    self.es_fecha_similar(row_cont['fecha'], row_bco['fecha'], 1) and
                    abs(row_cont['monto'] - row_bco['monto']) < 0.01):
                    
                    coincidencias.append({
                        'cont_index': i,
                        'bco_index': j,
                        'nivel': 4,
                        'descripcion': 'Coincidencia con tolerancia de fecha',
                        'similitud_concepto': self.calcular_similitud_concepto(row_cont['concepto'], row_bco['concepto']),
                        'diferencia_monto': abs(row_cont['monto'] - row_bco['monto']),
                        'diferencia_fecha': abs((row_cont['fecha'] - row_bco['fecha']).days)
                    })
                    
                    cont_procesados.add(i)
                    bco_procesados.add(j)
                    break
        
        print(f"Nivel 4 completado: {len(coincidencias) - len([c for c in coincidencias if c['nivel'] <= 3])} nuevas coincidencias")
        
        self.coincidencias = coincidencias
        self.cont_sin_conciliar = [i for i in range(len(self.df_cont)) if i not in cont_procesados]
        self.bco_sin_conciliar = [i for i in range(len(self.df_bco)) if i not in bco_procesados]
        
        print(f"\nResumen de coincidencias:")
        print(f"Total coincidencias: {len(coincidencias)}")
        print(f"Contables sin conciliar: {len(self.cont_sin_conciliar)}")
        print(f"Bancarias sin conciliar: {len(self.bco_sin_conciliar)}")
    
    def generar_reporte(self, archivo_salida='conciliacion_result.xlsx'):
        """Genera el archivo de reporte con los resultados"""
        
        print(f"Generando reporte: {archivo_salida}")
        
        with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
            
            # Hoja 1: Coincidencias encontradas
            coincidencias_data = []
            for coinc in self.coincidencias:
                row_cont = self.df_cont.iloc[coinc['cont_index']]
                row_bco = self.df_bco.iloc[coinc['bco_index']]
                
                coincidencias_data.append({
                    'Nivel': coinc['nivel'],
                    'Descripcion': coinc['descripcion'],
                    'Fecha_Cont': row_cont['fecha'].strftime('%d/%m/%Y'),
                    'Concepto_Cont': row_cont['concepto'][:50],
                    'Monto_Cont': row_cont['monto'],
                    'Tipo_Cont': row_cont['tipo'],
                    'Fecha_Bco': row_bco['fecha'].strftime('%d/%m/%Y'),
                    'Concepto_Bco': row_bco['concepto'][:50], 
                    'Monto_Bco': row_bco['monto'],
                    'Tipo_Bco': row_bco['tipo'],
                    'Dif_Monto': coinc['diferencia_monto'],
                    'Dif_Fecha_Dias': coinc['diferencia_fecha'],
                    'Similitud_Concepto': round(coinc['similitud_concepto'], 3)
                })
            
            df_coincidencias = pd.DataFrame(coincidencias_data)
            df_coincidencias.to_excel(writer, sheet_name='Coincidencias', index=False)
            
            # Hoja 2: Contables sin conciliar
            cont_sin_conciliar_data = []
            for idx in self.cont_sin_conciliar:
                row = self.df_cont.iloc[idx]
                cont_sin_conciliar_data.append({
                    'Fecha': row['fecha'].strftime('%d/%m/%Y'),
                    'Concepto': row['concepto'],
                    'Monto': row['monto'],
                    'Tipo': row['tipo'],
                    'Debe': row['debe'],
                    'Haber': row['haber']
                })
            
            df_cont_sin_conciliar = pd.DataFrame(cont_sin_conciliar_data)
            df_cont_sin_conciliar.to_excel(writer, sheet_name='Contable_Sin_Conciliar', index=False)
            
            # Hoja 3: Bancarias sin conciliar
            bco_sin_conciliar_data = []
            for idx in self.bco_sin_conciliar:
                row = self.df_bco.iloc[idx]
                bco_sin_conciliar_data.append({
                    'Fecha': row['fecha'].strftime('%d/%m/%Y'),
                    'Concepto': row['concepto'],
                    'Monto': row['monto'],
                    'Tipo': row['tipo'],
                    'Debito': row['debito'],
                    'Credito': row['credito']
                })
            
            df_bco_sin_conciliar = pd.DataFrame(bco_sin_conciliar_data)
            df_bco_sin_conciliar.to_excel(writer, sheet_name='Bancario_Sin_Conciliar', index=False)
            
            # Hoja 4: Resumen estadistico
            resumen_data = [
                ['Total Transacciones Contables', len(self.df_cont)],
                ['Total Transacciones Bancarias', len(self.df_bco)],
                ['Total Coincidencias', len(self.coincidencias)],
                ['Nivel 1 - Exactas', len([c for c in self.coincidencias if c['nivel'] == 1])],
                ['Nivel 2 - Fecha y Monto', len([c for c in self.coincidencias if c['nivel'] == 2])],
                ['Nivel 3 - Monto Aproximado', len([c for c in self.coincidencias if c['nivel'] == 3])],
                ['Nivel 4 - Tolerancia Fecha', len([c for c in self.coincidencias if c['nivel'] == 4])],
                ['Contables Sin Conciliar', len(self.cont_sin_conciliar)],
                ['Bancarias Sin Conciliar', len(self.bco_sin_conciliar)],
                ['% Conciliacion Contable', round(len(self.coincidencias) / len(self.df_cont) * 100, 2)],
                ['% Conciliacion Bancaria', round(len(self.coincidencias) / len(self.df_bco) * 100, 2)]
            ]
            
            df_resumen = pd.DataFrame(resumen_data, columns=['Metrica', 'Valor'])
            df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
        
        print(f"Reporte generado exitosamente: {archivo_salida}")

# Funcion principal para ejecutar la conciliacion
def ejecutar_conciliacion(archivo_cont, archivo_bco, archivo_salida='conciliacion_resultado.xlsx'):
    """
    Ejecuta el proceso completo de conciliacion bancaria
    
    Args:
        archivo_cont: Ruta del archivo contable (credi_cont_01_062025.xls)
        archivo_bco: Ruta del archivo bancario (credi_bco_01_062025.xls)
        archivo_salida: Nombre del archivo de salida (opcional)
    """
    
    conciliador = ConciliadorBancario()
    
    try:
        # Cargar archivos
        conciliador.cargar_archivos(archivo_cont, archivo_bco)
        
        # Buscar coincidencias
        conciliador.buscar_coincidencias()
        
        # Generar reporte
        conciliador.generar_reporte(archivo_salida)
        
        print(f"\n✅ Proceso completado exitosamente!")
        print(f"📊 Archivo de resultado: {archivo_salida}")
        
        return conciliador
        
    except Exception as e:
        print(f"❌ Error durante el proceso: {str(e)}")
        raise e

# Ejemplo de uso:
if __name__ == "__main__":
    # Verificar dependencias primero
    if not verificar_dependencias():
        print("Por favor instala las dependencias faltantes antes de continuar.")
        exit(1)
    
    # Ejecutar conciliacion
    resultado = ejecutar_conciliacion(
        archivo_cont='credi_cont_01_062025.xls',
        archivo_bco='credi_bco_01_062025.xls',
        archivo_salida='conciliacion_resultado.xlsx'
    )