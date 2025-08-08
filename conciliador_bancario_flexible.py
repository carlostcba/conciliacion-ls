import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import os
import glob
from difflib import SequenceMatcher
import openpyxl
from openpyxl.styles import PatternFill
from pathlib import Path

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

class ConciliadorBancarioFlexible:
    def __init__(self):
        self.tolerancia_monto_pct = 0.02  # 2% de tolerancia para montos
        self.tolerancia_fecha_dias = 1    # 1 dia de tolerancia general
        
        # Directorios de trabajo
        self.dir_bancos = "Bancos"
        self.dir_contable = "Contable"
        self.dir_procesado = "Procesado"
        
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
    
    def crear_directorios(self):
        """Crea los directorios necesarios si no existen"""
        for directorio in [self.dir_bancos, self.dir_contable, self.dir_procesado]:
            if not os.path.exists(directorio):
                os.makedirs(directorio)
                print(f"📁 Directorio creado: {directorio}")
    
    def parsear_nombre_archivo(self, nombre_archivo):
        """
        Parsea el nombre del archivo para extraer banco, cuenta y periodo
        Formato esperado: {banco}_{tipo}_{cuenta}_{periodo}.{ext}
        Ejemplo: credi_cont_01_062025.xls -> banco=credi, tipo=cont, cuenta=01, periodo=062025
        """
        nombre_base = os.path.splitext(os.path.basename(nombre_archivo))[0]
        partes = nombre_base.split('_')
        
        if len(partes) >= 4:
            return {
                'banco': partes[0],
                'tipo': partes[1],
                'cuenta': partes[2],
                'periodo': partes[3],
                'archivo_completo': nombre_archivo
            }
        else:
            print(f"⚠️  Formato de archivo no reconocido: {nombre_archivo}")
            return None
    
    def encontrar_archivos_por_patron(self, banco=None, cuenta=None, periodo=None):
        """
        Encuentra archivos contables y bancarios que coincidan con el patron
        """
        patron_cont = f"{self.dir_contable}/"
        patron_bco = f"{self.dir_bancos}/"
        
        # Construir patrones de busqueda
        if banco and cuenta and periodo:
            patron_cont += f"{banco}_cont_{cuenta}_{periodo}.*"
            patron_bco += f"{banco}_bco_{cuenta}_{periodo}.*"
        elif banco and cuenta:
            patron_cont += f"{banco}_cont_{cuenta}_*.*"
            patron_bco += f"{banco}_bco_{cuenta}_*.*"
        elif banco:
            patron_cont += f"{banco}_cont_*.*"
            patron_bco += f"{banco}_bco_*.*"
        else:
            patron_cont += "*_cont_*.*"
            patron_bco += "*_bco_*.*"
        
        archivos_cont = glob.glob(patron_cont)
        archivos_bco = glob.glob(patron_bco)
        
        return archivos_cont, archivos_bco
    
    def listar_archivos_disponibles(self):
        """Lista todos los archivos disponibles organizados por banco, cuenta y periodo"""
        print("\n📋 ARCHIVOS DISPONIBLES:")
        print("=" * 60)
        
        # Buscar todos los archivos
        archivos_cont, archivos_bco = self.encontrar_archivos_por_patron()
        
        # Organizar por banco, cuenta, periodo
        organizacion = {}
        
        for archivo in archivos_cont + archivos_bco:
            info = self.parsear_nombre_archivo(archivo)
            if info:
                banco = info['banco']
                cuenta = info['cuenta']
                periodo = info['periodo']
                tipo = info['tipo']
                
                if banco not in organizacion:
                    organizacion[banco] = {}
                if cuenta not in organizacion[banco]:
                    organizacion[banco][cuenta] = {}
                if periodo not in organizacion[banco][cuenta]:
                    organizacion[banco][cuenta][periodo] = {}
                
                organizacion[banco][cuenta][periodo][tipo] = archivo
        
        # Mostrar organizacion
        for banco in sorted(organizacion.keys()):
            print(f"\n🏦 {banco.upper()}")
            for cuenta in sorted(organizacion[banco].keys()):
                print(f"  💳 Cuenta {cuenta}")
                for periodo in sorted(organizacion[banco][cuenta].keys()):
                    archivos_periodo = organizacion[banco][cuenta][periodo]
                    cont_status = "✅" if 'cont' in archivos_periodo else "❌"
                    bco_status = "✅" if 'bco' in archivos_periodo else "❌"
                    print(f"    📅 {periodo}: Contable {cont_status} | Bancario {bco_status}")
        
        return organizacion
    
    def encontrar_pares_conciliables(self):
        """Encuentra pares de archivos (contable + bancario) que se pueden conciliar"""
        organizacion = self.listar_archivos_disponibles()
        pares = []
        
        for banco in organizacion:
            for cuenta in organizacion[banco]:
                for periodo in organizacion[banco][cuenta]:
                    archivos = organizacion[banco][cuenta][periodo]
                    if 'cont' in archivos and 'bco' in archivos:
                        pares.append({
                            'banco': banco,
                            'cuenta': cuenta,
                            'periodo': periodo,
                            'archivo_cont': archivos['cont'],
                            'archivo_bco': archivos['bco'],
                            'archivo_salida': f"{self.dir_procesado}/{banco}_pro_{cuenta}_{periodo}.xlsx"
                        })
        
        print(f"\n🔍 PARES ENCONTRADOS PARA CONCILIAR: {len(pares)}")
        for i, par in enumerate(pares, 1):
            print(f"{i}. {par['banco'].upper()} - Cuenta {par['cuenta']} - {par['periodo']}")
        
        return pares
    
    def cargar_archivos(self, archivo_cont, archivo_bco):
        """Carga y limpia los archivos contable y bancario"""
        
        print(f"Cargando archivo contable: {archivo_cont}")
        
        try:
            # Intentar cargar con engine openpyxl primero
            df_cont_raw = pd.read_excel(archivo_cont, header=None, engine='openpyxl')
        except Exception as e1:
            try:
                # Si falla, intentar con xlrd
                df_cont_raw = pd.read_excel(archivo_cont, header=None, engine='xlrd')
            except Exception as e2:
                print(f"⚠️  No se pudo cargar {archivo_cont}")
                print(f"Error con openpyxl: {e1}")
                print(f"Error con xlrd: {e2}")
                raise e2
        
        # Limpiar datos contables
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
        
        print(f"Cargando archivo bancario: {archivo_bco}")
        
        try:
            df_bco_raw = pd.read_excel(archivo_bco, header=None, engine='openpyxl')
        except Exception as e1:
            try:
                df_bco_raw = pd.read_excel(archivo_bco, header=None, engine='xlrd')
            except Exception as e2:
                print(f"⚠️  No se pudo cargar {archivo_bco}")
                print(f"Error con openpyxl: {e1}")
                print(f"Error con xlrd: {e2}")
                raise e2
        
        # Limpiar datos bancarios
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
        concepto = re.sub(r'[^\w\s]', ' ', concepto)
        concepto = re.sub(r'\s+', ' ', concepto)
        return concepto
    
    def calcular_similitud_concepto(self, concepto1, concepto2):
        """Calcula similitud entre conceptos usando mapeo y similitud de texto"""
        c1_norm = self.normalizar_concepto(concepto1)
        c2_norm = self.normalizar_concepto(concepto2)
        
        similitud_directa = SequenceMatcher(None, c1_norm, c2_norm).ratio()
        
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
                
                tipos_coinciden = (
                    (row_cont['tipo'] == 'DEBE' and row_bco['tipo'] == 'CREDITO') or
                    (row_cont['tipo'] == 'HABER' and row_bco['tipo'] == 'DEBITO')
                )
                
                if (tipos_coinciden and 
                    self.es_fecha_similar(row_cont['fecha'], row_bco['fecha'], 0) and
                    abs(row_cont['monto'] - row_bco['monto']) < 0.01 and
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
        
        # Nivel 2: Coincidencia por fecha y monto
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
        
        # Nivel 3: Coincidencia aproximada por monto
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
        
        # Nivel 4: Tolerancia de fecha
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
    
    def generar_reporte(self, archivo_salida, banco=None, cuenta=None, periodo=None):
        """Genera el archivo de reporte con los resultados"""
        
        print(f"Generando reporte: {archivo_salida}")
        
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(archivo_salida), exist_ok=True)
        
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
                ['Banco', banco or 'N/A'],
                ['Cuenta', cuenta or 'N/A'],
                ['Periodo', periodo or 'N/A'],
                ['Fecha Procesamiento', datetime.now().strftime('%d/%m/%Y %H:%M')],
                ['', ''],
                ['Total Transacciones Contables', len(self.df_cont)],
                ['Total Transacciones Bancarias', len(self.df_bco)],
                ['Total Coincidencias', len(self.coincidencias)],
                ['', ''],
                ['Nivel 1 - Exactas', len([c for c in self.coincidencias if c['nivel'] == 1])],
                ['Nivel 2 - Fecha y Monto', len([c for c in self.coincidencias if c['nivel'] == 2])],
                ['Nivel 3 - Monto Aproximado', len([c for c in self.coincidencias if c['nivel'] == 3])],
                ['Nivel 4 - Tolerancia Fecha', len([c for c in self.coincidencias if c['nivel'] == 4])],
                ['', ''],
                ['Contables Sin Conciliar', len(self.cont_sin_conciliar)],
                ['Bancarias Sin Conciliar', len(self.bco_sin_conciliar)],
                ['', ''],
                ['% Conciliacion Contable', round(len(self.coincidencias) / len(self.df_cont) * 100, 2) if len(self.df_cont) > 0 else 0],
                ['% Conciliacion Bancaria', round(len(self.coincidencias) / len(self.df_bco) * 100, 2) if len(self.df_bco) > 0 else 0]
            ]
            
            df_resumen = pd.DataFrame(resumen_data, columns=['Metrica', 'Valor'])
            df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
        
        print(f"Reporte generado exitosamente: {archivo_salida}")

def conciliar_un_par(banco, cuenta, periodo):
    """
    Concilia un par especifico de archivos
    """
    conciliador = ConciliadorBancarioFlexible()
    conciliador.crear_directorios()
    
    archivo_cont = f"{conciliador.dir_contable}/{banco}_cont_{cuenta}_{periodo}.xls"
    archivo_bco = f"{conciliador.dir_bancos}/{banco}_bco_{cuenta}_{periodo}.xls"
    archivo_salida = f"{conciliador.dir_procesado}/{banco}_pro_{cuenta}_{periodo}.xlsx"
    
    if not os.path.exists(archivo_cont):
        print(f"❌ Archivo contable no encontrado: {archivo_cont}")
        return None
    
    if not os.path.exists(archivo_bco):
        print(f"❌ Archivo bancario no encontrado: {archivo_bco}")
        return None
    
    try:
        print(f"\n🔄 Conciliando: {banco.upper()} - Cuenta {cuenta} - {periodo}")
        print("=" * 60)
        
        # Cargar archivos
        conciliador.cargar_archivos(archivo_cont, archivo_bco)
        
        # Buscar coincidencias
        conciliador.buscar_coincidencias()
        
        # Generar reporte
        conciliador.generar_reporte(archivo_salida, banco, cuenta, periodo)
        
        print(f"✅ Conciliacion completada: {archivo_salida}")
        return conciliador
        
    except Exception as e:
        print(f"❌ Error en conciliacion: {str(e)}")
        return None

def conciliar_todos_los_pares():
    """
    Encuentra y concilia todos los pares de archivos disponibles
    """
    conciliador = ConciliadorBancarioFlexible()
    conciliador.crear_directorios()
    
    pares = conciliador.encontrar_pares_conciliables()
    
    if not pares:
        print("❌ No se encontraron pares de archivos para conciliar")
        return []
    
    resultados = []
    exitosos = 0
    
    print(f"\n🚀 INICIANDO CONCILIACION MASIVA: {len(pares)} pares")
    print("=" * 80)
    
    for i, par in enumerate(pares, 1):
        print(f"\n[{i}/{len(pares)}] Procesando: {par['banco']}-{par['cuenta']}-{par['periodo']}")
        
        resultado = conciliar_un_par(par['banco'], par['cuenta'], par['periodo'])
        
        if resultado:
            exitosos += 1
            resultados.append({
                'par': par,
                'resultado': resultado,
                'status': 'EXITOSO'
            })
        else:
            resultados.append({
                'par': par,
                'resultado': None,
                'status': 'ERROR'
            })
    
    print(f"\n📊 RESUMEN FINAL:")
    print(f"Total procesados: {len(pares)}")
    print(f"Exitosos: {exitosos}")
    print(f"Con errores: {len(pares) - exitosos}")
    
    return resultados

def menu_interactivo():
    """
    Menu interactivo para seleccionar que conciliar
    """
    if not verificar_dependencias():
        print("Por favor instala las dependencias faltantes antes de continuar.")
        return
    
    conciliador = ConciliadorBancarioFlexible()
    conciliador.crear_directorios()
    
    while True:
        print("\n" + "="*60)
        print("🏦 CONCILIADOR BANCARIO - MENU PRINCIPAL")
        print("="*60)
        print("1. 📋 Listar archivos disponibles")
        print("2. 🔍 Conciliar un par especifico")
        print("3. 🚀 Conciliar todos los pares disponibles")
        print("4. 📁 Crear estructura de directorios")
        print("5. ❌ Salir")
        print()
        
        opcion = input("Selecciona una opcion (1-5): ").strip()
        
        if opcion == "1":
            conciliador.listar_archivos_disponibles()
            
        elif opcion == "2":
            pares = conciliador.encontrar_pares_conciliables()
            if not pares:
                print("❌ No hay pares disponibles para conciliar")
                continue
            
            print("\nPares disponibles:")
            for i, par in enumerate(pares, 1):
                print(f"{i}. {par['banco'].upper()} - Cuenta {par['cuenta']} - {par['periodo']}")
            
            try:
                seleccion = int(input(f"\nSelecciona un par (1-{len(pares)}): ")) - 1
                if 0 <= seleccion < len(pares):
                    par = pares[seleccion]
                    conciliar_un_par(par['banco'], par['cuenta'], par['periodo'])
                else:
                    print("❌ Seleccion invalida")
            except ValueError:
                print("❌ Por favor ingresa un numero valido")
                
        elif opcion == "3":
            confirmar = input("¿Confirmas conciliar TODOS los pares? (s/N): ").strip().lower()
            if confirmar == 's':
                conciliar_todos_los_pares()
            else:
                print("Operacion cancelada")
                
        elif opcion == "4":
            conciliador.crear_directorios()
            print("✅ Estructura de directorios creada")
            
        elif opcion == "5":
            print("👋 ¡Hasta luego!")
            break
            
        else:
            print("❌ Opcion no valida. Por favor selecciona 1-5")
        
        input("\nPresiona Enter para continuar...")

# Funciones de conveniencia para uso directo
def conciliar_banco_completo(banco, periodo=None):
    """
    Concilia todas las cuentas de un banco especifico
    """
    conciliador = ConciliadorBancarioFlexible()
    conciliador.crear_directorios()
    
    archivos_cont, archivos_bco = conciliador.encontrar_archivos_por_patron(banco=banco)
    
    # Organizar por cuenta y periodo
    pares_banco = {}
    
    for archivo in archivos_cont:
        info = conciliador.parsear_nombre_archivo(archivo)
        if info and (not periodo or info['periodo'] == periodo):
            clave = f"{info['cuenta']}_{info['periodo']}"
            if clave not in pares_banco:
                pares_banco[clave] = {}
            pares_banco[clave]['cont'] = archivo
            pares_banco[clave]['info'] = info
    
    for archivo in archivos_bco:
        info = conciliador.parsear_nombre_archivo(archivo)
        if info and (not periodo or info['periodo'] == periodo):
            clave = f"{info['cuenta']}_{info['periodo']}"
            if clave in pares_banco:
                pares_banco[clave]['bco'] = archivo
    
    # Conciliar pares completos
    exitosos = 0
    for clave, datos in pares_banco.items():
        if 'cont' in datos and 'bco' in datos:
            info = datos['info']
            resultado = conciliar_un_par(info['banco'], info['cuenta'], info['periodo'])
            if resultado:
                exitosos += 1
    
    print(f"\n✅ Banco {banco.upper()}: {exitosos} conciliaciones exitosas")
    return exitosos

# Funciones principales de ejecucion
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) == 1:
        # Sin argumentos - menu interactivo
        menu_interactivo()
    
    elif len(sys.argv) == 2:
        comando = sys.argv[1].lower()
        
        if comando == "todos":
            # Conciliar todos los pares
            if verificar_dependencias():
                conciliar_todos_los_pares()
        
        elif comando == "listar":
            # Listar archivos disponibles
            conciliador = ConciliadorBancarioFlexible()
            conciliador.crear_directorios()
            conciliador.listar_archivos_disponibles()
        
        elif comando == "directorios":
            # Crear directorios
            conciliador = ConciliadorBancarioFlexible()
            conciliador.crear_directorios()
            print("✅ Estructura de directorios creada")
        
        else:
            # Conciliar banco especifico
            if verificar_dependencias():
                conciliar_banco_completo(comando)
    
    elif len(sys.argv) == 4:
        # Conciliar par especifico: banco cuenta periodo
        banco, cuenta, periodo = sys.argv[1], sys.argv[2], sys.argv[3]
        if verificar_dependencias():
            conciliar_un_par(banco, cuenta, periodo)
    
    else:
        print("💡 MODOS DE USO:")
        print()
        print("1. Menu interactivo:")
        print("   python conciliador_bancario_flexible.py")
        print()
        print("2. Conciliar todos los pares:")
        print("   python conciliador_bancario_flexible.py todos")
        print()
        print("3. Conciliar banco especifico:")
        print("   python conciliador_bancario_flexible.py credi")
        print("   python conciliador_bancario_flexible.py macro")
        print()
        print("4. Conciliar par especifico:")
        print("   python conciliador_bancario_flexible.py credi 01 062025")
        print()
        print("5. Otras utilidades:")
        print("   python conciliador_bancario_flexible.py listar")
        print("   python conciliador_bancario_flexible.py directorios")
        print()
        print("📁 ESTRUCTURA DE DIRECTORIOS ESPERADA:")
        print("C:.")
        print("├───Bancos/")
        print("│       credi_bco_01_062025.xls")
        print("│       macro_bco_01_062025.xls")
        print("├───Contable/")
        print("│       credi_cont_01_062025.xls")
        print("│       macro_cont_01_062025.xls")
        print("└───Procesado/")
        print("        credi_pro_01_062025.xlsx")
        print("        macro_pro_01_062025.xlsx")