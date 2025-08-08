import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from difflib import SequenceMatcher
import os
import glob

# Importar scipy solo si está disponible
try:
    from scipy.optimize import linear_sum_assignment
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

class ConciliadorAutomatico:
    def __init__(self):
        # Pesos para el calculo del score de coincidencia
        self.pesos = {
            'fecha': 0.3,      # 30% peso a la fecha
            'monto': 0.4,      # 40% peso al monto  
            'concepto': 0.2,   # 20% peso al concepto
            'tipo': 0.1        # 10% peso a compatibilidad de tipo
        }
        
        # Parametros de tolerancia
        self.tolerancia_monto_pct = 0.05  # 5% tolerancia para montos
        self.tolerancia_fecha_dias = 3    # 3 dias tolerancia maxima
        
        # Umbral minimo para considerar una coincidencia valida
        self.umbral_coincidencia = 0.6
        
        # Mapeo de conceptos
        self.mapeo_conceptos = {
            'transferencia': ['transferencia', 'transf', 'credito inmediato', 'debin'],
            'retencion': ['retencion', 'ret'],
            'debito': ['debito', 'deb', 'debito automatico'],
            'credito': ['credito', 'cred', 'acreditacion'],
            'iva': ['iva', 'impuesto'],
            'cheque': ['cheque', 'ch'],
            'mercado_pago': ['mercado pago', 'mp', 'mercadopago'],
            'salario': ['salario', 'sueldo', 'haberes'],
            'intereses': ['intereses', 'int'],
            'comisiones': ['comision', 'com', 'gastos']
        }
    
    def parsear_nombre_archivo(self, nombre_archivo):
        """Extrae banco, cuenta y periodo del nombre del archivo"""
        nombre_base = os.path.splitext(os.path.basename(nombre_archivo))[0]
        partes = nombre_base.split('_')
        
        if len(partes) >= 4:
            return {
                'banco': partes[0],
                'tipo': partes[1],
                'cuenta': partes[2],
                'periodo': partes[3]
            }
        return None
    
    def encontrar_pares_archivos(self):
        """Encuentra automáticamente todos los pares de archivos para conciliar"""
        pares = []
        
        # Buscar archivos contables
        archivos_cont = glob.glob("Contable/*_cont_*.xls")
        
        for archivo_cont in archivos_cont:
            info = self.parsear_nombre_archivo(archivo_cont)
            if info:
                # Buscar archivo bancario correspondiente
                archivo_bco = f"Bancos/{info['banco']}_bco_{info['cuenta']}_{info['periodo']}.xls"
                if os.path.exists(archivo_bco):
                    archivo_salida = f"Procesado/{info['banco']}_pro_{info['cuenta']}_{info['periodo']}.xlsx"
                    
                    pares.append({
                        'banco': info['banco'],
                        'cuenta': info['cuenta'],
                        'periodo': info['periodo'],
                        'archivo_cont': archivo_cont,
                        'archivo_bco': archivo_bco,
                        'archivo_salida': archivo_salida
                    })
        
        return pares
    
    def cargar_archivo_contable(self, archivo_cont):
        """Carga y limpia archivo contable"""
        print(f"📄 Cargando: {os.path.basename(archivo_cont)}")
        
        # Intentar cargar con diferentes engines
        try:
            df_raw = pd.read_excel(archivo_cont, header=None, engine='xlrd')
        except Exception:
            try:
                df_raw = pd.read_excel(archivo_cont, header=None, engine='openpyxl')
            except Exception as e:
                raise Exception(f"No se pudo cargar {archivo_cont}: {e}")
        
        # Limpiar datos contables
        cont_data = []
        for i in range(2, len(df_raw)):
            row = df_raw.iloc[i]
            if pd.notna(row.iloc[4]):  # Si tiene fecha
                fecha_excel = row.iloc[4]
                if isinstance(fecha_excel, (int, float)):
                    fecha = datetime(1899, 12, 30) + timedelta(days=fecha_excel)
                else:
                    fecha = pd.to_datetime(fecha_excel)
                
                debe = float(row.iloc[9]) if pd.notna(row.iloc[9]) else 0
                haber = float(row.iloc[11]) if pd.notna(row.iloc[11]) else 0
                
                transaccion = {
                    'fecha': fecha,
                    'concepto': str(row.iloc[5]) if pd.notna(row.iloc[5]) else '',
                    'monto': debe if debe > 0 else haber,
                    'tipo': 'DEBE' if debe > 0 else 'HABER',
                    'debe': debe,
                    'haber': haber
                }
                cont_data.append(transaccion)
        
        return pd.DataFrame(cont_data)
    
    def cargar_archivo_bancario(self, archivo_bco):
        """Carga y limpia archivo bancario"""
        print(f"🏦 Cargando: {os.path.basename(archivo_bco)}")
        
        # Intentar cargar con diferentes engines
        try:
            df_raw = pd.read_excel(archivo_bco, header=None, engine='xlrd')
        except Exception:
            try:
                df_raw = pd.read_excel(archivo_bco, header=None, engine='openpyxl')
            except Exception as e:
                raise Exception(f"No se pudo cargar {archivo_bco}: {e}")
        
        # Limpiar datos bancarios
        bco_data = []
        for i in range(1, len(df_raw)):
            row = df_raw.iloc[i]
            if pd.notna(row.iloc[1]):  # Si tiene fecha
                fecha = pd.to_datetime(str(row.iloc[1]), format='%d/%m/%Y')
                debito = float(row.iloc[4]) if pd.notna(row.iloc[4]) else 0
                credito = float(row.iloc[5]) if pd.notna(row.iloc[5]) else 0
                
                transaccion = {
                    'fecha': fecha,
                    'concepto': str(row.iloc[2]) if pd.notna(row.iloc[2]) else '',
                    'monto': debito if debito > 0 else credito,
                    'tipo': 'DEBITO' if debito > 0 else 'CREDITO',
                    'debito': debito,
                    'credito': credito
                }
                bco_data.append(transaccion)
        
        return pd.DataFrame(bco_data)
    
    def calcular_similitud_fecha(self, fecha1, fecha2):
        """Calcula similitud entre fechas (0-1)"""
        if fecha1 is None or fecha2 is None:
            return 0.0
        
        diferencia_dias = abs((fecha1 - fecha2).days)
        
        if diferencia_dias == 0:
            return 1.0
        elif diferencia_dias == 1:
            return 0.8
        elif diferencia_dias <= 3:
            return 0.6
        else:
            return 0.0
    
    def calcular_similitud_monto(self, monto1, monto2):
        """Calcula similitud entre montos (0-1)"""
        if monto1 == 0 or monto2 == 0:
            return 1.0 if monto1 == monto2 else 0.0
        
        diferencia_pct = abs(monto1 - monto2) / max(monto1, monto2)
        
        if diferencia_pct == 0:
            return 1.0
        elif diferencia_pct <= 0.01:
            return 0.95
        elif diferencia_pct <= 0.02:
            return 0.9
        elif diferencia_pct <= 0.05:
            return 0.7
        else:
            return 0.0
    
    def calcular_similitud_concepto(self, concepto1, concepto2):
        """Calcula similitud entre conceptos (0-1)"""
        c1_norm = self.normalizar_concepto(concepto1)
        c2_norm = self.normalizar_concepto(concepto2)
        
        # Similitud textual directa
        similitud_textual = SequenceMatcher(None, c1_norm, c2_norm).ratio()
        
        # Similitud por mapeo de categorias
        similitud_mapeo = 0.0
        for categoria, keywords in self.mapeo_conceptos.items():
            c1_match = any(kw in c1_norm for kw in keywords)
            c2_match = any(kw in c2_norm for kw in keywords)
            if c1_match and c2_match:
                similitud_mapeo = max(similitud_mapeo, 0.8)
        
        return max(similitud_textual, similitud_mapeo)
    
    def calcular_compatibilidad_tipo(self, tipo_cont, tipo_bco):
        """Verifica compatibilidad de tipos contable vs bancario (0-1)"""
        compatible = (
            (tipo_cont == 'DEBE' and tipo_bco == 'CREDITO') or
            (tipo_cont == 'HABER' and tipo_bco == 'DEBITO')
        )
        return 1.0 if compatible else 0.0
    
    def normalizar_concepto(self, concepto):
        """Normaliza conceptos para comparacion"""
        concepto = str(concepto).lower().strip()
        concepto = re.sub(r'[^\w\s]', ' ', concepto)
        concepto = re.sub(r'\s+', ' ', concepto)
        return concepto
    
    def construir_matriz_coincidencia(self, df_cont, df_bco):
        """Construye la matriz de coincidencia entre transacciones"""
        n_cont = len(df_cont)
        n_bco = len(df_bco)
        
        print(f"🧮 Construyendo matriz {n_cont}x{n_bco}...")
        
        # Inicializar matriz
        matriz = np.zeros((n_cont, n_bco))
        
        # Calcular scores para cada par
        for i, row_cont in df_cont.iterrows():
            for j, row_bco in df_bco.iterrows():
                
                # Calcular similitudes individuales
                sim_fecha = self.calcular_similitud_fecha(row_cont['fecha'], row_bco['fecha'])
                sim_monto = self.calcular_similitud_monto(row_cont['monto'], row_bco['monto'])
                sim_concepto = self.calcular_similitud_concepto(row_cont['concepto'], row_bco['concepto'])
                sim_tipo = self.calcular_compatibilidad_tipo(row_cont['tipo'], row_bco['tipo'])
                
                # Calcular score final ponderado
                score_final = (
                    self.pesos['fecha'] * sim_fecha +
                    self.pesos['monto'] * sim_monto +
                    self.pesos['concepto'] * sim_concepto +
                    self.pesos['tipo'] * sim_tipo
                )
                
                matriz[i, j] = score_final
        
        return matriz
    
    def encontrar_coincidencias_optimas(self, df_cont, df_bco):
        """Encuentra las mejores coincidencias usando matriz"""
        
        # Construir matriz de coincidencia
        matriz = self.construir_matriz_coincidencia(df_cont, df_bco)
        
        if SCIPY_AVAILABLE:
            # Usar algoritmo Hungarian (óptimo)
            matriz_costo = 1 - matriz
            indices_cont, indices_bco = linear_sum_assignment(matriz_costo)
        else:
            # Usar algoritmo greedy simple
            indices_cont, indices_bco = self._asignacion_greedy(matriz)
        
        # Filtrar coincidencias por umbral minimo
        coincidencias = []
        for i, j in zip(indices_cont, indices_bco):
            score = matriz[i, j]
            if score >= self.umbral_coincidencia:
                coincidencias.append({
                    'cont_index': i,
                    'bco_index': j,
                    'score_total': score,
                    'nivel_confianza': self.clasificar_nivel_confianza(score)
                })
        
        return coincidencias
    
    def _asignacion_greedy(self, matriz):
        """Algoritmo greedy simple cuando scipy no está disponible"""
        used_cont = set()
        used_bco = set()
        indices_cont = []
        indices_bco = []
        
        # Encontrar pares en orden de score descendente
        coords = [(i, j) for i in range(matriz.shape[0]) for j in range(matriz.shape[1])]
        coords.sort(key=lambda x: matriz[x[0], x[1]], reverse=True)
        
        for i, j in coords:
            if i not in used_cont and j not in used_bco and matriz[i, j] > 0:
                indices_cont.append(i)
                indices_bco.append(j)
                used_cont.add(i)
                used_bco.add(j)
        
        return np.array(indices_cont), np.array(indices_bco)
    
    def clasificar_nivel_confianza(self, score):
        """Clasifica el nivel de confianza basado en el score"""
        if score >= 0.9:
            return "MUY_ALTA"
        elif score >= 0.8:
            return "ALTA"
        elif score >= 0.7:
            return "MEDIA"
        elif score >= 0.6:
            return "BAJA"
        else:
            return "MUY_BAJA"
    
    def generar_archivo_procesado(self, df_cont, df_bco, coincidencias, info_archivo):
        """Genera el archivo procesado principal (.xlsx)"""
        
        archivo_salida = info_archivo['archivo_salida']
        
        # Crear directorio Procesado si no existe
        os.makedirs("Procesado", exist_ok=True)
        
        print(f"📝 Generando: {os.path.basename(archivo_salida)}")
        
        # Crear sets de índices conciliados
        indices_cont_conciliados = set([coinc['cont_index'] for coinc in coincidencias])
        indices_bco_conciliados = set([coinc['bco_index'] for coinc in coincidencias])
        
        with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
            
            # HOJA 1: Coincidencias Encontradas
            coincidencias_data = []
            for coinc in coincidencias:
                row_cont = df_cont.iloc[coinc['cont_index']]
                row_bco = df_bco.iloc[coinc['bco_index']]
                
                coincidencias_data.append({
                    'Estado': 'CONCILIADO',
                    'Nivel_Confianza': coinc['nivel_confianza'],
                    'Score_Total': round(coinc['score_total'], 3),
                    'Fecha_Cont': row_cont['fecha'].strftime('%d/%m/%Y'),
                    'Concepto_Cont': row_cont['concepto'],
                    'Monto_Cont': row_cont['monto'],
                    'Tipo_Cont': row_cont['tipo'],
                    'Debe_Cont': row_cont.get('debe', 0),
                    'Haber_Cont': row_cont.get('haber', 0),
                    'Fecha_Bco': row_bco['fecha'].strftime('%d/%m/%Y'),
                    'Concepto_Bco': row_bco['concepto'],
                    'Monto_Bco': row_bco['monto'],
                    'Tipo_Bco': row_bco['tipo'],
                    'Debito_Bco': row_bco.get('debito', 0),
                    'Credito_Bco': row_bco.get('credito', 0),
                    'Diferencia_Monto': abs(row_cont['monto'] - row_bco['monto']),
                    'Diferencia_Dias': abs((row_cont['fecha'] - row_bco['fecha']).days)
                })
            
            df_coincidencias = pd.DataFrame(coincidencias_data)
            df_coincidencias = df_coincidencias.sort_values('Score_Total', ascending=False)
            df_coincidencias.to_excel(writer, sheet_name='Coincidencias', index=False)
            
            # HOJA 2: Contables Sin Conciliar
            cont_sin_conciliar = df_cont[~df_cont.index.isin(indices_cont_conciliados)].copy()
            
            cont_pendientes_data = []
            for idx, row in cont_sin_conciliar.iterrows():
                cont_pendientes_data.append({
                    'Estado': 'PENDIENTE_CONCILIAR',
                    'Fecha': row['fecha'].strftime('%d/%m/%Y'),
                    'Concepto': row['concepto'],
                    'Monto': row['monto'],
                    'Tipo': row['tipo'],
                    'Debe': row.get('debe', 0),
                    'Haber': row.get('haber', 0),
                    'Observaciones': 'Sin coincidencia en extracto bancario'
                })
            
            df_cont_pendientes = pd.DataFrame(cont_pendientes_data)
            df_cont_pendientes.to_excel(writer, sheet_name='Contables_Pendientes', index=False)
            
            # HOJA 3: Bancarias Sin Conciliar
            bco_sin_conciliar = df_bco[~df_bco.index.isin(indices_bco_conciliados)].copy()
            
            bco_pendientes_data = []
            for idx, row in bco_sin_conciliar.iterrows():
                bco_pendientes_data.append({
                    'Estado': 'PENDIENTE_REGISTRAR',
                    'Fecha': row['fecha'].strftime('%d/%m/%Y'),
                    'Concepto': row['concepto'],
                    'Monto': row['monto'],
                    'Tipo': row['tipo'],
                    'Debito': row.get('debito', 0),
                    'Credito': row.get('credito', 0),
                    'Observaciones': 'Sin registro en contabilidad'
                })
            
            df_bco_pendientes = pd.DataFrame(bco_pendientes_data)
            df_bco_pendientes.to_excel(writer, sheet_name='Bancarias_Pendientes', index=False)
            
            # HOJA 4: Resumen Ejecutivo
            total_cont = len(df_cont)
            total_bco = len(df_bco)
            total_conciliado = len(coincidencias)
            
            # Sumar montos
            monto_conciliado_cont = sum([df_cont.iloc[c['cont_index']]['monto'] for c in coincidencias])
            monto_pendiente_cont = cont_sin_conciliar['monto'].sum() if len(cont_sin_conciliar) > 0 else 0
            monto_pendiente_bco = bco_sin_conciliar['monto'].sum() if len(bco_sin_conciliar) > 0 else 0
            
            # Distribución por confianza
            distribucion_confianza = {}
            for coinc in coincidencias:
                nivel = coinc['nivel_confianza']
                distribucion_confianza[nivel] = distribucion_confianza.get(nivel, 0) + 1
            
            resumen_data = [
                ['=== INFORMACIÓN GENERAL ===', ''],
                ['Banco', info_archivo['banco'].upper()],
                ['Cuenta', info_archivo['cuenta']],
                ['Período', info_archivo['periodo']],
                ['Fecha Procesamiento', datetime.now().strftime('%d/%m/%Y %H:%M:%S')],
                ['Método', 'Matriz de Coincidencia Automática'],
                ['', ''],
                ['=== ESTADÍSTICAS ===', ''],
                ['Total Transacciones Contables', total_cont],
                ['Total Transacciones Bancarias', total_bco],
                ['Total Conciliadas', total_conciliado],
                ['Contables Pendientes', len(cont_sin_conciliar)],
                ['Bancarias Pendientes', len(bco_sin_conciliar)],
                ['', ''],
                ['=== PORCENTAJES ===', ''],
                ['% Conciliación Contables', f"{(total_conciliado/total_cont)*100:.1f}%" if total_cont > 0 else "0%"],
                ['% Conciliación Bancarias', f"{(total_conciliado/total_bco)*100:.1f}%" if total_bco > 0 else "0%"],
                ['', ''],
                ['=== MONTOS ===', ''],
                ['Monto Conciliado', f"${monto_conciliado_cont:,.2f}"],
                ['Monto Pendiente Contable', f"${monto_pendiente_cont:,.2f}"],
                ['Monto Pendiente Bancario', f"${monto_pendiente_bco:,.2f}"],
                ['', ''],
                ['=== CONFIANZA ===', ''],
            ]
            
            for nivel in ['MUY_ALTA', 'ALTA', 'MEDIA', 'BAJA']:
                if nivel in distribucion_confianza:
                    resumen_data.append([f'{nivel}', distribucion_confianza[nivel]])
            
            df_resumen = pd.DataFrame(resumen_data, columns=['Métrica', 'Valor'])
            df_resumen.to_excel(writer, sheet_name='Resumen_Ejecutivo', index=False)
        
        return archivo_salida
    
    def conciliar_par(self, info_archivo):
        """Concilia un par específico de archivos"""
        try:
            # Cargar archivos
            df_cont = self.cargar_archivo_contable(info_archivo['archivo_cont'])
            df_bco = self.cargar_archivo_bancario(info_archivo['archivo_bco'])
            
            # Encontrar coincidencias
            print(f"🎯 Buscando coincidencias...")
            coincidencias = self.encontrar_coincidencias_optimas(df_cont, df_bco)
            
            # Generar archivo procesado
            archivo_salida = self.generar_archivo_procesado(df_cont, df_bco, coincidencias, info_archivo)
            
            # Mostrar resultados
            print(f"✅ Conciliado: {len(coincidencias)}/{len(df_cont)} contables ({len(coincidencias)/len(df_cont)*100:.1f}%)")
            print(f"📄 Archivo: {os.path.basename(archivo_salida)}")
            
            return {
                'exito': True,
                'archivo_salida': archivo_salida,
                'total_cont': len(df_cont),
                'total_bco': len(df_bco),
                'conciliadas': len(coincidencias)
            }
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return {'exito': False, 'error': str(e)}
    
    def procesar_todos(self):
        """Procesa automáticamente todos los pares de archivos encontrados"""
        
        print("🚀 CONCILIADOR AUTOMÁTICO")
        print("=" * 50)
        
        # Crear directorios si no existen
        os.makedirs("Procesado", exist_ok=True)
        
        # Encontrar pares de archivos
        pares = self.encontrar_pares_archivos()
        
        if not pares:
            print("❌ No se encontraron pares de archivos para conciliar")
            print("💡 Verifica que existan archivos en:")
            print("   - Contable/*_cont_*.xls")
            print("   - Bancos/*_bco_*.xls")
            return []
        
        print(f"📋 Encontrados {len(pares)} pares para conciliar")
        
        resultados = []
        exitosos = 0
        
        for i, par in enumerate(pares, 1):
            print(f"\n[{i}/{len(pares)}] {par['banco'].upper()}-{par['cuenta']}-{par['periodo']}")
            print("-" * 40)
            
            resultado = self.conciliar_par(par)
            resultado['info'] = par
            resultados.append(resultado)
            
            if resultado['exito']:
                exitosos += 1
        
        # Resumen final
        print(f"\n📊 RESUMEN FINAL")
        print("=" * 50)
        print(f"Total procesados: {len(pares)}")
        print(f"Exitosos: {exitosos}")
        print(f"Con errores: {len(pares) - exitosos}")
        
        if exitosos > 0:
            print(f"\n✅ Archivos generados en carpeta 'Procesado/':")
            for resultado in resultados:
                if resultado['exito']:
                    info = resultado['info']
                    print(f"   📄 {info['banco']}_pro_{info['cuenta']}_{info['periodo']}.xlsx")
        
        return resultados

def main():
    """Función principal - ejecuta conciliación automática"""
    conciliador = ConciliadorAutomatico()
    resultados = conciliador.procesar_todos()
    
    if not resultados:
        print("\n💡 INSTRUCCIONES:")
        print("1. Coloca archivos contables en: Contable/")
        print("2. Coloca archivos bancarios en: Bancos/")
        print("3. Formato de nombres: banco_tipo_cuenta_periodo.xls")
        print("   Ejemplo: credi_cont_01_062025.xls")
        print("            credi_bco_01_062025.xls")

if __name__ == "__main__":
    main()