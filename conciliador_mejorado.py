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

class ConciliadorMejorado:
    def __init__(self):
        # Pesos ajustados para priorizar monto y fecha sobre concepto
        self.pesos = {
            'fecha': 0.35,     # 35% peso a la fecha (aumentado)
            'monto': 0.45,     # 45% peso al monto (aumentado) 
            'concepto': 0.15,  # 15% peso al concepto (reducido)
            'tipo': 0.05       # 5% peso a compatibilidad de tipo (reducido)
        }
        
        # Parametros de tolerancia
        self.tolerancia_monto_pct = 0.02  # 2% tolerancia para montos (más estricto)
        self.tolerancia_fecha_dias = 2    # 2 dias tolerancia maxima
        
        # Umbral minimo ajustado para compensar menor peso de concepto
        self.umbral_coincidencia = 0.55  # Reducido de 0.6 a 0.55
        
        # Mapeo de conceptos mejorado y más específico para bancos argentinos
        self.mapeo_conceptos = {
            'transferencia': [
                'transferencia', 'transf', 'credito inmediato', 'debin', 
                'acreditacion', 'debito inmediato', 'transferencia entre cuentas',
                'transfer', 'credito por transferencia'
            ],
            'retencion': [
                'retencion', 'ret', 'retencion impositiva', 'retencion ganancias',
                'retencion iva', 'retencion iibb', 'withholding'
            ],
            'debito_automatico': [
                'debito automatico', 'deb aut', 'debito aut', 'pago automatico',
                'debito por servicio', 'cobro automatico'
            ],
            'tarjeta': [
                'tarjeta', 'tc', 'td', 'visa', 'mastercard', 'american express',
                'compra con tarjeta', 'pago tarjeta'
            ],
            'cheque': [
                'cheque', 'ch', 'cheque propio', 'cheque tercero', 'deposito cheque',
                'cobro cheque', 'rechazo cheque'
            ],
            'efectivo': [
                'efectivo', 'extraccion', 'deposito efectivo', 'cajero automatico',
                'atm', 'retiro efectivo'
            ],
            'servicios': [
                'servicio', 'luz', 'gas', 'agua', 'telefono', 'internet',
                'cable', 'expensas', 'alquiler'
            ],
            'comisiones': [
                'comision', 'com', 'mantenimiento', 'gastos', 'cargo',
                'fee', 'tarifa'
            ],
            'impuestos': [
                'impuesto', 'iva', 'ganancias', 'bienes personales', 'iibb',
                'monotributo', 'ingresos brutos'
            ],
            'nomina': [
                'sueldo', 'salario', 'haberes', 'aguinaldo', 'liquidacion',
                'nomina', 'pago empleados'
            ]
        }
        
        # Palabras clave para identificar entidades/empresas comunes
        self.entidades_comunes = [
            'edenor', 'edesur', 'metrogas', 'aba', 'aysa', 'telecom', 'fibertel',
            'claro', 'movistar', 'personal', 'directv', 'flow', 'cablevision',
            'mercadopago', 'mercado pago', 'rapipago', 'pagofacil', 'link',
            'banelco', 'red link', 'prisma', 'first data', 'lapos'
        ]
    
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
                
                # Extraer concepto completo y limpio
                concepto_original = str(row.iloc[5]) if pd.notna(row.iloc[5]) else ''
                
                transaccion = {
                    'fecha': fecha,
                    'concepto': concepto_original,
                    'concepto_normalizado': self.normalizar_concepto_avanzado(concepto_original),
                    'monto': debe if debe > 0 else haber,
                    'tipo': 'DEBE' if debe > 0 else 'HABER',
                    'debe': debe,
                    'haber': haber,
                    'entidades_detectadas': self.extraer_entidades(concepto_original)
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
                
                # Extraer concepto completo y limpio
                concepto_original = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ''
                
                transaccion = {
                    'fecha': fecha,
                    'concepto': concepto_original,
                    'concepto_normalizado': self.normalizar_concepto_avanzado(concepto_original),
                    'monto': debito if debito > 0 else credito,
                    'tipo': 'DEBITO' if debito > 0 else 'CREDITO',
                    'debito': debito,
                    'credito': credito,
                    'entidades_detectadas': self.extraer_entidades(concepto_original)
                }
                bco_data.append(transaccion)
        
        return pd.DataFrame(bco_data)
    
    def normalizar_concepto_avanzado(self, concepto):
        """Normalización avanzada de conceptos"""
        if pd.isna(concepto):
            return ""
        
        concepto = str(concepto).lower().strip()
        
        # Remover caracteres especiales y números de comprobante
        concepto = re.sub(r'[^\w\s]', ' ', concepto)
        concepto = re.sub(r'\b\d{4,}\b', '', concepto)  # Remover números largos (IDs, etc)
        concepto = re.sub(r'\s+', ' ', concepto)
        
        # Remover palabras comunes que no aportan valor
        palabras_ruido = [
            'el', 'la', 'de', 'del', 'y', 'en', 'por', 'con', 'para', 'al', 'los', 'las',
            'pago', 'cobro', 'operacion', 'movimiento', 'transaccion'
        ]
        
        palabras = concepto.split()
        palabras_filtradas = [p for p in palabras if p not in palabras_ruido and len(p) > 2]
        
        return ' '.join(palabras_filtradas)
    
    def extraer_entidades(self, concepto):
        """Extrae entidades conocidas del concepto"""
        if pd.isna(concepto):
            return []
        
        concepto_lower = str(concepto).lower()
        entidades_encontradas = []
        
        for entidad in self.entidades_comunes:
            if entidad in concepto_lower:
                entidades_encontradas.append(entidad)
        
        return entidades_encontradas
    
    def calcular_similitud_fecha(self, fecha1, fecha2):
        """Calcula similitud entre fechas (0-1)"""
        if fecha1 is None or fecha2 is None:
            return 0.0
        
        diferencia_dias = abs((fecha1 - fecha2).days)
        
        if diferencia_dias == 0:
            return 1.0
        elif diferencia_dias == 1:
            return 0.9
        elif diferencia_dias == 2:
            return 0.7
        elif diferencia_dias <= 5:
            return 0.4
        else:
            return 0.0
    
    def calcular_similitud_monto(self, monto1, monto2):
        """Calcula similitud entre montos con mayor precisión"""
        if monto1 == 0 or monto2 == 0:
            return 1.0 if monto1 == monto2 else 0.0
        
        diferencia_pct = abs(monto1 - monto2) / max(monto1, monto2)
        
        if diferencia_pct == 0:
            return 1.0
        elif diferencia_pct <= 0.001:  # 0.1%
            return 0.98
        elif diferencia_pct <= 0.005:  # 0.5%
            return 0.95
        elif diferencia_pct <= 0.01:   # 1%
            return 0.9
        elif diferencia_pct <= 0.02:   # 2%
            return 0.8
        elif diferencia_pct <= 0.05:   # 5%
            return 0.5
        else:
            return 0.0
    
    def calcular_similitud_concepto_avanzada(self, concepto1, concepto2, entidades1, entidades2):
        """Cálculo avanzado de similitud de conceptos"""
        
        # 1. Similitud por entidades comunes
        similitud_entidades = 0.0
        if entidades1 and entidades2:
            entidades_comunes = set(entidades1).intersection(set(entidades2))
            if entidades_comunes:
                similitud_entidades = 0.9  # Alta similitud si comparten entidades
        
        # 2. Similitud textual directa
        c1_norm = self.normalizar_concepto_avanzado(concepto1)
        c2_norm = self.normalizar_concepto_avanzado(concepto2)
        similitud_textual = SequenceMatcher(None, c1_norm, c2_norm).ratio()
        
        # 3. Similitud por mapeo de categorías
        similitud_mapeo = 0.0
        for categoria, keywords in self.mapeo_conceptos.items():
            c1_match = any(kw in c1_norm for kw in keywords)
            c2_match = any(kw in c2_norm for kw in keywords)
            if c1_match and c2_match:
                similitud_mapeo = max(similitud_mapeo, 0.7)
        
        # 4. Similitud por palabras clave comunes (mejorada)
        palabras1 = set(c1_norm.split())
        palabras2 = set(c2_norm.split())
        if palabras1 and palabras2:
            interseccion = len(palabras1.intersection(palabras2))
            union = len(palabras1.union(palabras2))
            similitud_palabras = (interseccion / union) if union > 0 else 0
            
            # Bonificación si hay palabras exactamente iguales importantes
            if interseccion >= 2:
                similitud_palabras += 0.2
        else:
            similitud_palabras = 0
        
        # Tomar el máximo de todas las similitudes
        return max(similitud_entidades, similitud_textual, similitud_mapeo, similitud_palabras)
    
    def calcular_compatibilidad_tipo(self, tipo_cont, tipo_bco):
        """Verifica compatibilidad de tipos contable vs bancario (0-1)"""
        compatible = (
            (tipo_cont == 'DEBE' and tipo_bco == 'CREDITO') or
            (tipo_cont == 'HABER' and tipo_bco == 'DEBITO')
        )
        return 1.0 if compatible else 0.0
    
    def construir_matriz_coincidencia(self, df_cont, df_bco):
        """Construye la matriz de coincidencia entre transacciones"""
        n_cont = len(df_cont)
        n_bco = len(df_bco)
        
        print(f"🧮 Construyendo matriz {n_cont}x{n_bco}...")
        
        # Inicializar matriz
        matriz = np.zeros((n_cont, n_bco))
        
        # Matrices de componentes para análisis
        self.matriz_fecha = np.zeros((n_cont, n_bco))
        self.matriz_monto = np.zeros((n_cont, n_bco))
        self.matriz_concepto = np.zeros((n_cont, n_bco))
        self.matriz_tipo = np.zeros((n_cont, n_bco))
        
        # Calcular scores para cada par
        for i, row_cont in df_cont.iterrows():
            if i % 50 == 0:  # Mostrar progreso
                print(f"  Procesando fila {i+1}/{n_cont}...")
            
            for j, row_bco in df_bco.iterrows():
                
                # Calcular similitudes individuales
                sim_fecha = self.calcular_similitud_fecha(row_cont['fecha'], row_bco['fecha'])
                sim_monto = self.calcular_similitud_monto(row_cont['monto'], row_bco['monto'])
                sim_concepto = self.calcular_similitud_concepto_avanzada(
                    row_cont['concepto'], row_bco['concepto'],
                    row_cont['entidades_detectadas'], row_bco['entidades_detectadas']
                )
                sim_tipo = self.calcular_compatibilidad_tipo(row_cont['tipo'], row_bco['tipo'])
                
                # Guardar componentes
                self.matriz_fecha[i, j] = sim_fecha
                self.matriz_monto[i, j] = sim_monto
                self.matriz_concepto[i, j] = sim_concepto
                self.matriz_tipo[i, j] = sim_tipo
                
                # Calcular score final ponderado
                score_final = (
                    self.pesos['fecha'] * sim_fecha +
                    self.pesos['monto'] * sim_monto +
                    self.pesos['concepto'] * sim_concepto +
                    self.pesos['tipo'] * sim_tipo
                )
                
                matriz[i, j] = score_final
        
        self.matriz_coincidencia = matriz
        print(f"✅ Matriz construida. Score máximo: {np.max(matriz):.3f}")
        return matriz
    
    def encontrar_coincidencias_optimas(self, df_cont, df_bco):
        """Encuentra las mejores coincidencias usando matriz"""
        
        # Construir matriz de coincidencia
        matriz = self.construir_matriz_coincidencia(df_cont, df_bco)
        
        if SCIPY_AVAILABLE:
            # Usar algoritmo Hungarian (óptimo)
            print("🎯 Aplicando algoritmo Hungarian...")
            matriz_costo = 1 - matriz
            indices_cont, indices_bco = linear_sum_assignment(matriz_costo)
        else:
            # Usar algoritmo greedy simple
            print("🎯 Aplicando algoritmo greedy...")
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
                    'score_fecha': self.matriz_fecha[i, j],
                    'score_monto': self.matriz_monto[i, j],
                    'score_concepto': self.matriz_concepto[i, j],
                    'score_tipo': self.matriz_tipo[i, j],
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
        """Clasifica el nivel de confianza basado en el score ajustado"""
        if score >= 0.85:
            return "MUY_ALTA"
        elif score >= 0.75:
            return "ALTA"
        elif score >= 0.65:
            return "MEDIA"
        elif score >= 0.55:
            return "BAJA"
        else:
            return "MUY_BAJA"
    
    def analizar_no_conciliadas(self, df_cont, df_bco, coincidencias):
        """Analiza patrones en transacciones no conciliadas"""
        
        indices_cont_conciliados = set([coinc['cont_index'] for coinc in coincidencias])
        indices_bco_conciliados = set([coinc['bco_index'] for coinc in coincidencias])
        
        cont_no_conciliadas = df_cont[~df_cont.index.isin(indices_cont_conciliados)]
        bco_no_conciliadas = df_bco[~df_bco.index.isin(indices_bco_conciliados)]
        
        print("\n📋 ANÁLISIS DE NO CONCILIADAS:")
        print("-" * 40)
        
        # Análisis por rangos de monto
        print("📊 Contables no conciliadas por rango de monto:")
        if len(cont_no_conciliadas) > 0:
            for rango, grupo in cont_no_conciliadas.groupby(pd.cut(cont_no_conciliadas['monto'], 
                                                                   bins=[0, 1000, 10000, 100000, float('inf')], 
                                                                   labels=['<$1K', '$1K-$10K', '$10K-$100K', '>$100K'])):
                print(f"  {rango}: {len(grupo)} transacciones")
        
        print("\n📊 Bancarias no conciliadas por rango de monto:")
        if len(bco_no_conciliadas) > 0:
            for rango, grupo in bco_no_conciliadas.groupby(pd.cut(bco_no_conciliadas['monto'], 
                                                                  bins=[0, 1000, 10000, 100000, float('inf')], 
                                                                  labels=['<$1K', '$1K-$10K', '$10K-$100K', '>$100K'])):
                print(f"  {rango}: {len(grupo)} transacciones")
        
        return cont_no_conciliadas, bco_no_conciliadas
    
    def generar_archivo_procesado_mejorado(self, df_cont, df_bco, coincidencias, info_archivo):
        """Genera el archivo procesado mejorado con más detalles"""
        
        archivo_salida = info_archivo['archivo_salida']
        
        # Crear directorio Procesado si no existe
        os.makedirs("Procesado", exist_ok=True)
        
        print(f"📝 Generando: {os.path.basename(archivo_salida)}")
        
        # Analizar no conciliadas
        cont_no_conciliadas, bco_no_conciliadas = self.analizar_no_conciliadas(df_cont, df_bco, coincidencias)
        
        # Crear sets de índices conciliados
        indices_cont_conciliados = set([coinc['cont_index'] for coinc in coincidencias])
        indices_bco_conciliados = set([coinc['bco_index'] for coinc in coincidencias])
        
        # Usar opciones específicas para evitar errores
        with pd.ExcelWriter(archivo_salida, engine='openpyxl', options={'remove_timezone': True}) as writer:
            
            # HOJA 1: Coincidencias Encontradas (mejorada)
            if coincidencias:
                coincidencias_data = []
                for coinc in coincidencias:
                    row_cont = df_cont.iloc[coinc['cont_index']]
                    row_bco = df_bco.iloc[coinc['bco_index']]
                    
                    # Limpiar datos para evitar errores
                    concepto_cont = str(row_cont['concepto'])[:200] if pd.notna(row_cont['concepto']) else ''
                    concepto_bco = str(row_bco['concepto'])[:200] if pd.notna(row_bco['concepto']) else ''
                    
                    coincidencias_data.append({
                        'Estado': 'CONCILIADO',
                        'Nivel_Confianza': coinc['nivel_confianza'],
                        'Score_Total': round(float(coinc['score_total']), 3),
                        'Score_Fecha': round(float(coinc['score_fecha']), 3),
                        'Score_Monto': round(float(coinc['score_monto']), 3),
                        'Score_Concepto': round(float(coinc['score_concepto']), 3),
                        'Score_Tipo': round(float(coinc['score_tipo']), 3),
                        'Fecha_Cont': row_cont['fecha'].strftime('%d/%m/%Y'),
                        'Concepto_Cont': concepto_cont,
                        'Monto_Cont': float(row_cont['monto']),
                        'Tipo_Cont': str(row_cont['tipo']),
                        'Entidades_Cont': ', '.join(row_cont['entidades_detectadas']) if row_cont['entidades_detectadas'] else '',
                        'Fecha_Bco': row_bco['fecha'].strftime('%d/%m/%Y'),
                        'Concepto_Bco': concepto_bco,
                        'Monto_Bco': float(row_bco['monto']),
                        'Tipo_Bco': str(row_bco['tipo']),
                        'Entidades_Bco': ', '.join(row_bco['entidades_detectadas']) if row_bco['entidades_detectadas'] else '',
                        'Diferencia_Monto': round(float(abs(row_cont['monto'] - row_bco['monto'])), 2),
                        'Diferencia_Dias': int(abs((row_cont['fecha'] - row_bco['fecha']).days))
                    })
                
                df_coincidencias = pd.DataFrame(coincidencias_data)
                df_coincidencias = df_coincidencias.sort_values('Score_Total', ascending=False)
            else:
                # DataFrame vacío con todas las columnas
                df_coincidencias = pd.DataFrame(columns=[
                    'Estado', 'Nivel_Confianza', 'Score_Total', 'Score_Fecha', 'Score_Monto',
                    'Score_Concepto', 'Score_Tipo', 'Fecha_Cont', 'Concepto_Cont', 'Monto_Cont',
                    'Tipo_Cont', 'Entidades_Cont', 'Fecha_Bco', 'Concepto_Bco', 'Monto_Bco',
                    'Tipo_Bco', 'Entidades_Bco', 'Diferencia_Monto', 'Diferencia_Dias'
                ])
            
            df_coincidencias.to_excel(writer, sheet_name='Coincidencias', index=False)
            
            # HOJA 2: Contables Sin Conciliar (mejorada)
            if len(cont_no_conciliadas) > 0:
                cont_pendientes_data = []
                for idx, row in cont_no_conciliadas.iterrows():
                    concepto_limpio = str(row['concepto'])[:200] if pd.notna(row['concepto']) else ''
                    
                    cont_pendientes_data.append({
                        'Estado': 'PENDIENTE_CONCILIAR',
                        'Fecha': row['fecha'].strftime('%d/%m/%Y'),
                        'Concepto': concepto_limpio,
                        'Concepto_Normalizado': row['concepto_normalizado'],
                        'Monto': float(row['monto']),
                        'Tipo': str(row['tipo']),
                        'Entidades_Detectadas': ', '.join(row['entidades_detectadas']) if row['entidades_detectadas'] else '',
                        'Posibles_Causas': self.sugerir_causa_no_conciliacion(row),
                        'Observaciones': 'Sin coincidencia en extracto bancario'
                    })
                
                df_cont_pendientes = pd.DataFrame(cont_pendientes_data)
            else:
                df_cont_pendientes = pd.DataFrame(columns=[
                    'Estado', 'Fecha', 'Concepto', 'Concepto_Normalizado', 'Monto', 'Tipo',
                    'Entidades_Detectadas', 'Posibles_Causas', 'Observaciones'
                ])
            
            df_cont_pendientes