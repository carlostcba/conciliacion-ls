import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from difflib import SequenceMatcher
import os

# Verificar dependencias necesarias
def verificar_dependencias():
    """Verifica dependencias necesarias para el conciliador de matriz"""
    dependencias_faltantes = []
    
    try:
        import xlrd
    except ImportError:
        dependencias_faltantes.append('xlrd>=2.0.1')
    
    try:
        from scipy.optimize import linear_sum_assignment
    except ImportError:
        dependencias_faltantes.append('scipy')
    
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
    except ImportError:
        print("⚠️  matplotlib y seaborn opcionales para gráficos no disponibles")
    
    if dependencias_faltantes:
        print("❌ DEPENDENCIAS FALTANTES:")
        print("Ejecuta este comando para instalar:")
        print(f"   pip install {' '.join(dependencias_faltantes)}")
        return False
    
    return True

# Importar scipy solo si está disponible
try:
    from scipy.optimize import linear_sum_assignment
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("⚠️  scipy no disponible. Usando algoritmo de asignación simple.")

# Importar matplotlib y seaborn solo si están disponibles
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    PLOT_AVAILABLE = True
except ImportError:
    PLOT_AVAILABLE = False

class ConciliadorMatriz:
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
        elif diferencia_dias <= 7:
            return 0.3
        else:
            return 0.0
    
    def calcular_similitud_monto(self, monto1, monto2):
        """Calcula similitud entre montos (0-1)"""
        if monto1 == 0 or monto2 == 0:
            return 1.0 if monto1 == monto2 else 0.0
        
        diferencia_pct = abs(monto1 - monto2) / max(monto1, monto2)
        
        if diferencia_pct == 0:
            return 1.0
        elif diferencia_pct <= 0.01:  # 1%
            return 0.95
        elif diferencia_pct <= 0.02:  # 2%
            return 0.9
        elif diferencia_pct <= 0.05:  # 5%
            return 0.7
        elif diferencia_pct <= 0.10:  # 10%
            return 0.4
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
        
        # Similitud por palabras clave comunes
        palabras1 = set(c1_norm.split())
        palabras2 = set(c2_norm.split())
        if palabras1 and palabras2:
            interseccion = len(palabras1.intersection(palabras2))
            union = len(palabras1.union(palabras2))
            similitud_palabras = interseccion / union if union > 0 else 0
        else:
            similitud_palabras = 0
        
        return max(similitud_textual, similitud_mapeo, similitud_palabras)
    
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
        
        print(f"Construyendo matriz de coincidencia {n_cont}x{n_bco}...")
        
        # Inicializar matriz
        matriz = np.zeros((n_cont, n_bco))
        
        # Matrices de componentes para analisis
        self.matriz_fecha = np.zeros((n_cont, n_bco))
        self.matriz_monto = np.zeros((n_cont, n_bco))
        self.matriz_concepto = np.zeros((n_cont, n_bco))
        self.matriz_tipo = np.zeros((n_cont, n_bco))
        
        # Calcular scores para cada par
        for i, row_cont in df_cont.iterrows():
            for j, row_bco in df_bco.iterrows():
                
                # Calcular similitudes individuales
                sim_fecha = self.calcular_similitud_fecha(row_cont['fecha'], row_bco['fecha'])
                sim_monto = self.calcular_similitud_monto(row_cont['monto'], row_bco['monto'])
                sim_concepto = self.calcular_similitud_concepto(row_cont['concepto'], row_bco['concepto'])
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
        print(f"Matriz construida. Score maximo: {np.max(matriz):.3f}")
        return matriz
    
    def encontrar_coincidencias_optimas(self, df_cont, df_bco):
        """Encuentra la asignacion optima usando algoritmo Hungarian o alternativo"""
        
        # Construir matriz de coincidencia
        matriz = self.construir_matriz_coincidencia(df_cont, df_bco)
        
        if SCIPY_AVAILABLE:
            # Usar algoritmo Hungarian (óptimo)
            print("Aplicando algoritmo Hungarian (óptimo)...")
            matriz_costo = 1 - matriz
            indices_cont, indices_bco = linear_sum_assignment(matriz_costo)
        else:
            # Usar algoritmo greedy simple
            print("Aplicando algoritmo greedy (subóptimo)...")
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
        
        print(f"Coincidencias encontradas: {len(coincidencias)}")
        if coincidencias:
            print(f"Score promedio: {np.mean([c['score_total'] for c in coincidencias]):.3f}")
        
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
    
    def analizar_matriz(self, df_cont, df_bco, mostrar_graficos=True):
        """Analiza la matriz de coincidencia y genera estadisticas"""
        
        if not hasattr(self, 'matriz_coincidencia'):
            self.construir_matriz_coincidencia(df_cont, df_bco)
        
        matriz = self.matriz_coincidencia
        
        print("\n📊 ANALISIS DE MATRIZ DE COINCIDENCIA")
        print("=" * 50)
        
        # Estadisticas generales
        print(f"Dimensiones: {matriz.shape}")
        print(f"Score maximo global: {np.max(matriz):.3f}")
        print(f"Score promedio: {np.mean(matriz):.3f}")
        print(f"Score mediano: {np.median(matriz):.3f}")
        print(f"Scores > umbral ({self.umbral_coincidencia}): {np.sum(matriz >= self.umbral_coincidencia)}")
        
        # Analisis por filas (transacciones contables)
        max_por_fila = np.max(matriz, axis=1)
        print(f"\nTransacciones contables con coincidencia > {self.umbral_coincidencia}: {np.sum(max_por_fila >= self.umbral_coincidencia)}")
        print(f"Score maximo promedio por transaccion contable: {np.mean(max_por_fila):.3f}")
        
        # Analisis por columnas (transacciones bancarias)
        max_por_columna = np.max(matriz, axis=0)
        print(f"Transacciones bancarias con coincidencia > {self.umbral_coincidencia}: {np.sum(max_por_columna >= self.umbral_coincidencia)}")
        print(f"Score maximo promedio por transaccion bancaria: {np.mean(max_por_columna):.3f}")
        
        if mostrar_graficos:
            self.generar_graficos_matriz(df_cont, df_bco)
        
        return {
            'matriz': matriz,
            'stats': {
                'max_global': np.max(matriz),
                'promedio': np.mean(matriz),
                'mediana': np.median(matriz),
                'coincidencias_potenciales': np.sum(matriz >= self.umbral_coincidencia),
                'cont_con_coincidencia': np.sum(max_por_fila >= self.umbral_coincidencia),
                'bco_con_coincidencia': np.sum(max_por_columna >= self.umbral_coincidencia)
            }
        }
    
    def generar_graficos_matriz(self, df_cont, df_bco):
        """Genera visualizaciones de la matriz de coincidencia"""
        
        if not PLOT_AVAILABLE:
            print("⚠️  matplotlib/seaborn no disponible. Saltando gráficos.")
            return
        
        try:
            fig, axes = plt.subplots(2, 3, figsize=(18, 12))
            fig.suptitle('Analisis de Matriz de Coincidencia', fontsize=16)
            
            # 1. Heatmap de matriz principal (muestra reducida si es muy grande)
            matriz_muestra = self.matriz_coincidencia
            if matriz_muestra.shape[0] > 50 or matriz_muestra.shape[1] > 50:
                # Tomar muestra para visualizacion
                filas = min(50, matriz_muestra.shape[0])
                cols = min(50, matriz_muestra.shape[1])
                matriz_muestra = matriz_muestra[:filas, :cols]
            
            sns.heatmap(matriz_muestra, ax=axes[0,0], cmap='viridis', cbar_kws={'label': 'Score'})
            axes[0,0].set_title('Matriz de Coincidencia (muestra)')
            axes[0,0].set_xlabel('Transacciones Bancarias')
            axes[0,0].set_ylabel('Transacciones Contables')
            
            # 2. Distribucion de scores
            scores_flat = self.matriz_coincidencia.flatten()
            axes[0,1].hist(scores_flat, bins=50, alpha=0.7, color='skyblue', edgecolor='black')
            axes[0,1].axvline(self.umbral_coincidencia, color='red', linestyle='--', 
                             label=f'Umbral ({self.umbral_coincidencia})')
            axes[0,1].set_title('Distribucion de Scores')
            axes[0,1].set_xlabel('Score de Coincidencia')
            axes[0,1].set_ylabel('Frecuencia')
            axes[0,1].legend()
            
            # 3. Scores maximos por transaccion contable
            max_por_fila = np.max(self.matriz_coincidencia, axis=1)
            axes[0,2].plot(max_por_fila, 'b-', alpha=0.7)
            axes[0,2].axhline(self.umbral_coincidencia, color='red', linestyle='--')
            axes[0,2].set_title('Score Maximo por Transaccion Contable')
            axes[0,2].set_xlabel('Indice Transaccion Contable')
            axes[0,2].set_ylabel('Score Maximo')
            
            # 4. Heatmap componente fecha
            if hasattr(self, 'matriz_fecha'):
                muestra_fecha = self.matriz_fecha
                if muestra_fecha.shape[0] > 30:
                    muestra_fecha = muestra_fecha[:30, :30]
                sns.heatmap(muestra_fecha, ax=axes[1,0], cmap='Reds', 
                           cbar_kws={'label': 'Similitud Fecha'})
                axes[1,0].set_title('Componente: Similitud de Fecha')
            
            # 5. Heatmap componente monto
            if hasattr(self, 'matriz_monto'):
                muestra_monto = self.matriz_monto
                if muestra_monto.shape[0] > 30:
                    muestra_monto = muestra_monto[:30, :30]
                sns.heatmap(muestra_monto, ax=axes[1,1], cmap='Blues',
                           cbar_kws={'label': 'Similitud Monto'})
                axes[1,1].set_title('Componente: Similitud de Monto')
            
            # 6. Heatmap componente concepto
            if hasattr(self, 'matriz_concepto'):
                muestra_concepto = self.matriz_concepto
                if muestra_concepto.shape[0] > 30:
                    muestra_concepto = muestra_concepto[:30, :30]
                sns.heatmap(muestra_concepto, ax=axes[1,2], cmap='Greens',
                           cbar_kws={'label': 'Similitud Concepto'})
                axes[1,2].set_title('Componente: Similitud de Concepto')
            
            plt.tight_layout()
            plt.savefig('matriz_coincidencia_analisis.png', dpi=300, bbox_inches='tight')
            print("\n📈 Graficos guardados en: matriz_coincidencia_analisis.png")
            plt.show()
            
        except ImportError:
            print("⚠️  matplotlib/seaborn no disponible. Instala con: pip install matplotlib seaborn")
        except Exception as e:
            print(f"⚠️  Error generando graficos: {e}")
    
    def exportar_matriz(self, nombre_archivo='matriz_coincidencia.xlsx'):
        """Exporta la matriz y componentes a Excel para analisis externo"""
        
        if not hasattr(self, 'matriz_coincidencia'):
            print("❌ No hay matriz construida para exportar")
            return
        
        with pd.ExcelWriter(nombre_archivo, engine='openpyxl') as writer:
            
            # Matriz principal
            df_matriz = pd.DataFrame(self.matriz_coincidencia)
            df_matriz.to_excel(writer, sheet_name='Matriz_Principal', index=True)
            
            # Componentes
            if hasattr(self, 'matriz_fecha'):
                pd.DataFrame(self.matriz_fecha).to_excel(writer, sheet_name='Componente_Fecha', index=True)
            
            if hasattr(self, 'matriz_monto'):
                pd.DataFrame(self.matriz_monto).to_excel(writer, sheet_name='Componente_Monto', index=True)
            
            if hasattr(self, 'matriz_concepto'):
                pd.DataFrame(self.matriz_concepto).to_excel(writer, sheet_name='Componente_Concepto', index=True)
            
            if hasattr(self, 'matriz_tipo'):
                pd.DataFrame(self.matriz_tipo).to_excel(writer, sheet_name='Componente_Tipo', index=True)
        
        print(f"📊 Matriz exportada a: {nombre_archivo}")

def conciliar_con_matriz(archivo_cont, archivo_bco, mostrar_graficos=True):
    """
    Función principal para conciliar archivos reales usando matriz de coincidencia
    """
    print("🔄 CONCILIADOR CON MATRIZ DE COINCIDENCIA")
    print("=" * 60)
    
    # Cargar archivos usando la misma lógica del conciliador original
    conciliador_matriz = ConciliadorMatriz()
    
    try:
        # Cargar archivo contable
        print(f"Cargando archivo contable: {archivo_cont}")
        
        # Intentar diferentes engines para archivos .xls
        try:
            df_cont_raw = pd.read_excel(archivo_cont, header=None, engine='xlrd')
            print("  ✅ Cargado con engine 'xlrd'")
        except Exception as e1:
            try:
                df_cont_raw = pd.read_excel(archivo_cont, header=None, engine='openpyxl')
                print("  ✅ Cargado con engine 'openpyxl'")
            except Exception as e2:
                print(f"  ❌ Error con xlrd: {e1}")
                print(f"  ❌ Error con openpyxl: {e2}")
                raise e1
        
        # Limpiar datos contables (misma lógica que antes)
        cont_data = []
        for i in range(2, len(df_cont_raw)):
            row = df_cont_raw.iloc[i]
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
                    'haber': haber,
                    'indice_original': i
                }
                cont_data.append(transaccion)
        
        df_cont = pd.DataFrame(cont_data)
        print(f"✅ Archivo contable cargado: {len(df_cont)} transacciones")
        
        # Cargar archivo bancario
        print(f"Cargando archivo bancario: {archivo_bco}")
        
        try:
            df_bco_raw = pd.read_excel(archivo_bco, header=None, engine='xlrd')
            print("  ✅ Cargado con engine 'xlrd'")
        except Exception as e1:
            try:
                df_bco_raw = pd.read_excel(archivo_bco, header=None, engine='openpyxl')
                print("  ✅ Cargado con engine 'openpyxl'")
            except Exception as e2:
                print(f"  ❌ Error con xlrd: {e1}")
                print(f"  ❌ Error con openpyxl: {e2}")
                raise e1
        
        # Limpiar datos bancarios
        bco_data = []
        for i in range(1, len(df_bco_raw)):
            row = df_bco_raw.iloc[i]
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
                    'credito': credito,
                    'indice_original': i
                }
                bco_data.append(transaccion)
        
        df_bco = pd.DataFrame(bco_data)
        print(f"✅ Archivo bancario cargado: {len(df_bco)} transacciones")
        
        # Análisis con matriz
        print(f"\n🧮 Construyendo matriz {len(df_cont)}x{len(df_bco)}...")
        analisis = conciliador_matriz.analizar_matriz(df_cont, df_bco, mostrar_graficos)
        
        # Encontrar coincidencias óptimas
        print("\n🎯 Buscando coincidencias óptimas...")
        coincidencias = conciliador_matriz.encontrar_coincidencias_optimas(df_cont, df_bco)
        
        # Estadísticas de resultados
        print(f"\n📊 RESULTADOS:")
        print(f"Total coincidencias encontradas: {len(coincidencias)}")
        print(f"Transacciones contables conciliadas: {len(coincidencias)}/{len(df_cont)} ({len(coincidencias)/len(df_cont)*100:.1f}%)")
        print(f"Transacciones bancarias conciliadas: {len(coincidencias)}/{len(df_bco)} ({len(coincidencias)/len(df_bco)*100:.1f}%)")
        
        # Distribución por nivel de confianza
        niveles = {}
        for coinc in coincidencias:
            nivel = coinc['nivel_confianza']
            niveles[nivel] = niveles.get(nivel, 0) + 1
        
        print(f"\n📈 DISTRIBUCIÓN POR CONFIANZA:")
        for nivel, cantidad in sorted(niveles.items()):
            print(f"  {nivel}: {cantidad} coincidencias")
        
        # Generar reporte detallado y archivo procesado
        archivo_procesado = generar_archivo_procesado(df_cont, df_bco, coincidencias, 
                                                     conciliador_matriz, archivo_cont, archivo_bco)
        
        print(f"\n🎉 ¡Conciliación completada con éxito!")
        print(f"📄 Archivo procesado: {archivo_procesado}")
        print(f"📊 Revisa los archivos generados para análisis detallado")
        
        return df_cont, df_bco, coincidencias, conciliador_matriz
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def generar_archivo_procesado(df_cont, df_bco, coincidencias, conciliador_matriz, 
                             archivo_cont, archivo_bco):
    """Genera el archivo procesado principal (.xlsx) con la conciliación"""
    
    # Extraer información del archivo para nombrar el procesado
    nombre_cont = os.path.basename(archivo_cont)
    # Convertir credi_cont_01_062025.xls -> credi_pro_01_062025.xlsx
    if '_cont_' in nombre_cont:
        nombre_procesado = nombre_cont.replace('_cont_', '_pro_').replace('.xls', '.xlsx')
    else:
        nombre_procesado = f"procesado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    archivo_salida = f"Procesado/{nombre_procesado}"
    
    # Crear directorio Procesado si no existe
    os.makedirs("Procesado", exist_ok=True)
    
    print(f"\n📝 Generando archivo procesado: {archivo_salida}")
    
    # Crear sets de índices conciliados
    indices_cont_conciliados = set([coinc['cont_index'] for coinc in coincidencias])
    indices_bco_conciliados = set([coinc['bco_index'] for coinc in coincidencias])
    
    with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
        
        # HOJA 1: Coincidencias Encontradas
        print("  📄 Generando hoja: Coincidencias")
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
                'Diferencia_Dias': abs((row_cont['fecha'] - row_bco['fecha']).days),
                'Score_Fecha': round(coinc['score_fecha'], 3),
                'Score_Monto': round(coinc['score_monto'], 3),
                'Score_Concepto': round(coinc['score_concepto'], 3)
            })
        
        df_coincidencias = pd.DataFrame(coincidencias_data)
        df_coincidencias = df_coincidencias.sort_values('Score_Total', ascending=False)
        df_coincidencias.to_excel(writer, sheet_name='Coincidencias', index=False)
        
        # HOJA 2: Transacciones Contables Sin Conciliar
        print("  📄 Generando hoja: Contables_Pendientes")
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
        
        # HOJA 3: Transacciones Bancarias Sin Conciliar
        print("  📄 Generando hoja: Bancarias_Pendientes")
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
        print("  📄 Generando hoja: Resumen_Ejecutivo")
        
        # Calcular estadísticas
        total_cont = len(df_cont)
        total_bco = len(df_bco)
        total_conciliado = len(coincidencias)
        cont_pendientes = len(cont_sin_conciliar)
        bco_pendientes = len(bco_sin_conciliar)
        
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
            ['Archivo Contable', os.path.basename(archivo_cont)],
            ['Archivo Bancario', os.path.basename(archivo_bco)],
            ['Fecha Procesamiento', datetime.now().strftime('%d/%m/%Y %H:%M:%S')],
            ['Método', 'Matriz de Coincidencia'],
            ['', ''],
            ['=== ESTADÍSTICAS DE TRANSACCIONES ===', ''],
            ['Total Transacciones Contables', total_cont],
            ['Total Transacciones Bancarias', total_bco],
            ['Total Conciliadas', total_conciliado],
            ['Contables Pendientes', cont_pendientes],
            ['Bancarias Pendientes', bco_pendientes],
            ['', ''],
            ['=== PORCENTAJES DE CONCILIACIÓN ===', ''],
            ['% Conciliación Contables', f"{(total_conciliado/total_cont)*100:.1f}%" if total_cont > 0 else "0%"],
            ['% Conciliación Bancarias', f"{(total_conciliado/total_bco)*100:.1f}%" if total_bco > 0 else "0%"],
            ['', ''],
            ['=== MONTOS ===', ''],
            ['Monto Conciliado (Contable)', f"${monto_conciliado_cont:,.2f}"],
            ['Monto Pendiente (Contable)', f"${monto_pendiente_cont:,.2f}"],
            ['Monto Pendiente (Bancario)', f"${monto_pendiente_bco:,.2f}"],
            ['', ''],
            ['=== DISTRIBUCIÓN POR CONFIANZA ===', ''],
        ]
        
        for nivel in ['MUY_ALTA', 'ALTA', 'MEDIA', 'BAJA']:
            if nivel in distribucion_confianza:
                resumen_data.append([f'Confianza {nivel}', distribucion_confianza[nivel]])
        
        resumen_data.extend([
            ['', ''],
            ['=== CONFIGURACIÓN MATRIZ ===', ''],
            ['Umbral Mínimo Score', conciliador_matriz.umbral_coincidencia],
            ['Peso Fecha', conciliador_matriz.pesos['fecha']],
            ['Peso Monto', conciliador_matriz.pesos['monto']],
            ['Peso Concepto', conciliador_matriz.pesos['concepto']],
            ['Peso Tipo', conciliador_matriz.pesos['tipo']],
        ])
        
        df_resumen = pd.DataFrame(resumen_data, columns=['Métrica', 'Valor'])
        df_resumen.to_excel(writer, sheet_name='Resumen_Ejecutivo', index=False)
        
        # HOJA 5: Análisis de Diferencias (solo las que tienen diferencias)
        print("  📄 Generando hoja: Analisis_Diferencias")
        diferencias_data = []
        
        for coinc in coincidencias:
            row_cont = df_cont.iloc[coinc['cont_index']]
            row_bco = df_bco.iloc[coinc['bco_index']]
            
            diff_monto = abs(row_cont['monto'] - row_bco['monto'])
            diff_fecha = abs((row_cont['fecha'] - row_bco['fecha']).days)
            
            if diff_monto > 0.01 or diff_fecha > 0:  # Solo si hay diferencias
                diferencias_data.append({
                    'Tipo_Diferencia': 'MONTO' if diff_monto > 0.01 else 'FECHA',
                    'Score_Total': round(coinc['score_total'], 3),
                    'Fecha_Cont': row_cont['fecha'].strftime('%d/%m/%Y'),
                    'Fecha_Bco': row_bco['fecha'].strftime('%d/%m/%Y'),
                    'Concepto_Cont': row_cont['concepto'][:50],
                    'Concepto_Bco': row_bco['concepto'][:50],
                    'Monto_Cont': row_cont['monto'],
                    'Monto_Bco': row_bco['monto'],
                    'Diferencia_Monto': diff_monto,
                    'Diferencia_Dias': diff_fecha,
                    'Accion_Sugerida': 'REVISAR_MANUAL' if coinc['nivel_confianza'] in ['BAJA', 'MUY_BAJA'] else 'VERIFICAR'
                })
        
        if diferencias_data:
            df_diferencias = pd.DataFrame(diferencias_data)
            df_diferencias = df_diferencias.sort_values('Diferencia_Monto', ascending=False)
            df_diferencias.to_excel(writer, sheet_name='Analisis_Diferencias', index=False)
    
    print(f"  ✅ Archivo procesado generado: {archivo_salida}")
    
    # También generar el reporte técnico de matriz (opcional)
    generar_reporte_tecnico_matriz(df_cont, df_bco, coincidencias, conciliador_matriz)
    
    return archivo_salida

def generar_reporte_tecnico_matriz(df_cont, df_bco, coincidencias, conciliador_matriz):
    """Genera reporte técnico adicional con detalles de la matriz (opcional)"""
    
    nombre_tecnico = f"reporte_tecnico_matriz_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    print(f"  📊 Generando reporte técnico: {nombre_tecnico}")
    
    with pd.ExcelWriter(nombre_tecnico, engine='openpyxl') as writer:
        
        # Hoja 1: Scores detallados por componente
        scores_detallados = []
        for coinc in coincidencias:
            row_cont = df_cont.iloc[coinc['cont_index']]
            row_bco = df_bco.iloc[coinc['bco_index']]
            
            scores_detallados.append({
                'Cont_Index': coinc['cont_index'],
                'Bco_Index': coinc['bco_index'],
                'Score_Total': round(coinc['score_total'], 4),
                'Score_Fecha': round(coinc['score_fecha'], 4),
                'Score_Monto': round(coinc['score_monto'], 4),
                'Score_Concepto': round(coinc['score_concepto'], 4),
                'Score_Tipo': round(coinc['score_tipo'], 4),
                'Nivel_Confianza': coinc['nivel_confianza'],
                'Fecha_Cont': row_cont['fecha'].strftime('%d/%m/%Y'),
                'Monto_Cont': row_cont['monto'],
                'Concepto_Cont_Truncado': row_cont['concepto'][:100],
                'Fecha_Bco': row_bco['fecha'].strftime('%d/%m/%Y'),
                'Monto_Bco': row_bco['monto'],
                'Concepto_Bco_Truncado': row_bco['concepto'][:100]
            })
        
        df_scores = pd.DataFrame(scores_detallados)
        df_scores.to_excel(writer, sheet_name='Scores_Detallados', index=False)
        
        # Hoja 2: Configuración y parámetros
        config_data = [
            ['=== CONFIGURACIÓN MATRIZ ===', ''],
            ['Umbral Coincidencia', conciliador_matriz.umbral_coincidencia],
            ['Tolerancia Monto (%)', conciliador_matriz.tolerancia_monto_pct * 100],
            ['Tolerancia Fecha (días)', conciliador_matriz.tolerancia_fecha_dias],
            ['', ''],
            ['=== PESOS DE COMPONENTES ===', ''],
            ['Peso Fecha', conciliador_matriz.pesos['fecha']],
            ['Peso Monto', conciliador_matriz.pesos['monto']],
            ['Peso Concepto', conciliador_matriz.pesos['concepto']],
            ['Peso Tipo', conciliador_matriz.pesos['tipo']],
            ['', ''],
            ['=== ALGORITMO UTILIZADO ===', ''],
            ['Optimización', 'Hungarian Algorithm' if SCIPY_AVAILABLE else 'Greedy Algorithm'],
            ['Scipy Disponible', 'Sí' if SCIPY_AVAILABLE else 'No'],
            ['Gráficos Disponibles', 'Sí' if PLOT_AVAILABLE else 'No'],
        ]
        
        df_config = pd.DataFrame(config_data, columns=['Parámetro', 'Valor'])
        df_config.to_excel(writer, sheet_name='Configuracion', index=False)
    
    # Exportar matriz completa para análisis avanzado
    conciliador_matriz.exportar_matriz(f"matriz_completa_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")

# Ejemplo con datos sintéticos
def ejemplo_matriz():
    """Ejemplo de como usar el conciliador con matriz usando datos sintéticos"""
    
    print("🧪 EJEMPLO CON DATOS SINTÉTICOS")
    print("=" * 50)
    
    # Crear datos de ejemplo
    df_cont = pd.DataFrame({
        'fecha': [
            pd.Timestamp('2025-06-01'), 
            pd.Timestamp('2025-06-02'),
            pd.Timestamp('2025-06-03'),
            pd.Timestamp('2025-06-04')
        ],
        'concepto': [
            'Transferencia Banco X', 
            'Pago Proveedor Y',
            'Cobro Cliente Z',
            'Retención AFIP'
        ],
        'monto': [15000.0, 8500.0, 12000.0, 2500.0],
        'tipo': ['DEBE', 'HABER', 'DEBE', 'HABER'],
        'debe': [15000.0, 0, 12000.0, 0],
        'haber': [0, 8500.0, 0, 2500.0]
    })
    
    df_bco = pd.DataFrame({
        'fecha': [
            pd.Timestamp('2025-06-01'), 
            pd.Timestamp('2025-06-02'),
            pd.Timestamp('2025-06-03'),
            pd.Timestamp('2025-06-05')  # Fecha ligeramente diferente
        ],
        'concepto': [
            'Credito Inmediato - Cliente ABC', 
            'Debito Automatico - Proveedor Y',
            'Acreditacion - Cliente Z',
            'Retencion Impuestos'
        ],
        'monto': [15000.0, 8500.0, 12000.0, 2500.0],
        'tipo': ['CREDITO', 'DEBITO', 'CREDITO', 'DEBITO'],
        'debito': [0, 8500.0, 0, 2500.0],
        'credito': [15000.0, 0, 12000.0, 0]
    })
    
    print(f"📊 Datos contables: {len(df_cont)} transacciones")
    print(f"📊 Datos bancarios: {len(df_bco)} transacciones")
    
    # Crear conciliador
    conciliador = ConciliadorMatriz()
    
    # Analizar matriz
    print("\n🧮 Analizando matriz de coincidencia...")
    analisis = conciliador.analizar_matriz(df_cont, df_bco, mostrar_graficos=False)
    
    # Encontrar coincidencias optimas
    print("\n🎯 Buscando coincidencias óptimas...")
    coincidencias = conciliador.encontrar_coincidencias_optimas(df_cont, df_bco)
    
    print(f"\n📋 RESULTADOS DEL EJEMPLO:")
    print(f"Coincidencias encontradas: {len(coincidencias)}")
    
    for i, coinc in enumerate(coincidencias, 1):
        row_cont = df_cont.iloc[coinc['cont_index']]
        row_bco = df_bco.iloc[coinc['bco_index']]
        
        print(f"\n{i}. Score: {coinc['score_total']:.3f} - Confianza: {coinc['nivel_confianza']}")
        print(f"   Contable: {row_cont['concepto'][:40]} | ${row_cont['monto']:,.0f}")
        print(f"   Bancario: {row_bco['concepto'][:40]} | ${row_bco['monto']:,.0f}")
        print(f"   Componentes: Fecha={coinc['score_fecha']:.2f}, Monto={coinc['score_monto']:.2f}, Concepto={coinc['score_concepto']:.2f}")
    
    print(f"\n✅ Ejemplo completado. Matriz de {len(df_cont)}x{len(df_bco)} analizada.")
    
    return conciliador, coincidencias, df_cont, df_bco

def ejemplo_con_archivos_reales():
    """Ejemplo para ejecutar con tus archivos reales"""
    
    # Usar tus archivos reales
    archivo_cont = "Contable/credi_cont_01_062025.xls"  # Ajusta la ruta
    archivo_bco = "Bancos/credi_bco_01_062025.xls"     # Ajusta la ruta
    
    # Verificar que los archivos existen
    if not os.path.exists(archivo_cont):
        print(f"❌ No se encuentra: {archivo_cont}")
        print("💡 Crea la estructura de directorios o ajusta las rutas")
        return
    
    if not os.path.exists(archivo_bco):
        print(f"❌ No se encuentra: {archivo_bco}")
        print("💡 Crea la estructura de directorios o ajusta las rutas")
        return
    
    # Ejecutar conciliación con matriz
    resultado = conciliar_con_matriz(archivo_cont, archivo_bco, mostrar_graficos=True)
    
    if resultado:
        df_cont, df_bco, coincidencias, conciliador = resultado
        print(f"\n🎉 ¡Conciliación completada con éxito!")
        print(f"📊 Revisa los archivos generados para análisis detallado")
    
    return resultado

if __name__ == "__main__":
    # Verificar dependencias primero
    if not verificar_dependencias():
        print("\n💡 Instalando dependencias faltantes...")
        print("Puedes continuar, pero algunas funciones pueden no estar disponibles.")
        input("Presiona Enter para continuar o Ctrl+C para salir...")
    
    print("🔧 CONCILIADOR CON MATRIZ DE COINCIDENCIA")
    print("=" * 60)
    print("1. Ejemplo con datos sintéticos")
    print("2. Conciliación con archivos reales")
    print("3. Solo análisis de ejemplo (sin input)")
    
    try:
        opcion = input("\nSelecciona (1/2/3) o Enter para ejemplo rápido: ").strip()
        
        if opcion == "1" or opcion == "":
            print("\n🧪 Ejecutando ejemplo con datos sintéticos...")
            ejemplo_matriz()
        elif opcion == "2":
            print("\n📁 Ejecutando con archivos reales...")
            ejemplo_con_archivos_reales()
        elif opcion == "3":
            print("\n⚡ Ejecutando análisis rápido...")
            ejemplo_matriz()
        else:
            print("❌ Opción inválida, ejecutando ejemplo por defecto...")
            ejemplo_matriz()
            
    except KeyboardInterrupt:
        print("\n👋 Operación cancelada por el usuario")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()