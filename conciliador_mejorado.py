import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import os
import glob

# Librerias optimizadas para similitud de texto
try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
    print("✅ RapidFuzz disponible - Algoritmo optimizado")
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    print("⚠️  RapidFuzz no disponible. Instala con: pip install rapidfuzz")
    from difflib import SequenceMatcher

# Librerias para analisis vectorial
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_AVAILABLE = True
    print("✅ Scikit-learn disponible - Analisis vectorial activo")
except ImportError:
    SKLEARN_AVAILABLE = False
    print("⚠️  Scikit-learn no disponible. Instala con: pip install scikit-learn")

# Scipy para algoritmo Hungarian
try:
    from scipy.optimize import linear_sum_assignment
    SCIPY_AVAILABLE = True
    print("✅ Scipy disponible - Algoritmo Hungarian activo")
except ImportError:
    SCIPY_AVAILABLE = False
    print("⚠️  Scipy no disponible. Usando algoritmo greedy")

class ConciliadorAutomaticoMejorado:
    def __init__(self):
        # Pesos optimizados basados en analisis
        self.pesos = {
            'fecha': 0.35,     # 35% peso a la fecha
            'monto': 0.45,     # 45% peso al monto (prioridad maxima)
            'concepto': 0.15,  # 15% peso al concepto (optimizado con rapidfuzz)
            'tipo': 0.05       # 5% peso a compatibilidad de tipo
        }
        
        # Parametros de tolerancia optimizados
        self.tolerancia_monto_pct = 0.02  # 2% tolerancia para montos
        self.tolerancia_fecha_dias = 2    # 2 dias tolerancia maxima
        
        # Umbral ajustado para rapidfuzz
        self.umbral_coincidencia = 0.50  # Mas permisivo con mejor algoritmo
        
        # Umbrales especificos para rapidfuzz
        self.umbral_similitud_alta = 0.85      # 85% para alta similitud
        self.umbral_similitud_media = 0.65     # 65% para media similitud
        self.umbral_similitud_parcial = 0.75   # 75% para similitud parcial
        
        # Mapeo de conceptos mejorado para rapidfuzz
        self.mapeo_conceptos = {
            'transferencia': [
                'transferencia', 'transf', 'credito inmediato', 'debin', 
                'acreditacion', 'debito inmediato', 'transfer', 'credito por transferencia',
                'transferencia entre cuentas', 'transf inmediata'
            ],
            'retencion': [
                'retencion', 'ret', 'retencion impositiva', 'retencion ganancias',
                'retencion iva', 'retencion iibb', 'withholding', 'ret gcias'
            ],
            'debito_automatico': [
                'debito automatico', 'deb aut', 'debito aut', 'pago automatico',
                'debito por servicio', 'cobro automatico', 'deb automatico'
            ],
            'tarjeta': [
                'tarjeta', 'tc', 'td', 'visa', 'mastercard', 'american express',
                'compra con tarjeta', 'pago tarjeta', 'tarj credito', 'tarj debito'
            ],
            'cheque': [
                'cheque', 'ch', 'cheque propio', 'cheque tercero', 'deposito cheque',
                'cobro cheque', 'rechazo cheque', 'chq'
            ],
            'efectivo': [
                'efectivo', 'extraccion', 'deposito efectivo', 'cajero automatico',
                'atm', 'retiro efectivo', 'dep efectivo'
            ],
            'servicios': [
                'servicio', 'luz', 'gas', 'agua', 'telefono', 'internet',
                'cable', 'expensas', 'alquiler', 'edenor', 'edesur', 'metrogas'
            ],
            'comisiones': [
                'comision', 'com', 'mantenimiento', 'gastos', 'cargo',
                'fee', 'tarifa', 'mant cuenta'
            ],
            'impuestos': [
                'impuesto', 'iva', 'ganancias', 'bienes personales', 'iibb',
                'monotributo', 'ingresos brutos', 'imp gcias', 'imp iva'
            ],
            'nomina': [
                'sueldo', 'salario', 'haberes', 'aguinaldo', 'liquidacion',
                'nomina', 'pago empleados', 'liq sueldo'
            ]
        }
        
        # Entidades especificas argentinas para mejor deteccion
        self.entidades_argentinas = [
            'edenor', 'edesur', 'metrogas', 'aba', 'aysa', 'telecom', 'fibertel',
            'claro', 'movistar', 'personal', 'directv', 'flow', 'cablevision',
            'mercadopago', 'mercado pago', 'rapipago', 'pagofacil', 'link',
            'banelco', 'red link', 'prisma', 'first data', 'lapos', 'naranja',
            'visa', 'mastercard', 'american express', 'cabal', 'tarjeta shopping',
            'afip', 'arba', 'agip', 'rentas', 'anses', 'pami'
        ]
        
        # Configuracion de TF-IDF si sklearn esta disponible
        if SKLEARN_AVAILABLE:
            self.vectorizer = TfidfVectorizer(
                max_features=1000,
                stop_words=None,  # Manejaremos stop words manualmente
                ngram_range=(1, 2),  # Unigramas y bigramas
                lowercase=True,
                token_pattern=r'\b\w{2,}\b'  # Palabras de al menos 2 caracteres
            )
            self.conceptos_vectorizados = None
    
    def verificar_dependencias(self):
        """Verifica y reporta el estado de las dependencias"""
        print("\n🔧 VERIFICACION DE DEPENDENCIAS:")
        print("-" * 40)
        print(f"RapidFuzz: {'✅ Disponible' if RAPIDFUZZ_AVAILABLE else '❌ No disponible'}")
        print(f"Scikit-learn: {'✅ Disponible' if SKLEARN_AVAILABLE else '❌ No disponible'}")
        print(f"Scipy: {'✅ Disponible' if SCIPY_AVAILABLE else '❌ No disponible'}")
        
        if not RAPIDFUZZ_AVAILABLE:
            print("\n💡 Para mejor rendimiento instala: pip install rapidfuzz")
        if not SKLEARN_AVAILABLE:
            print("💡 Para analisis vectorial instala: pip install scikit-learn")
        if not SCIPY_AVAILABLE:
            print("💡 Para algoritmo optimo instala: pip install scipy")
    
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
        """Encuentra automaticamente todos los pares de archivos para conciliar"""
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
                
                concepto_original = str(row.iloc[5]) if pd.notna(row.iloc[5]) else ''
                
                transaccion = {
                    'fecha': fecha,
                    'concepto': concepto_original,
                    'concepto_normalizado': self.normalizar_concepto_rapidfuzz(concepto_original),
                    'monto': debe if debe > 0 else haber,
                    'tipo': 'DEBE' if debe > 0 else 'HABER',
                    'debe': debe,
                    'haber': haber,
                    'entidades_detectadas': self.extraer_entidades_rapidfuzz(concepto_original),
                    'palabras_clave': self.extraer_palabras_clave(concepto_original)
                }
                cont_data.append(transaccion)
        
        return pd.DataFrame(cont_data)
    
    def cargar_archivo_bancario(self, archivo_bco):
        """Carga y limpia archivo bancario"""
        print(f"🏦 Cargando: {os.path.basename(archivo_bco)}")
        
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
                
                concepto_original = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ''
                
                transaccion = {
                    'fecha': fecha,
                    'concepto': concepto_original,
                    'concepto_normalizado': self.normalizar_concepto_rapidfuzz(concepto_original),
                    'monto': debito if debito > 0 else credito,
                    'tipo': 'DEBITO' if debito > 0 else 'CREDITO',
                    'debito': debito,
                    'credito': credito,
                    'entidades_detectadas': self.extraer_entidades_rapidfuzz(concepto_original),
                    'palabras_clave': self.extraer_palabras_clave(concepto_original)
                }
                bco_data.append(transaccion)
        
        return pd.DataFrame(bco_data)
    
    def normalizar_concepto_rapidfuzz(self, concepto):
        """Normalizacion optimizada para rapidfuzz"""
        if pd.isna(concepto):
            return ""
        
        concepto = str(concepto).lower().strip()
        
        # Remover caracteres especiales pero mantener informacion util
        concepto = re.sub(r'[^\w\s-]', ' ', concepto)
        concepto = re.sub(r'\b\d{6,}\b', '', concepto)  # Remover numeros largos (IDs)
        concepto = re.sub(r'\s+', ' ', concepto)
        
        # Remover palabras de ruido menos agresivamente para rapidfuzz
        palabras_ruido = ['el', 'la', 'de', 'del', 'y', 'en', 'por', 'con', 'para', 'al']
        palabras = concepto.split()
        palabras_filtradas = [p for p in palabras if p not in palabras_ruido and len(p) > 1]
        
        return ' '.join(palabras_filtradas)
    
    def extraer_entidades_rapidfuzz(self, concepto):
        """Extrae entidades usando rapidfuzz para mejor matching"""
        if pd.isna(concepto):
            return []
        
        concepto_lower = str(concepto).lower()
        entidades_encontradas = []
        
        if RAPIDFUZZ_AVAILABLE:
            # Usar rapidfuzz para matching mas flexible
            for entidad in self.entidades_argentinas:
                # Usar fuzz.partial_ratio para matching parcial
                ratio = fuzz.partial_ratio(entidad, concepto_lower)
                if ratio >= 80:  # 80% de similitud parcial
                    entidades_encontradas.append(entidad)
        else:
            # Fallback al metodo original
            for entidad in self.entidades_argentinas:
                if entidad in concepto_lower:
                    entidades_encontradas.append(entidad)
        
        return entidades_encontradas
    
    def extraer_palabras_clave(self, concepto):
        """Extrae palabras clave importantes del concepto"""
        if pd.isna(concepto):
            return []
        
        concepto_norm = self.normalizar_concepto_rapidfuzz(concepto)
        palabras = concepto_norm.split()
        
        # Filtrar palabras clave importantes (mas de 3 caracteres)
        palabras_clave = [p for p in palabras if len(p) > 3]
        
        return palabras_clave[:5]  # Maximo 5 palabras clave
    
    def calcular_similitud_fecha(self, fecha1, fecha2):
        """Calcula similitud entre fechas optimizada"""
        if fecha1 is None or fecha2 is None:
            return 0.0
        
        diferencia_dias = abs((fecha1 - fecha2).days)
        
        if diferencia_dias == 0:
            return 1.0
        elif diferencia_dias == 1:
            return 0.9
        elif diferencia_dias == 2:
            return 0.8
        elif diferencia_dias <= 5:
            return 0.5
        elif diferencia_dias <= 7:
            return 0.3
        else:
            return 0.0
    
    def calcular_similitud_monto(self, monto1, monto2):
        """Calcula similitud entre montos con alta precision"""
        if monto1 == 0 or monto2 == 0:
            return 1.0 if monto1 == monto2 else 0.0
        
        diferencia_pct = abs(monto1 - monto2) / max(monto1, monto2)
        
        if diferencia_pct == 0:
            return 1.0
        elif diferencia_pct <= 0.001:  # 0.1%
            return 0.99
        elif diferencia_pct <= 0.005:  # 0.5%
            return 0.97
        elif diferencia_pct <= 0.01:   # 1%
            return 0.95
        elif diferencia_pct <= 0.02:   # 2%
            return 0.9
        elif diferencia_pct <= 0.05:   # 5%
            return 0.7
        elif diferencia_pct <= 0.1:    # 10%
            return 0.4
        else:
            return 0.0
    
    def calcular_similitud_concepto_rapidfuzz(self, concepto1, concepto2, entidades1, entidades2, palabras1, palabras2):
        """Calculo de similitud usando RapidFuzz y metodos avanzados"""
        
        if not concepto1 or not concepto2:
            return 0.0
        
        c1_norm = self.normalizar_concepto_rapidfuzz(concepto1)
        c2_norm = self.normalizar_concepto_rapidfuzz(concepto2)
        
        if not c1_norm or not c2_norm:
            return 0.0
        
        similitudes = []
        
        if RAPIDFUZZ_AVAILABLE:
            # 1. Similitud basica con rapidfuzz
            similitud_basica = fuzz.ratio(c1_norm, c2_norm) / 100.0
            similitudes.append(('basica', similitud_basica))
            
            # 2. Similitud parcial (para conceptos que contienen el otro)
            similitud_parcial = fuzz.partial_ratio(c1_norm, c2_norm) / 100.0
            similitudes.append(('parcial', similitud_parcial))
            
            # 3. Similitud de tokens ordenados (ignora orden)
            similitud_token_sort = fuzz.token_sort_ratio(c1_norm, c2_norm) / 100.0
            similitudes.append(('token_sort', similitud_token_sort))
            
            # 4. Similitud de tokens set (ignora duplicados y orden)
            similitud_token_set = fuzz.token_set_ratio(c1_norm, c2_norm) / 100.0
            similitudes.append(('token_set', similitud_token_set))
            
        else:
            # Fallback con SequenceMatcher
            from difflib import SequenceMatcher
            similitud_basica = SequenceMatcher(None, c1_norm, c2_norm).ratio()
            similitudes.append(('basica', similitud_basica))
        
        # 5. Similitud por entidades comunes
        if entidades1 and entidades2:
            entidades_comunes = set(entidades1).intersection(set(entidades2))
            if entidades_comunes:
                similitud_entidades = 0.95  # Alta similitud si comparten entidades
                similitudes.append(('entidades', similitud_entidades))
        
        # 6. Similitud por palabras clave
        if palabras1 and palabras2:
            palabras_comunes = set(palabras1).intersection(set(palabras2))
            if palabras_comunes:
                similitud_palabras = len(palabras_comunes) / max(len(palabras1), len(palabras2))
                similitudes.append(('palabras', similitud_palabras))
        
        # 7. Similitud por mapeo de categorias
        similitud_mapeo = self.calcular_similitud_mapeo(c1_norm, c2_norm)
        if similitud_mapeo > 0:
            similitudes.append(('mapeo', similitud_mapeo))
        
        # 8. Similitud vectorial con TF-IDF (si sklearn disponible)
        if SKLEARN_AVAILABLE:
            similitud_vectorial = self.calcular_similitud_vectorial(c1_norm, c2_norm)
            if similitud_vectorial > 0:
                similitudes.append(('vectorial', similitud_vectorial))
        
        # Tomar el maximo de todas las similitudes calculadas
        if similitudes:
            max_similitud = max([sim[1] for sim in similitudes])
            return max_similitud
        
        return 0.0
    
    def calcular_similitud_mapeo(self, c1_norm, c2_norm):
        """Calcula similitud por mapeo de categorias"""
        similitud_mapeo = 0.0
        
        for categoria, keywords in self.mapeo_conceptos.items():
            c1_match = any(kw in c1_norm for kw in keywords)
            c2_match = any(kw in c2_norm for kw in keywords)
            if c1_match and c2_match:
                similitud_mapeo = max(similitud_mapeo, 0.8)
        
        return similitud_mapeo
    
    def calcular_similitud_vectorial(self, c1_norm, c2_norm):
        """Calcula similitud usando vectorizacion TF-IDF"""
        try:
            # Crear corpus temporal
            corpus = [c1_norm, c2_norm]
            
            # Vectorizar
            tfidf_matrix = self.vectorizer.fit_transform(corpus)
            
            # Calcular similitud coseno
            similitud = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            
            return similitud
            
        except Exception:
            return 0.0
    
    def calcular_compatibilidad_tipo(self, tipo_cont, tipo_bco):
        """Verifica compatibilidad de tipos contable vs bancario"""
        compatible = (
            (tipo_cont == 'DEBE' and tipo_bco == 'CREDITO') or
            (tipo_cont == 'HABER' and tipo_bco == 'DEBITO')
        )
        return 1.0 if compatible else 0.0
    
    def construir_matriz_coincidencia_optimizada(self, df_cont, df_bco):
        """Construye matriz de coincidencia usando algoritmos optimizados"""
        n_cont = len(df_cont)
        n_bco = len(df_bco)
        
        print(f"🧮 Construyendo matriz optimizada {n_cont}x{n_bco}...")
        if RAPIDFUZZ_AVAILABLE:
            print("   🚀 Usando RapidFuzz para analisis de conceptos")
        if SKLEARN_AVAILABLE:
            print("   🧠 Usando analisis vectorial TF-IDF")
        
        # Inicializar matrices
        matriz = np.zeros((n_cont, n_bco))
        self.matriz_fecha = np.zeros((n_cont, n_bco))
        self.matriz_monto = np.zeros((n_cont, n_bco))
        self.matriz_concepto = np.zeros((n_cont, n_bco))
        self.matriz_tipo = np.zeros((n_cont, n_bco))
        
        # Progreso para matrices grandes
        progreso_cada = max(1, n_cont // 10)
        
        # Calcular scores para cada par
        for i, row_cont in df_cont.iterrows():
            if i % progreso_cada == 0:
                porcentaje = (i / n_cont) * 100
                print(f"   📊 Progreso: {porcentaje:.0f}% ({i+1}/{n_cont})")
            
            for j, row_bco in df_bco.iterrows():
                
                # Calcular similitudes individuales
                sim_fecha = self.calcular_similitud_fecha(row_cont['fecha'], row_bco['fecha'])
                sim_monto = self.calcular_similitud_monto(row_cont['monto'], row_bco['monto'])
                sim_concepto = self.calcular_similitud_concepto_rapidfuzz(
                    row_cont['concepto'], row_bco['concepto'],
                    row_cont['entidades_detectadas'], row_bco['entidades_detectadas'],
                    row_cont['palabras_clave'], row_bco['palabras_clave']
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
        print(f"✅ Matriz optimizada construida. Score maximo: {np.max(matriz):.3f}")
        return matriz
    
    def encontrar_coincidencias_optimas(self, df_cont, df_bco):
        """Encuentra coincidencias usando algoritmo optimizado"""
        
        # Construir matriz de coincidencia optimizada
        matriz = self.construir_matriz_coincidencia_optimizada(df_cont, df_bco)
        
        if SCIPY_AVAILABLE:
            print("🎯 Aplicando algoritmo Hungarian (optimo)...")
            matriz_costo = 1 - matriz
            indices_cont, indices_bco = linear_sum_assignment(matriz_costo)
        else:
            print("🎯 Aplicando algoritmo greedy...")
            indices_cont, indices_bco = self._asignacion_greedy(matriz)
        
        # Filtrar coincidencias por umbral
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
                    'nivel_confianza': self.clasificar_nivel_confianza(score),
                    'metodo': 'RAPIDFUZZ' if RAPIDFUZZ_AVAILABLE else 'STANDARD'
                })
        
        return coincidencias
    
    def _asignacion_greedy(self, matriz):
        """Algoritmo greedy optimizado"""
        used_cont = set()
        used_bco = set()
        indices_cont = []
        indices_bco = []
        
        # Crear lista de coordenadas ordenadas por score
        coords = []
        for i in range(matriz.shape[0]):
            for j in range(matriz.shape[1]):
                if matriz[i, j] > self.umbral_coincidencia:
                    coords.append((i, j, matriz[i, j]))
        
        # Ordenar por score descendente
        coords.sort(key=lambda x: x[2], reverse=True)
        
        # Asignar greedily
        for i, j, score in coords:
            if i not in used_cont and j not in used_bco:
                indices_cont.append(i)
                indices_bco.append(j)
                used_cont.add(i)
                used_bco.add(j)
        
        return np.array(indices_cont), np.array(indices_bco)
    
    def clasificar_nivel_confianza(self, score):
        """Clasifica nivel de confianza con umbrales optimizados"""
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
    
    def buscar_candidatos_cercanos(self, row, df_target, indices_usados, max_candidatos=3):
        """Busca candidatos cercanos para transacciones no conciliadas"""
        candidatos = []
        
        # Filtrar transacciones no usadas
        df_disponibles = df_target[~df_target.index.isin(indices_usados)]
        
        if len(df_disponibles) == 0:
            return "Sin candidatos disponibles"
        
        # Buscar por monto similar primero
        tolerance_monto = 0.05  # 5% tolerancia
        for idx, target_row in df_disponibles.iterrows():
            
            # Verificar compatibilidad de monto
            if target_row['monto'] > 0 and row['monto'] > 0:
                diff_monto = abs(target_row['monto'] - row['monto']) / max(target_row['monto'], row['monto'])
                if diff_monto <= tolerance_monto:
                    
                    # Calcular similitud de concepto
                    if RAPIDFUZZ_AVAILABLE:
                        similitud = fuzz.token_sort_ratio(
                            row['concepto_normalizado'], 
                            target_row['concepto_normalizado']
                        )
                    else:
                        from difflib import SequenceMatcher
                        similitud = SequenceMatcher(None, 
                            row['concepto_normalizado'], 
                            target_row['concepto_normalizado']
                        ).ratio() * 100
                    
                    # Diferencia de dias
                    diff_dias = abs((row['fecha'] - target_row['fecha']).days)
                    
                    candidatos.append({
                        'monto': target_row['monto'],
                        'fecha': target_row['fecha'].strftime('%d/%m/%Y'),
                        'concepto': str(target_row['concepto'])[:50],
                        'similitud': similitud,
                        'diff_dias': diff_dias,
                        'score': similitud - diff_dias  # Score simple
                    })
        
        # Ordenar por score y tomar los mejores
        candidatos.sort(key=lambda x: x['score'], reverse=True)
        candidatos = candidatos[:max_candidatos]
        
        if candidatos:
            resultado = []
            for i, cand in enumerate(candidatos, 1):
                resultado.append(f"{i}. ${cand['monto']:,.0f} - {cand['fecha']} - {cand['concepto']} (Sim: {cand['similitud']:.0f}%)")
            return " | ".join(resultado)
        
        return "Sin candidatos con monto similar"
    
    def analizar_causa_no_conciliacion(self, row):
        """Analisis avanzado de causas de no conciliacion"""
        causas = []
        
        monto = row['monto']
        concepto = str(row['concepto']).lower()
        entidades = row.get('entidades_detectadas', [])
        palabras = row.get('palabras_clave', [])
        
        # Analisis por monto
        if monto < 50:
            causas.append("Monto muy pequeño")
        elif monto > 500000:
            causas.append("Monto alto - revisar")
        
        # Analisis por entidades
        if not entidades and not palabras:
            causas.append("Concepto generico sin entidades")
        elif 'comision' in concepto or 'cargo' in concepto:
            causas.append("Posible comision bancaria")
        elif 'reversa' in concepto or 'anulacion' in concepto:
            causas.append("Reversion o anulacion")
        
        # Analisis temporal
        if row['fecha'].weekday() in [5, 6]:
            causas.append("Transaccion fin de semana")
        
        # Analisis de normalizacion
        concepto_norm = row.get('concepto_normalizado', '')
        if len(concepto_norm.split()) < 2:
            causas.append("Concepto muy corto")
        
        return '; '.join(causas) if causas else 'Requiere analisis manual'
    
    def generar_archivo_procesado_mejorado(self, df_cont, df_bco, coincidencias, info_archivo):
        """Genera archivo procesado con informacion de optimizacion"""
        
        archivo_salida = info_archivo['archivo_salida']
        os.makedirs("Procesado", exist_ok=True)
        
        print(f"📝 Generando archivo mejorado: {os.path.basename(archivo_salida)}")
        
        # Crear sets de indices conciliados
        indices_cont_conciliados = set([coinc['cont_index'] for coinc in coincidencias])
        indices_bco_conciliados = set([coinc['bco_index'] for coinc in coincidencias])
        
        with pd.ExcelWriter(archivo_salida, engine='openpyxl', options={'remove_timezone': True}) as writer:
            
            # HOJA 1: Coincidencias Optimizadas
            if coincidencias:
                coincidencias_data = []
                for coinc in coincidencias:
                    row_cont = df_cont.iloc[coinc['cont_index']]
                    row_bco = df_bco.iloc[coinc['bco_index']]
                    
                    coincidencias_data.append({
                        'Estado': 'CONCILIADO',
                        'Metodo': coinc.get('metodo', 'STANDARD'),
                        'Nivel_Confianza': coinc['nivel_confianza'],
                        'Score_Total': round(float(coinc['score_total']), 4),
                        'Score_Fecha': round(float(coinc['score_fecha']), 4),
                        'Score_Monto': round(float(coinc['score_monto']), 4),
                        'Score_Concepto': round(float(coinc['score_concepto']), 4),
                        'Score_Tipo': round(float(coinc['score_tipo']), 4),
                        'Fecha_Cont': row_cont['fecha'].strftime('%d/%m/%Y'),
                        'Concepto_Cont': str(row_cont['concepto'])[:180],
                        'Concepto_Cont_Norm': str(row_cont['concepto_normalizado'])[:100],
                        'Monto_Cont': float(row_cont['monto']),
                        'Tipo_Cont': str(row_cont['tipo']),
                        'Entidades_Cont': ', '.join(row_cont['entidades_detectadas']) if row_cont['entidades_detectadas'] else '',
                        'Palabras_Clave_Cont': ', '.join(row_cont['palabras_clave']) if row_cont['palabras_clave'] else '',
                        'Fecha_Bco': row_bco['fecha'].strftime('%d/%m/%Y'),
                        'Concepto_Bco': str(row_bco['concepto'])[:180],
                        'Concepto_Bco_Norm': str(row_bco['concepto_normalizado'])[:100],
                        'Monto_Bco': float(row_bco['monto']),
                        'Tipo_Bco': str(row_bco['tipo']),
                        'Entidades_Bco': ', '.join(row_bco['entidades_detectadas']) if row_bco['entidades_detectadas'] else '',
                        'Palabras_Clave_Bco': ', '.join(row_bco['palabras_clave']) if row_bco['palabras_clave'] else '',
                        'Diferencia_Monto': round(float(abs(row_cont['monto'] - row_bco['monto'])), 2),
                        'Diferencia_Dias': int(abs((row_cont['fecha'] - row_bco['fecha']).days))
                    })
                
                df_coincidencias = pd.DataFrame(coincidencias_data)
                df_coincidencias = df_coincidencias.sort_values('Score_Total', ascending=False)
            else:
                df_coincidencias = pd.DataFrame()
            
            df_coincidencias.to_excel(writer, sheet_name='Coincidencias_Optimizadas', index=False)
            
            # HOJA 2: Contables Pendientes con Analisis
            cont_no_conciliadas = df_cont[~df_cont.index.isin(indices_cont_conciliados)].copy()
            
            if len(cont_no_conciliadas) > 0:
                cont_pendientes_data = []
                for idx, row in cont_no_conciliadas.iterrows():
                    
                    # Buscar candidatos cercanos usando rapidfuzz
                    candidatos = self.buscar_candidatos_cercanos(row, df_bco, indices_bco_conciliados)
                    
                    cont_pendientes_data.append({
                        'Estado': 'PENDIENTE_CONCILIAR',
                        'Fecha': row['fecha'].strftime('%d/%m/%Y'),
                        'Concepto': str(row['concepto'])[:180],
                        'Concepto_Normalizado': str(row['concepto_normalizado']),
                        'Monto': float(row['monto']),
                        'Tipo': str(row['tipo']),
                        'Entidades_Detectadas': ', '.join(row['entidades_detectadas']) if row['entidades_detectadas'] else '',
                        'Palabras_Clave': ', '.join(row['palabras_clave']) if row['palabras_clave'] else '',
                        'Candidatos_Cercanos': candidatos,
                        'Posibles_Causas': self.analizar_causa_no_conciliacion(row),
                        'Observaciones': 'Sin coincidencia en extracto bancario'
                    })
                
                df_cont_pendientes = pd.DataFrame(cont_pendientes_data)
            else:
                df_cont_pendientes = pd.DataFrame()
            
            df_cont_pendientes.to_excel(writer, sheet_name='Contables_Pendientes', index=False)
            
            # HOJA 3: Bancarias Pendientes con Analisis
            bco_no_conciliadas = df_bco[~df_bco.index.isin(indices_bco_conciliados)].copy()
            
            if len(bco_no_conciliadas) > 0:
                bco_pendientes_data = []
                for idx, row in bco_no_conciliadas.iterrows():
                    
                    # Buscar candidatos cercanos
                    candidatos = self.buscar_candidatos_cercanos(row, df_cont, indices_cont_conciliados)
                    
                    bco_pendientes_data.append({
                        'Estado': 'PENDIENTE_REGISTRAR',
                        'Fecha': row['fecha'].strftime('%d/%m/%Y'),
                        'Concepto': str(row['concepto'])[:180],
                        'Concepto_Normalizado': str(row['concepto_normalizado']),
                        'Monto': float(row['monto']),
                        'Tipo': str(row['tipo']),
                        'Entidades_Detectadas': ', '.join(row['entidades_detectadas']) if row['entidades_detectadas'] else '',
                        'Palabras_Clave': ', '.join(row['palabras_clave']) if row['palabras_clave'] else '',
                        'Candidatos_Cercanos': candidatos,
                        'Posibles_Causas': self.analizar_causa_no_conciliacion(row),
                        'Observaciones': 'Sin registro en contabilidad'
                    })
                
                df_bco_pendientes = pd.DataFrame(bco_pendientes_data)
            else:
                df_bco_pendientes = pd.DataFrame()
            
            df_bco_pendientes.to_excel(writer, sheet_name='Bancarias_Pendientes', index=False)
            
            # HOJA 4: Resumen Optimizado
            self.generar_resumen_optimizado(writer, df_cont, df_bco, coincidencias, info_archivo)
            
            # HOJA 5: Analisis de Algoritmos
            self.generar_analisis_algoritmos(writer, coincidencias)
        
        print(f"✅ Archivo mejorado generado: {os.path.basename(archivo_salida)}")
        return archivo_salida
    
    def generar_resumen_optimizado(self, writer, df_cont, df_bco, coincidencias, info_archivo):
        """Genera resumen con metricas de optimizacion"""
        
        total_cont = len(df_cont)
        total_bco = len(df_bco)
        total_conciliado = len(coincidencias)
        
        # Calcular metricas de algoritmo
        if coincidencias:
            scores = [c['score_total'] for c in coincidencias]
            score_promedio = np.mean(scores)
            score_maximo = np.max(scores)
            score_minimo = np.min(scores)
            
            # Metricas por componente
            scores_fecha = [c['score_fecha'] for c in coincidencias]
            scores_monto = [c['score_monto'] for c in coincidencias]
            scores_concepto = [c['score_concepto'] for c in coincidencias]
            
            # Distribucion por confianza
            distribucion = {}
            for c in coincidencias:
                nivel = c['nivel_confianza']
                distribucion[nivel] = distribucion.get(nivel, 0) + 1
        else:
            score_promedio = score_maximo = score_minimo = 0
            scores_fecha = scores_monto = scores_concepto = []
            distribucion = {}
        
        # Crear datos del resumen
        resumen_data = [
            ['=== INFORMACION GENERAL ===', ''],
            ['Banco', str(info_archivo['banco']).upper()],
            ['Cuenta', str(info_archivo['cuenta'])],
            ['Periodo', str(info_archivo['periodo'])],
            ['Fecha Procesamiento', datetime.now().strftime('%d/%m/%Y %H:%M:%S')],
            ['Algoritmo', 'RapidFuzz + Matriz Optimizada'],
            ['', ''],
            ['=== ALGORITMOS UTILIZADOS ===', ''],
            ['RapidFuzz', '✅ Disponible' if RAPIDFUZZ_AVAILABLE else '❌ No disponible'],
            ['Scikit-learn', '✅ Disponible' if SKLEARN_AVAILABLE else '❌ No disponible'],
            ['Scipy Hungarian', '✅ Disponible' if SCIPY_AVAILABLE else '❌ Greedy usado'],
            ['', ''],
            ['=== CONFIGURACION OPTIMIZADA ===', ''],
            ['Peso Fecha', f"{self.pesos['fecha']*100:.0f}%"],
            ['Peso Monto', f"{self.pesos['monto']*100:.0f}%"],
            ['Peso Concepto', f"{self.pesos['concepto']*100:.0f}%"],
            ['Peso Tipo', f"{self.pesos['tipo']*100:.0f}%"],
            ['Umbral Minimo', f"{self.umbral_coincidencia:.2f}"],
            ['Tolerancia Monto', f"{self.tolerancia_monto_pct*100:.1f}%"],
            ['Tolerancia Fecha', f"{self.tolerancia_fecha_dias} dias"],
            ['', ''],
            ['=== RESULTADOS ===', ''],
            ['Total Transacciones Contables', total_cont],
            ['Total Transacciones Bancarias', total_bco],
            ['Total Conciliadas', total_conciliado],
            ['% Conciliacion Contables', f"{(total_conciliado/total_cont)*100:.1f}%" if total_cont > 0 else "0%"],
            ['% Conciliacion Bancarias', f"{(total_conciliado/total_bco)*100:.1f}%" if total_bco > 0 else "0%"],
            ['Mejora vs 80% objetivo', f"{((total_conciliado/total_cont)*100 - 80):+.1f} puntos" if total_cont > 0 else "N/A"],
            ['', ''],
            ['=== METRICAS DE CALIDAD ===', ''],
            ['Score Promedio', f"{score_promedio:.3f}"],
            ['Score Maximo', f"{score_maximo:.3f}"],
            ['Score Minimo', f"{score_minimo:.3f}"],
            ['Promedio Score Fecha', f"{np.mean(scores_fecha):.3f}" if scores_fecha else "0"],
            ['Promedio Score Monto', f"{np.mean(scores_monto):.3f}" if scores_monto else "0"],
            ['Promedio Score Concepto', f"{np.mean(scores_concepto):.3f}" if scores_concepto else "0"],
            ['', ''],
            ['=== DISTRIBUCION POR CONFIANZA ===', ''],
        ]
        
        # Agregar distribucion por confianza
        for nivel in ['MUY_ALTA', 'ALTA', 'MEDIA', 'BAJA']:
            if nivel in distribucion:
                resumen_data.append([f'{nivel}', f"{distribucion[nivel]} coincidencias"])
        
        # Agregar recomendaciones especificas
        recomendaciones = self.generar_recomendaciones_automaticas(score_promedio, distribucion, total_conciliado, total_cont)
        resumen_data.extend([
            ['', ''],
            ['=== RECOMENDACIONES AUTOMATICAS ===', ''],
        ])
        resumen_data.extend(recomendaciones)
        
        df_resumen = pd.DataFrame(resumen_data, columns=['Metrica', 'Valor'])
        df_resumen['Metrica'] = df_resumen['Metrica'].astype(str)
        df_resumen['Valor'] = df_resumen['Valor'].astype(str)
        
        df_resumen.to_excel(writer, sheet_name='Resumen_Optimizado', index=False)
    
    def generar_recomendaciones_automaticas(self, score_promedio, distribucion, total_conciliado, total_cont):
        """Genera recomendaciones automaticas basadas en metricas"""
        recomendaciones = []
        
        porcentaje = (total_conciliado/total_cont)*100 if total_cont > 0 else 0
        
        if porcentaje >= 95:
            recomendaciones.append(['Estado', '🎉 Excelente conciliacion'])
        elif porcentaje >= 85:
            recomendaciones.append(['Estado', '✅ Buena conciliacion'])
        elif porcentaje >= 75:
            recomendaciones.append(['Estado', '⚠️ Conciliacion mejorable'])
        else:
            recomendaciones.append(['Estado', '❌ Requiere optimizacion'])
        
        if score_promedio < 0.7:
            recomendaciones.append(['Score Bajo', 'Revisar umbrales y tolerancias'])
        
        if distribucion.get('BAJA', 0) > 5:
            recomendaciones.append(['Muchas BAJA confianza', 'Revisar manualmente coincidencias BAJA'])
        
        if RAPIDFUZZ_AVAILABLE:
            recomendaciones.append(['RapidFuzz', '✅ Algoritmo optimizado activo'])
        else:
            recomendaciones.append(['Optimizacion', 'Instalar rapidfuzz para mejor rendimiento'])
        
        if not SKLEARN_AVAILABLE:
            recomendaciones.append(['Analisis vectorial', 'Instalar scikit-learn para TF-IDF'])
        
        return recomendaciones
    
    def generar_analisis_algoritmos(self, writer, coincidencias):
        """Genera analisis detallado de los algoritmos utilizados"""
        
        analisis_data = [
            ['=== ANALISIS DE ALGORITMOS ===', ''],
            ['Algoritmo Principal', 'Matriz de Coincidencia Optimizada'],
            ['Similitud de Texto', 'RapidFuzz' if RAPIDFUZZ_AVAILABLE else 'SequenceMatcher'],
            ['Analisis Vectorial', 'TF-IDF + Cosine Similarity' if SKLEARN_AVAILABLE else 'No disponible'],
            ['Optimizacion', 'Hungarian Algorithm' if SCIPY_AVAILABLE else 'Greedy Algorithm'],
            ['', ''],
            ['=== METRICAS RAPIDFUZZ ===', ''],
        ]
        
        if RAPIDFUZZ_AVAILABLE and coincidencias:
            # Analisis de que metodos de rapidfuzz funcionan mejor
            metodos_usados = ['ratio', 'partial_ratio', 'token_sort_ratio', 'token_set_ratio']
            analisis_data.append(['Metodos Disponibles', ', '.join(metodos_usados)])
            analisis_data.append(['Ventaja vs SequenceMatcher', 'Hasta 10x mas rapido'])
            analisis_data.append(['Precision', 'Mayor deteccion de similitudes parciales'])
        else:
            analisis_data.append(['Estado RapidFuzz', 'No disponible - usando fallback'])
            analisis_data.append(['Impacto', 'Menor precision en conceptos similares'])
        
        analisis_data.extend([
            ['', ''],
            ['=== OPTIMIZACIONES APLICADAS ===', ''],
            ['Normalizacion de conceptos', 'Avanzada con filtrado de ruido'],
            ['Deteccion de entidades', 'Especifica para entidades argentinas'],
            ['Extraccion de palabras clave', 'Filtrado inteligente'],
            ['Mapeo de categorias', 'Especifico para transacciones bancarias'],
            ['Matriz de similitud', 'Construccion optimizada con progreso'],
            ['', ''],
            ['=== RECOMENDACIONES TECNICAS ===', ''],
            ['Para mejorar velocidad', 'pip install rapidfuzz'],
            ['Para analisis semantico', 'pip install scikit-learn'],
            ['Para optimizacion maxima', 'pip install scipy'],
            ['Ajuste de umbrales', 'Basado en distribucion de scores'],
        ])
        
        df_analisis = pd.DataFrame(analisis_data, columns=['Aspecto', 'Detalle'])
        df_analisis.to_excel(writer, sheet_name='Analisis_Algoritmos', index=False)
    
    def conciliar_par_mejorado(self, info_archivo):
        """Concilia un par con todos los algoritmos optimizados"""
        try:
            print(f"\n🎯 Procesando con algoritmos mejorados: {info_archivo['banco'].upper()}-{info_archivo['cuenta']}-{info_archivo['periodo']}")
            print("-" * 60)
            
            # Cargar archivos
            df_cont = self.cargar_archivo_contable(info_archivo['archivo_cont'])
            df_bco = self.cargar_archivo_bancario(info_archivo['archivo_bco'])
            
            print(f"📊 Datos cargados: {len(df_cont)} contables, {len(df_bco)} bancarias")
            
            # Mostrar estadisticas de entidades detectadas
            total_entidades_cont = sum(len(row['entidades_detectadas']) for _, row in df_cont.iterrows())
            total_entidades_bco = sum(len(row['entidades_detectadas']) for _, row in df_bco.iterrows())
            print(f"🏢 Entidades detectadas: {total_entidades_cont} en contables, {total_entidades_bco} en bancarias")
            
            # Encontrar coincidencias con algoritmo optimizado
            print(f"🚀 Ejecutando algoritmo mejorado...")
            coincidencias = self.encontrar_coincidencias_optimas(df_cont, df_bco)
            
            # Generar archivo procesado optimizado
            archivo_salida = self.generar_archivo_procesado_mejorado(df_cont, df_bco, coincidencias, info_archivo)
            
            # Calcular metricas detalladas
            porcentaje_cont = (len(coincidencias)/len(df_cont)*100) if len(df_cont) > 0 else 0
            porcentaje_bco = (len(coincidencias)/len(df_bco)*100) if len(df_bco) > 0 else 0
            
            # Mostrar resultados optimizados
            print(f"✅ Conciliacion mejorada completada:")
            print(f"   📊 Coincidencias: {len(coincidencias)}")
            print(f"   📈 % Contables: {porcentaje_cont:.1f}%")
            print(f"   📈 % Bancarias: {porcentaje_bco:.1f}%")
            print(f"   📄 Archivo: {os.path.basename(archivo_salida)}")
            
            if coincidencias:
                scores = [c['score_total'] for c in coincidencias]
                print(f"   🎯 Score promedio: {np.mean(scores):.3f}")
                print(f"   🎯 Score maximo: {np.max(scores):.3f}")
                
                # Mostrar distribucion de confianza
                distribucion = {}
                for c in coincidencias:
                    nivel = c['nivel_confianza']
                    distribucion[nivel] = distribucion.get(nivel, 0) + 1
                
                print(f"   📊 Distribucion: ", end="")
                for nivel, count in distribucion.items():
                    print(f"{nivel}: {count}", end="  ")
                print()
            
            mejora = porcentaje_cont - 80
            if mejora > 0:
                print(f"   🎉 Mejora: +{mejora:.1f} puntos vs objetivo 80%")
            elif mejora > -5:
                print(f"   ⚠️ Resultado: {mejora:+.1f} puntos vs objetivo 80%")
            else:
                print(f"   ❌ Bajo rendimiento: {mejora:+.1f} puntos vs objetivo 80%")
            
            return {
                'exito': True,
                'archivo_salida': archivo_salida,
                'total_cont': len(df_cont),
                'total_bco': len(df_bco),
                'conciliadas': len(coincidencias),
                'porcentaje_cont': porcentaje_cont,
                'porcentaje_bco': porcentaje_bco,
                'score_promedio': np.mean([c['score_total'] for c in coincidencias]) if coincidencias else 0,
                'mejora': mejora,
                'distribucion': distribucion if coincidencias else {}
            }
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return {'exito': False, 'error': str(e)}
    
    def procesar_todos_mejorado(self):
        """Procesa todos los pares con algoritmo completamente mejorado"""
        
        print("🚀 CONCILIADOR AUTOMATICO MEJORADO - ALGORITMO OPTIMIZADO")
        print("=" * 70)
        
        # Verificar dependencias
        self.verificar_dependencias()
        
        # Crear directorios
        os.makedirs("Procesado", exist_ok=True)
        
        # Encontrar pares
        pares = self.encontrar_pares_archivos()
        
        if not pares:
            print("❌ No se encontraron pares de archivos para conciliar")
            return []
        
        print(f"\n📋 Encontrados {len(pares)} pares para conciliar")
        print("🎯 Configuracion optimizada:")
        print(f"   • RapidFuzz: {'✅ Activo' if RAPIDFUZZ_AVAILABLE else '❌ No disponible'}")
        print(f"   • TF-IDF: {'✅ Activo' if SKLEARN_AVAILABLE else '❌ No disponible'}")
        print(f"   • Hungarian: {'✅ Activo' if SCIPY_AVAILABLE else '❌ Greedy usado'}")
        print(f"   • Peso Monto: {self.pesos['monto']*100:.0f}%")
        print(f"   • Peso Fecha: {self.pesos['fecha']*100:.0f}%")
        print(f"   • Peso Concepto: {self.pesos['concepto']*100:.0f}%")
        print(f"   • Umbral: {self.umbral_coincidencia:.2f}")
        
        resultados = []
        exitosos = 0
        total_mejora = 0
        mejores_scores = []
        
        for i, par in enumerate(pares, 1):
            print(f"\n[{i}/{len(pares)}] Procesando...")
            
            resultado = self.conciliar_par_mejorado(par)
            resultado['info'] = par
            resultados.append(resultado)
            
            if resultado['exito']:
                exitosos += 1
                total_mejora += resultado['mejora']
                mejores_scores.append(resultado['score_promedio'])
        
        # Resumen final optimizado
        print(f"\n📊 RESUMEN FINAL MEJORADO")
        print("=" * 70)
        print(f"Total procesados: {len(pares)}")
        print(f"Exitosos: {exitosos}")
        print(f"Con errores: {len(pares) - exitosos}")
        
        if exitosos > 0:
            mejora_promedio = total_mejora / exitosos
            score_promedio_global = np.mean(mejores_scores)
            
            print(f"Mejora promedio: {mejora_promedio:+.1f} puntos vs objetivo 80%")
            print(f"Score promedio global: {score_promedio_global:.3f}")
            
            if mejora_promedio > 10:
                print("🎉 ¡Excelente optimizacion lograda!")
            elif mejora_promedio > 5:
                print("✅ Buena mejora con algoritmos optimizados")
            elif mejora_promedio > 0:
                print("👍 Mejora moderada detectada")
            else:
                print("⚠️ Revisar configuracion - puede necesitar ajustes")
            
            print(f"\n✅ Archivos mejorados generados en 'Procesado/':")
            for resultado in resultados:
                if resultado['exito']:
                    info = resultado['info']
                    porcentaje = resultado['porcentaje_cont']
                    mejora = resultado['mejora']
                    print(f"   📄 {info['banco']}_pro_{info['cuenta']}_{info['periodo']}.xlsx ({porcentaje:.1f}%, {mejora:+.1f}pts)")
        
        return resultados

def main():
    """Funcion principal mejorada"""
    conciliador = ConciliadorAutomaticoMejorado()
    resultados = conciliador.procesar_todos_mejorado()
    
    if not resultados:
        print("\n💡 INSTRUCCIONES PARA USAR EL CONCILIADOR MEJORADO:")
        print("=" * 60)
        print("1. Coloca archivos contables en: Contable/")
        print("2. Coloca archivos bancarios en: Bancos/")
        print("3. Formato de nombres: banco_tipo_cuenta_periodo.xls")
        print("   Ejemplo: credi_cont_01_062025.xls")
        print("            credi_bco_01_062025.xls")
        print("\n🚀 DEPENDENCIAS RECOMENDADAS PARA MAXIMO RENDIMIENTO:")
        print("pip install rapidfuzz scikit-learn scipy")
        print("\n📈 BENEFICIOS DEL ALGORITMO MEJORADO:")
        print("• RapidFuzz: Hasta 10x mas rapido en similitud de texto")
        print("• TF-IDF: Analisis vectorial inteligente de conceptos")
        print("• Hungarian: Asignacion matematicamente optima")
        print("• Deteccion mejorada de entidades argentinas")
        print("• Analisis automatico de candidatos cercanos")
        print("• Recomendaciones automaticas basadas en metricas")

if __name__ == "__main__":
    main()
