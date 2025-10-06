from ..database.connection      import db
from flask                      import jsonify
from config                     import Config

import io 

#Logger
from ..utils.Logger                     import Logger

#Mapeos
from ..utils.Mapeo                      import Mapeo

from ..services.beneficiarios_service   import BeneficiariosService
from ..services.contacto_service        import ContactosService 
from ..services.apoyo_service           import ApoyosService
from ..services.search_service          import SearchService

import traceback

import polars as pl 
import uuid


class ExcelService:
    
    @staticmethod
    def process_file(file):
        try:
            Logger.add_to_log("info", "="*30)
            Logger.add_to_log("info", f"INICIO DE CARGA MASIVA")
            Logger.add_to_log("info", "="*30)
            
            data = pl.read_excel(io.BytesIO(file.read()))
            rows = data.to_dicts()
            
            Logger.add_to_log("info", "Columnas de los datos")
            Logger.add_to_log("info", data.columns)
            
            Logger.add_to_log("info", f"Total de filas del Excel: {len(rows)}")
            
            Logger.add_to_log("info", f"Inicio de estrucutura de los datos .....")
            
            # Listado de Beneficiarios Nuevos para insertar en BD
            beneficiarios_to_insert = []
            
            # Set de IDs de beneficiarios nuevos 
            beneficiarios_nuevos_ids = set()
            
            # Lista de relacciones completas: fila -> beneficiario -> contacto -> apoyo
            relaciones = []
            
            # Lista de filas con errores de validacion
            rows_errors = []
            
            """
                CACHE LOCAL: Detecta duplicidad DENTRO del Excel
                Estrucutura: {(curp, rfc): id_beneficiario}
            """
            cache_beneficiarios_excel = {}
            
            Logger.add_to_log("info", f"Inicio de estrucutura correctamente")
            
            Logger.add_to_log("info", "Extrayendo grupos de columnas")
            
            # INICIO de agrupamiento
            
            # GRUPO 1: Columna de Beneficiarios
            group_one_df = data.select(Config.GROUP_ONE_KEYS).to_dict()
            Logger.add_to_log("info", f"  - Grupo 1 (Beneficiarios): {len(Config.GROUP_ONE_KEYS)} columnas")
           
            # GRUPO 2: Columnas de Contacto
            group_two_df = data.select(Config.GROUP_TWO_KEYS).to_dict()
            Logger.add_to_log("info", f"  - Grupo 2 (Contacto): {len(Config.GROUP_TWO_KEYS)} columnas")
           
            # GRUPO 3: Columnas de Apoyos
            group_tree_df = data.select(Config.GROUP_TREE_KEYS).to_dict()
            Logger.add_to_log("info", f"  - Grupo 3 (Apoyos): {len(Config.GROUP_TREE_KEYS)} columnas")
            
            # FIN de agrupamiento
            
            # MAPEO por Grupos
            # Grupo 1 - Beneficiarios
            Logger.add_to_log("info", f"\nGrupo 1")
            sexos_map = SearchService.get_sexo_map()
            Logger.add_to_log("info", f"  ✓ Sexos: {len(sexos_map)} registros")
            
            # Grupo 2 - Contactos
            Logger.add_to_log("info", f"\nGrupo 2")
            estados_map = SearchService.get_estado_map()
            Logger.add_to_log("info", f"  ✓ Estados: {len(estados_map)} registros")
            municipios_map = SearchService.get_municipio_map()
            Logger.add_to_log("info", f"  ✓ Municipios: {len(municipios_map)} registros")
            colonias_map = SearchService.get_colonia_map()
            Logger.add_to_log("info", f"  ✓ Colonias: {len(colonias_map)} registros")
            estados_civiles_map = SearchService.get_estado_civil_map()
            Logger.add_to_log("info", f"  ✓ Estados Civiles: {len(estados_civiles_map)} registros")
        
            # Grupo 3 - Apoyos
            Logger.add_to_log("info", f"\nGrupo 3")
            dependencias_map = SearchService.get_dependencias_map()
            Logger.add_to_log("info", f"  ✓ Dependencias: {len(dependencias_map)} registros")
            programas_map = SearchService.get_programas_map()
            Logger.add_to_log("info", f"  ✓ Programas: {len(programas_map)} registros")
            componentes_map = SearchService.get_componentes_map()
            Logger.add_to_log("info", f"  ✓ Componentes: {len(componentes_map)} registros")
            acciones_map = SearchService.get_acciones_map()
            Logger.add_to_log("info", f"  ✓ Acciones: {len(acciones_map)} registros")
            tipos_beneficiarios_map = SearchService.get_tipos_beneficiarios_map()
            Logger.add_to_log("info", f"  ✓ Tipos de Beneficiarios: {len(tipos_beneficiarios_map)} registros")
            
            # Mapa de beneficiarios existentes en BD (para detectar duplicados con BD)
            beneficiario_map = SearchService.get_beneficiarios_map()
            Logger.add_to_log("info", f"  ✓ Beneficiarios existentes en BD: {len(beneficiario_map)} registros")
            Logger.add_to_log("info", f"  ✓ Beneficiarios: {beneficiario_map}")
        
            # Carpeta de Beneficiarios 
            carpetas_beneficiarios_map = SearchService.get_carpeta_beneficiarios_map()
            Logger.add_to_log("info", f"  ✓ Carpetas Beneficiarios: {len(carpetas_beneficiarios_map)} registros")
            
            Logger.add_to_log("info", "✓ Todos los catálogos cargados exitosamente")
            
            # Diccionario de Estadistica
            stats = {
                'total_filas': len(rows),
                'beneficiarios_nuevos':0,
                'beneficiarios_existentes_db':0,
                'duplicados_en_excel':0,
                'errores_validacion':0
            }
            
            for idx, row in enumerate(rows):
                Logger.add_to_log("debug", f"--- Procesando fila {idx + 1}/{len(rows)} ---")
                Logger.add_to_log("info", idx)
                Logger.add_to_log("info", row)
            
                # Grupo 1 - Beneficiarios
                id_sexo = sexos_map.get(row.get('Sexo'))
                
                Logger.add_to_log("info", f"Id Sexo: {id_sexo}")
                
                # Grupo 2 - Contacto
                id_estado = estados_map.get(row.get('Estado (catálogo)'))
                Logger.add_to_log("info", f"Id Estado: {id_estado}")
                
                id_municipio = municipios_map.get(row.get('Municipio Dirección (catálogo)'))
                Logger.add_to_log("info", f"Id Municipio: {id_municipio}")
                id_colonia = colonias_map.get(row.get('Colonia'))
                Logger.add_to_log("info", f"Id Colonia: {id_colonia}")
                id_estado_civil = estados_civiles_map.get(row.get('Estado Civil'))
                Logger.add_to_log("info", f"Id Estado Civil {id_estado_civil}")
                
                # Grupo 3 - Apoyos
                id_dependencia = dependencias_map.get(row.get('Dependencia'))
                Logger.add_to_log("info", f"Id Dependencia {id_dependencia}")
                id_programa = programas_map.get((row.get('Programa'), id_dependencia)) if id_dependencia else None
                Logger.add_to_log("info", f"Id Programa {id_programa}")
                
                id_componente = componentes_map.get((row.get('Componente'), id_programa)) if id_programa else None
                Logger.add_to_log("info", f"Id Componente {id_componente}")
                
                id_acciones = acciones_map.get(row.get('Accion'))
                Logger.add_to_log("info", f"Id Acciones: {id_acciones}")
                
                id_tipo_beneficiario = tipos_beneficiarios_map.get(row.get('Tipo de Beneficio'))
                Logger.add_to_log("info", f"Id Tipo Beneficiario { id_tipo_beneficiario }")

                # Valicacciones
                validacion_errores = {}
                
                if not id_dependencia:
                    validacion_errores['Dependecia'] = row.get('Dependencia')
               
                if not id_programa:
                    validacion_errores['Programa'] = row.get('Programa')    

                if not id_componente:
                    validacion_errores['Componente'] = row.get('Componente')
                
                if not id_acciones:
                    validacion_errores['Accion'] = row.get('Accion')
                
                if validacion_errores:
                    stats['errores_validacion'] += 1
                    
                    error_detail = {
                        'row_index': idx + 1,
                        'curp': row.get('Curp'),
                        'nombre_completo': f"{row.get('Nombre', '')} {('Apellido paterno', '')} {row.get('Apellido Materno', '')}".strip(),
                        'error': 'Faltan datos obligatorios',
                        'campos_invalidos': validacion_errores,
                        'data': row
                    }
                    
                    rows_errors.append(error_detail)
                    
                    Logger.add_to_log('warn', f'Fila {idx+1} rechazada - Faltan: {', '.join(validacion_errores.keys()) }')
                    Logger.add_to_log('warn', rows_errors)
                    continue
                
                curp = row.get('Curp') or None
                rfc  = row.get('RFC') or None
                
                # Quitar los espacio
                if curp:
                    curp = curp.strip()
                if rfc:
                    rfc = rfc.strip()

                id_beneficiario = None
                es_nuevo = False
                origen = "" # Para Tracking: 'cache_excel', 'db', 'nuevo'
                
                
                
                
                
        except Exception as ex:
            return jsonify({
                'success': False,
                'message': 'Error en el proceso de carga masiva',
                'error': str(ex)
            }),500
        
       