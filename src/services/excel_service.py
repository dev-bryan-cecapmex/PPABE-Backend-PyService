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

from datetime                           import datetime

import traceback

import polars as pl 
import uuid

import os
from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side


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
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", f"GRUPO 1")
            Logger.add_to_log("info", "=" * 60)
            sexos_map = SearchService.get_sexo_map()
            Logger.add_to_log("info", f"  ✓ Sexos: {len(sexos_map)} registros")
            
            # Grupo 2 - Contactos
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", f"GRUPO 2")
            Logger.add_to_log("info", "=" * 60)
            estados_map = SearchService.get_estado_map()
            Logger.add_to_log("info", f"  ✓ Estados: {len(estados_map)} registros")
            municipios_map = SearchService.get_municipio_map()
            Logger.add_to_log("info", f"  ✓ Municipios: {len(municipios_map)} registros")
            colonias_map = SearchService.get_colonia_map()
            Logger.add_to_log("info", f"  ✓ Colonias: {len(colonias_map)} registros")
            estados_civiles_map = SearchService.get_estado_civil_map()
            Logger.add_to_log("info", f"  ✓ Estados Civiles: {len(estados_civiles_map)} registros")
        
            # Grupo 3 - Apoyos
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", f"GRUPO 3")
            Logger.add_to_log("info", "=" * 60)
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
            Logger.add_to_log("info", carpetas_beneficiarios_map)
            
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
                Logger.add_to_log("info", idx + 1)
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
                
                # Carpeta Beneficiario
                
                fecha_plantilla= row.get('Fecha de Registro')
                
                if isinstance(fecha_plantilla, str):
                    # Plantilla del sistema
                    fecha_str = fecha_plantilla
                    fecha = datetime.strptime(fecha_str, "%d/%m/%Y" )
                    Logger.add_to_log("warn", f"Fecha String: {fecha_str}")
                else:
                    # Plantilla Ruy
                    fecha = fecha_plantilla
                    Logger.add_to_log("warn",f"Fecha:{fecha}")
                    
                mes     = fecha.month 
                anio    = fecha.year
                Logger.add_to_log('warn', f"{mes} - {anio}")
                id_carpeta_beneficiario = carpetas_beneficiarios_map.get((mes, anio))
                Logger.add_to_log('warn', f"{mes} - {anio} id: {id_carpeta_beneficiario}")
            
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
                
                # Buscar en CHACHE LOCAL del Excel
                if curp or rfc:
                    key_beneficiario = (curp, rfc)
                    Logger.add_to_log('info', f"Beneficiarios {key_beneficiario}")
                    
                if key_beneficiario in cache_beneficiarios_excel:
                    id_beneficiario = cache_beneficiarios_excel[key_beneficiario]
                    stats['duplicados_en_excel'] +=1
                    origen = 'cache_excel'
                    Logger.add_to_log("info", f"Fila: {idx + 1}: DUPLICADO EN EXCEL - CURP: {curp}, RFC:{rfc} -> Reutilizando ID: {id_beneficiario[:8]}....")
                
                # Busqueda por solo CURP en cache
                elif curp and not id_beneficiario:
                    for(c,r), id_ben in cache_beneficiarios_excel.items():
                        if c == curp:
                            id_beneficiario = id_ben
                            stats['duplicados_en_excel'] += 1
                            origen = "cache_excel"
                            Logger.add_to_log("info", f"Fila: {idx + 1}: DUPLICADO EN EXCEL (por CURP) - {curp} -> Reutilizando ID: {id_beneficiario[:8]}....")
                            break
                            
                # Busqueda por solo RFC en cache  
                elif rfc and not id_beneficiario:
                    for(c,r), id_ben in cache_beneficiarios_excel.items():
                        stats['duplicados_en_excel'] += 1
                        origen = "cache_excel"
                        Logger.add_to_log("info", f"Fila: {idx + 1}: DUPLICADO EN EXCEL (por RFC) - {rfc} -> Reutilizando ID: {id_beneficiario[:8]}...." )
                        break
                    
                # Busqueda en Base de Datos
                if not id_beneficiario:
                    # Busqueda por CURP y RFC
                    if curp and rfc:
                        id_beneficiario = beneficiario_map.get((curp, rfc))
                        if id_beneficiario:
                            origen = 'db'
                            
                    # Busqueda solo por CURP
                    elif curp and not id_beneficiario:
                        id_beneficiario = next(
                            (id_ben for (c,_), id_ben in beneficiario_map.items() if c == curp),
                            None
                        )
                        if id_beneficiario:
                            origen ='db'
                            
                    # Busqueda solo por RFC
                    elif rfc and not id_beneficiario:
                        id_beneficiario = next(
                            (id_ben for (_,r), id_ben in beneficiario_map.items() if r == rfc),
                            None
                        )
                        if id_beneficiario:
                            origen = 'db'
                    
                    if id_beneficiario and origen == 'db':
                        stats['beneficiarios_existentes_bd'] += 1
                        Logger.add_to_log("info", f"Fila {idx +1}: Beneficiario EXISTENTE en BD - CURP: {curp}, RFC: {rfc} -> ID: {id_beneficiario[:8]}....")
                        
                # Crea Nuevo beneficiario
                if not id_beneficiario:
                    id_beneficiario = str(uuid.uuid4())
                    es_nuevo = True
                    origen = "nuevo"
                    stats['beneficiarios_nuevos'] += 1
                    beneficiarios_nuevos_ids.add(id_beneficiario)
                    
                    # Objeto con beneficiario con mapeo correcto
                    nuevo_beneficiario = {
                        'id': id_beneficiario
                    }
                    
                    # Mapeo columnas del Excel a columnas de BD
                    for excel_col in Config.GROUP_ONE_KEYS:
                        db_col = Config.COLUMN_MAP_GROUP_ONE.get(excel_col, excel_col)
                        nuevo_beneficiario[db_col] = row.get((excel_col))
                    
                    # Asegurar que tenga el idSexo correcto
                    nuevo_beneficiario['idSexo'] = id_sexo

                    # Agregar a lista de inserción 
                    beneficiarios_to_insert.append(nuevo_beneficiario) 
                    
                    # REGISTRO en CACHE LOCAL
                    if curp or rfc:
                        cache_beneficiarios_excel[(curp, rfc)] = id_beneficiario
                    
                    Logger.add_to_log("info", f"Fila {idx + 1}: Beneficiario NUEVO - CURP {curp}, RFC: {rfc} -> ID: {id_beneficiario[:8]}....")
                                           
                    
                # Preparacion de contacto y apoyo
                
                # Pre-generar UUIDs temporales
                id_contacto_temp    = str(uuid.uuid4())
                id_apoyo_temp       = str(uuid.uuid4())
                
                # Construccion de objeto de CONTACTO
                contacto_data = {
                    'id': id_contacto_temp
                }
                
                for excel_col in Config.GROUP_TWO_KEYS:
                    if excel_col in Config.COLUMN_MAP_GROUP_TWO:
                        db_col = Config.COLUMN_MAP_GROUP_TWO[excel_col]
                        contacto_data[db_col] = row.get(excel_col)
                
                # Agregar ID's de catálogos
                contacto_data['idEstado']       = id_estado
                contacto_data['idMunicipio']    = str(id_municipio[1]) if id_municipio else None
                contacto_data['idColonia']      = str(id_colonia[0]) if id_colonia else None
                contacto_data['idEstadoCivil']  = id_estado_civil
                
                
                # Construccion de objeto de APOYO
                apoyo_data = {
                    'id': id_apoyo_temp,
                    'idBeneficiario': id_beneficiario,
                    'idContacto': id_contacto_temp
                }
                
                # Mapear columnas del Excel a columnas de DB
                for excel_col in Config.GROUP_TREE_KEYS:
                    db_col = Config.COLUMN_MAP_GROUP_TREE.get(excel_col, excel_col)
                    apoyo_data[db_col] = row.get(excel_col)
                
                # Agregar IDs de catálogos
                apoyo_data['idDependencia']     = id_dependencia
                apoyo_data['idPrograma']        = id_programa
                apoyo_data['idComponente']      = id_componente
                apoyo_data['idAccion']          = id_acciones
                apoyo_data['idTipoBeneficio']   = id_tipo_beneficiario  
                # Agregar despues idCarpetaBeneficiarios
                apoyo_data['idCarpetaBeneficiarios'] = id_carpeta_beneficiario
                # Registro de relación completa
                relacion = {
                    'row_index': idx + 1,
                    'id_beneficiario': id_beneficiario,
                    'id_contacto': id_contacto_temp,
                    'id_apoyo': id_apoyo_temp,
                    'es_beneficiario_nuevo': es_nuevo,
                    'origen_beneficiario': origen,
                    'contacto_data': contacto_data,
                    'apoyo_data': apoyo_data,
                    # Datos para reporte
                    'curp': curp,
                    'rfc': rfc,
                    'nombre_completo': f"{row.get('Nombre',' ')} {row.get('Apellido paterno','')} {row.get('Apellido Materno','')}".strip()
                }
                
                relaciones.append(relacion)
                 
                Logger.add_to_log("debug", f"Fila {idx +1} procesada correctamente")
                
                # Fin de loop
            # Estadistica y reporte de duplicados
            Logger.add_to_log("info", "")
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", "📊 ESTADÍSTICAS DE FASE 1:")
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", f"  Total de filas procesadas: {stats['total_filas']}")
            Logger.add_to_log("info", f"  ✨ Beneficiarios NUEVOS: {stats['beneficiarios_nuevos']}")
            Logger.add_to_log("info", f"  ✓ Beneficiarios EXISTENTES en BD: {stats['beneficiarios_existentes_db']}")
            Logger.add_to_log("info", f"  ♻️  Duplicados EN EXCEL: {stats['duplicados_en_excel']}")
            Logger.add_to_log("info", f"  ⚠️  Errores de validación: {stats['errores_validacion']}")
            Logger.add_to_log("info", f"  📝 Relaciones válidas creadas: {len(relaciones)}")
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", "")
            
            # Reporte detallado de duplicados en Excel
            if stats['duplicados_en_excel'] > 0:
                Logger.add_to_log("warn", "⚠️  REPORTE DE DUPLICADOS EN EXCEL:")
                Logger.add_to_log("warn", "-" * 60)
            
                # Contar ocurrencias de cada beneficiario
                beneficiario_ocurrencias = {}
                for rel in relaciones:
                    id_ben = rel['id_beneficiario']
                    if id_ben not in beneficiario_ocurrencias:
                        beneficiario_ocurrencias[id_ben] = {
                            'count': 0,
                            'curp': rel['curp'],
                            'rfc': rel['rfc'],
                            'nombre': rel['nombre_completo'],
                            'filas': []
                        }
                    beneficiario_ocurrencias[id_ben]['count'] += 1
                    beneficiario_ocurrencias[id_ben]['filas'].append(rel['row_index'])
                
                # Filtrar solo los que aparecen más de una vez
                duplicados = {k: v for k, v in beneficiario_ocurrencias.items() if v['count'] > 1}
                
                for id_ben, info in duplicados.items():
                    Logger.add_to_log("warn", f"  • {info['nombre']}")
                    Logger.add_to_log("warn", f"    CURP: {info['curp']}, RFC: {info['rfc']}")
                    Logger.add_to_log("warn", f"    Aparece {info['count']} veces en filas: {info['filas']}")
                    Logger.add_to_log("warn", "")
            
            # Reporte de errores de validación 
            if rows_errors:
                Logger.add_to_log("error", "REPORTE DE ERRORES DE VALIDACIÓN")
                Logger.add_to_log("error","-" * 60)
                
                for error in rows_errors[:10]: # Muesta solo primero 10
                    Logger.add_to_log("error", f"  Fila {error['row_index']}: {error.get('nombre_completo', 'N/A')}")
                    Logger.add_to_log("error", f"    CURP: {error.get('curp', 'N/A')}")
                    Logger.add_to_log("error", f"    Error: {error['error']}")
                    Logger.add_to_log("error", f"    Campos inválidos: {error['campos_invalidos']}")
                    Logger.add_to_log("error", "")
                
                if len(rows_errors) > 10:
                    Logger.add_to_log("error", f"  ... y {len(rows_errors) - 10} errores más")

            Logger.add_to_log("info", "")
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", "FASE 1 COMPLETADA - INICIANDO FASE 2...")
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", "")
            
            Logger.add_to_log("info","INICIANDO INSERCIÓN EN BASE DE DATOS ...")
            
            if not relaciones:
                Logger.add_to_log("warn", "No hay datos validos para insertar")
                return jsonify({
                    'success':False,
                    'message':'No se encontraron registros validos para procesar',
                     'data':{
                        'total_filas':stats['total_filas'],
                        'errores': stats['errores_validacion'],
                        'errores_detalle': rows_errors
                    },
                    'error':'Sin datos válidos'
                }),400
                
            # Insercion de beneficiarios nuevos
            if beneficiarios_to_insert:
                try:
                    Logger.add_to_log('info', f"💾 🗄️ Insertando {len(beneficiarios_to_insert)} beneficiarios nuevos ...")
                    Logger.add_to_log('debug', f"Primeros 3 beneficiarios: { beneficiarios_to_insert[:3]}")
                    # Llamada de al servicio de insercion
                    BeneficiariosService.bulk_insert(beneficiarios_to_insert)
                    Logger.add_to_log('info', f"✅ 💾 {len(beneficiarios_to_insert)} beneficiarios insertados exitosamente")
                except Exception as e:  
                    Logger.add_to_log('error', "❌ 💾 ERROR AL INSERTAR BENEFICIARIOS")
                    Logger.add_to_log('error', f"Detalles: {str(e)}")      
                    Logger.add_to_log("error", traceback.format_exc())
                    
                    return jsonify({
                        'success':False,
                        'message':'Error al insertar beneficiarios',
                        'data':{
                            'fase_fallida':'Insercion de Beneficiarios',
                            'beneficiarios_intentados': len(beneficiarios_to_insert)
                        },
                        'error':str(e)
                    }), 500       
            else:
                Logger.add_to_log("info", "✅ 💾 No hay beneficiarios nuevos para insertar")
                     
            # Preparacion de Contactos y Apoyos
            Logger.add_to_log('info', "Preparando lista de contactos y apoyos ...")
            
            contactos_to_insert = []
            apoyos_to_insert    = []
            
            for relacion in relaciones:
                # Extraer datos ya mapedas
                contactos_to_insert.append(relacion['contacto_data'])
                apoyos_to_insert.append(relacion['apoyo_data'])
            
            Logger.add_to_log("info", f"{len(contactos_to_insert)} contactos preparados")
            Logger.add_to_log("info", f"{len(apoyos_to_insert)} apoyos preparados")
            
            # INSERCIÓN DE CONTACTOS
            if contactos_to_insert:
                try:
                    Logger.add_to_log("info", f"💾 🗄️ Insertando {len(contactos_to_insert)} contactos nuevos ...")
                    Logger.add_to_log('debug', f"Primeros 3 contactos: {contactos_to_insert[:3]}")
                    # Llamada de al servicio de insercion
                    ContactosService.bluk_insert(contactos_to_insert)
                    Logger.add_to_log('info', f"✅ 💾 {len(contactos_to_insert)} contactos insertados exitosamente")

                except Exception as e:
                    Logger.add_to_log("error", "❌ 💾 ERROR AL INSERTAR CONTACTOS")
                    Logger.add_to_log("error", f"Detalles: {str(e)}")
                    Logger.add_to_log("error", traceback.format_exc())
                    
                    return jsonify({
                        'success':False,
                        'message':'Error al insertar contactos',
                        'data':{
                            'fase_fallida':'Insercion de contactos',
                            'beneficiarios_insertados': len(beneficiarios_to_insert),
                            'contactos_intentados': len(contactos_to_insert),
                            'warning':'Los beneficiarios quedaron en BD sin contactos asociados'
                        },
                        'error':str(e)
                    }), 500   
            else:
                Logger.add_to_log("warn", "✅ 💾 No hay contactos para insertar")       
            
            # INSERCIÓN DE APOYOS
            if apoyos_to_insert:
                try:
                    Logger.add_to_log("info", f"💾 🗄️ Insertando {len(apoyos_to_insert)} contactos nuevos ...")
                    Logger.add_to_log('debug', f"Primeros 3 apoyos: {apoyos_to_insert[:3]}")
                    # Llamada de al servicio de insercion
                    ApoyosService.bulk_insert(apoyos_to_insert)
                    Logger.add_to_log('info', f"✅ 💾 {len(apoyos_to_insert)} apoyos insertados exitosamente")

                except Exception as e:
                    Logger.add_to_log("error", "❌ 💾 ERROR AL INSERTAR APOYOS")
                    Logger.add_to_log("error", f"Detalles: {str(e)}")
                    Logger.add_to_log("error", traceback.format_exc())
                    
                    return jsonify({
                        'success':False,
                        'message':'Error al insertar apoyos',
                        'data':{
                            'fase_fallida':'Insercion de apoyos',
                            'beneficiarios_insertados': len(beneficiarios_to_insert),
                            'contactos_intentados': len(contactos_to_insert),
                            'apoyos_intentados': len(apoyos_to_insert),
                            'warning':'Los beneficiarios quedaron en BD sin contactos asociados'
                        },
                        'error':str(e)
                    }), 500   
            else:
                Logger.add_to_log("warn", "✅ 💾 No hay contactos para insertar")       
            
            
            
            
        except Exception as ex:
            
            return jsonify({
            'success': False,
            'message': 'Error crítico en el proceso de carga masiva',
            'data': None,
            'error': {
                'type': type(ex).__name__,
                'message': str(ex),
                'traceback': traceback.format_exc()
            }
        }), 500
        
       
    @staticmethod
    def generate_template(catalogos):
        wb = Workbook()

        # Hoja principal
        ws = wb.active
        ws.title = "Beneficiarios"

        # Hoja oculta con catálogos
        ws_cat = wb.create_sheet("Catalogos")

        # Insertar catálogos en hoja oculta
        col = 1
        ranges = {}
        for key, values in catalogos.items():
            ws_cat.cell(1, col, key)
            for i, v in enumerate(values, start=2):
                ws_cat.cell(i, col, v["nombre"])
            # Guardamos rango para validación
            end_row = len(values) + 1
            ranges[key] = f"Catalogos!${chr(64+col)}$2:${chr(64+col)}${end_row}"
            col += 1

        # Definir columnas visibles en hoja Beneficiarios
        headers = []
        for key in catalogos.keys():
            headers.append(key)
            headers.append(f"{key}_ID")

        ws.append(headers)

        # Agregar validaciones
        col_num = 1
        for key in catalogos.keys():
            dv = DataValidation(type="list", formula1=ranges[key], allow_blank=True)
            ws.add_data_validation(dv)
            dv.add(f"{chr(64+col_num)}2:{chr(64+col_num)}1048576")  # toda la columna
            col_num += 2  # saltamos el campo ID

        # Ocultar IDs si la variable está en false
        show_ids = os.getenv("SHOW_IDS", "false").lower() == "true"
        if not show_ids:
            for idx, key in enumerate(catalogos.keys()):
                col_to_hide = (idx * 2) + 2
                ws.column_dimensions[chr(64+col_to_hide)].hidden = True

        # Ocultamos hoja de catálogos
        ws_cat.sheet_state = "hidden"

        return wb