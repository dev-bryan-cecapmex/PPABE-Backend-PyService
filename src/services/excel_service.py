
from ..database.connection      import db
from flask                      import jsonify
from config                     import Config

import io
import traceback
import polars as pl
import uuid
import re
import os

from datetime import datetime

# Logger
from ..utils.Logger import Logger

# Mapeos
from ..utils.Mapeo import Mapeo

from ..services.beneficiarios_service   import BeneficiariosService
from ..services.contacto_service        import ContactosService
from ..services.apoyo_service           import ApoyosService
from ..services.apoyo_integral_service  import ApoyoIntegralService
from ..services.historial_carga_service import HistoriaCargaService

# OJO: SearchService lo seguimos usando SOLO para catálogos.
#      Ya NO se usa para buscar beneficiarios existentes.
from ..services.search_service import SearchService

# Cache por ejecución (job/session)
from ..services.cache_service import cache_service


from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

import uuid
import re
import unicodedata

class ExcelService:
    
    @staticmethod
    def generate_sequential_uuid():
        """
        Generates a sequential UUID (UUIDv1-like structure) suitable for
        MySQL InnoDB Primary Keys to prevent B-Tree fragmentation.
        Format: 8-4-4-4-12 string.
        """
        import time
        import os
        import binascii
        
        # 1. Get current time in milliseconds
        now_ms = int(time.time() * 1000)
        
        # 2. Convert to hex (will be ~11-12 chars for modern dates), pad to 12
        time_hex = f"{now_ms:012x}"[-12:] # Ensure strictly 12 characters
        
        # 3. Generate 24 characters (12 bytes) of random entropy
        random_hex = binascii.b2a_hex(os.urandom(12)).decode('ascii')
        
        # 4. Construct the UUID format: exactly 36 chars (8-4-4-4-12)
        seq_uuid = f"{time_hex[:8]}-{time_hex[8:12]}-{random_hex[:4]}-{random_hex[4:8]}-{random_hex[8:20]}"
        return seq_uuid

    @staticmethod      
    def limpiar_texto(value):
        if value is None:
            return ""
        
        texto = str(value)

        # Quitar tabulaciones y saltos de línea
        texto = texto.replace("\t", " ").replace("\n", " ").replace("\r", " ")

        # Normalizar espacios múltiples
        texto = re.sub(r"\s+", " ", texto)

        # Quitar espacios al inicio y final
        texto = texto.strip()

        return texto


    def limpiar_curp(value):
        if not value:
            return ""

        texto = str(value)

        # Normalizar unicode (convierte caracteres similares a su equivalente ASCII)
        texto = unicodedata.normalize("NFKD", texto)
        
        # Codificar a ASCII ignorando lo que no sea ASCII y decodificar
        texto = texto.encode("ascii", errors="ignore").decode("ascii")

        # Solo A-Z y 0-9
        texto = re.sub(r"[^A-Z0-9]", "", texto.upper())

        return texto

    @staticmethod
    def process_file(file, id_user, id_dependencia_user):
        # ✅ Crear job cache por ejecución (snapshot/local cache)
        job = cache_service.create_job_cache()
        try:
            Logger.add_to_log("info", "=" * 30)
            Logger.add_to_log("info", f"INICIO DE CARGA MASIVA")
            Logger.add_to_log("info", "=" * 30)

            Logger.add_to_log("info", f"Id User: {id_user}")
            Logger.add_to_log("info", f"Dependencia:{id_dependencia_user}")
            
            id_historia_carga = ExcelService.generate_sequential_uuid()
            HistoriaCargaService.insertCarga(id_historia_carga, id_user, id_dependencia_user)
            Logger.add_to_log("info", f"Id de Carga: {id_historia_carga}")

            # 1. Leer el Excel SIN schema_overrides
            file_bytes = file.read()

            data_preview = pl.read_excel(
                io.BytesIO(file_bytes),
                infer_schema_length=5000
            )

            columnas_excel = set(data_preview.columns)
            Logger.add_to_log("info", f"Columnas detectadas en el Excel: {columnas_excel}")

            # 2. Validar encabezados obligatorios
            faltantes = [c for c in Config.CAMPOS_OBLIGATORIOS if c not in columnas_excel]

            if faltantes:
                Logger.add_to_log("error", f"Faltan columnas obligatorias: {faltantes}")
                return jsonify({
                    "success": False,
                    "message": "Faltan columnas en el encabezado del archivo",
                    "data": {"faltantes": faltantes},
                    "error": "ENCABEZADO_INCOMPLETO"
                }), 400

            # 3. Filtrar schema_overrides solo a columnas EXISTENTES
            schema_filtrado = {col: dtype for col, dtype in Config.CELLS_DATA_TYPES.items() if col in columnas_excel}
            Logger.add_to_log("info", f"Schema aplicado a Polars: {schema_filtrado}")

            # 4. Ahora sí leer el Excel con schema_overrides SEGURO
            data = pl.read_excel(
                io.BytesIO(file_bytes),
                schema_overrides=schema_filtrado,
                infer_schema_length=10000
            )

            # Normalización fechas/flags
            data = data.with_columns(
                pl.col("Fecha de Nacimiento").is_null().alias("fecha_nac_vacia_original"),
                pl.coalesce(
                    pl.col("Fecha de Nacimiento").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
                    pl.col("Fecha de Nacimiento").str.strptime(pl.Datetime, "%Y-%m-%d", strict=False),
                    pl.col("Fecha de Nacimiento").str.strptime(pl.Datetime, "%d/%m/%Y", strict=False),
                    pl.col("Fecha de Nacimiento").str.strptime(pl.Datetime, "%d-%m-%Y", strict=False),
                ).dt.strftime("%d/%m/%Y").alias("Fecha de Nacimiento")
            )

            data = data.with_columns([
                pl.col("Estado Civil").is_null().alias("estado_civil_vacio_original"),
                pl.col("Sexo").is_null().alias("sexo_vacio_original"),
            ])

            # eliminar filas vacías
            # data = data.filter(
            #     pl.any_horizontal(
            #         pl.when(pl.col(c).is_not_null() & (pl.col(c).cast(pl.Utf8).str.strip_chars() != ""))
            #         .then(True)
            #         .otherwise(False)
            #         for c in data.columns
            #     )
            # )
            
            COLS_CLAVE = [
                "Curp", "RFC", "Nombre", "Apellido Paterno", "Apellido Materno",
                "Fecha de Nacimiento", "Estado", "Estado Civil", "Sexo",
                "Calle", "Numero", "Colonia", "Municipio Dirección",
                "Telefono", "Telefono 2", "Correo",
                "Dependencia", "Programa", "Subprograma", "Componente",
                "Accion", "Fecha de Registro", "Monto", "Tipo de Beneficio",
                "Regimen Capital", "Actividad", "Nombre Comercial", "Razón Social"
            ]
            cols_existentes = [c for c in COLS_CLAVE if c in data.columns]

            data = data.filter(
                pl.any_horizontal(
                    pl.col(c).is_not_null() & (pl.col(c).cast(pl.Utf8).str.strip_chars() != "")
                    for c in cols_existentes
                )
            )
            
            rows = data.to_dicts()

            Logger.add_to_log("info", "Columnas de los datos")
            Logger.add_to_log("info", data.columns)
            Logger.add_to_log("info", f"Total de filas del Excel: {len(rows)}")

            Logger.add_to_log("info", f"Inicio de estrucutura de los datos .....")
            
            # Apoyo Integral
            apoyosIntegral_to_insert = []

            # Listado para insertar en BD
            beneficiarios_to_insert = []
            relaciones = []
            rows_errors = []

            Logger.add_to_log("info", f"Inicio de estrucutura correctamente")
            Logger.add_to_log("info", "Extrayendo grupos de columnas")

            # Agrupamientos
            group_one_df  = data.select(Config.GROUP_ONE_KEYS).to_dict()
            group_two_df  = data.select(Config.GROUP_TWO_KEYS).to_dict()
            group_tree_df = data.select(Config.GROUP_TREE_KEYS).to_dict()

            # ==========================================================
            # ✅ CATÁLOGOS POR JOB (CACHE LOCAL DE ESTA EJECUCIÓN)
            #   - Se cargan una vez y se guardan en job.local
            #   - Si alguien refresca/limpia cache global, NO afecta esta corrida
            # ==========================================================
            Logger.add_to_log("info", "🚀 Cargando catálogos (JOB cache)...")
            catalog_start_time = datetime.now()

            maps = job.local.get("catalog_maps")
            if maps is None:
                maps = {}
                job.local["catalog_maps"] = maps

            if not maps:
                # Grupo 1 - Beneficiarios
                maps["sexos_map"] = SearchService.get_sexo_map()

                # Grupo 2 - Contactos
                maps["estados_map"] = SearchService.get_estado_map()
                maps["municipios_map"] = SearchService.get_municipio_map()
                maps["colonias_map"] = SearchService.get_colonia_map()
                maps["estados_civiles_map"] = SearchService.get_estado_civil_map()

                # Grupo 3 - Apoyos
                maps["dependencias_map"] = SearchService.get_dependencias_map()
                maps["programas_map"] = SearchService.get_programas_map()
                maps["subprograma_map"] = SearchService.get_subprogramas_map()
                maps["componentes_map"] = SearchService.get_componentes_map()
                maps["acciones_map"] = SearchService.get_acciones_map()
                maps["tipos_beneficiarios_map"] = SearchService.get_tipos_beneficiarios_map()
                maps["carpetas_beneficiarios_map"] = SearchService.get_carpeta_beneficiarios_map(id_dependencia_user)

            # sacar refs locales
            sexos_map = maps["sexos_map"]
            estados_map = maps["estados_map"]
            municipios_map = maps["municipios_map"]
            colonias_map = maps["colonias_map"]
            estados_civiles_map = maps["estados_civiles_map"]

            dependencias_map = maps["dependencias_map"]
            programas_map = maps["programas_map"]
            subprograma_map = maps["subprograma_map"]
            componentes_map = maps["componentes_map"]
            acciones_map = maps["acciones_map"]
            tipos_beneficiarios_map = maps["tipos_beneficiarios_map"]
            carpetas_beneficiarios_map = maps["carpetas_beneficiarios_map"]

            catalog_elapsed = (datetime.now() - catalog_start_time).total_seconds()
            Logger.add_to_log("info", f"✅ Catálogos cargados en {catalog_elapsed:.2f}s (JOB)")
            Logger.add_to_log("info", f"  ✓ Carpetas Beneficiarios: {len(carpetas_beneficiarios_map)} registros")

            # Estadística
            stats = {
                "total_filas": len(rows),
                "beneficiarios_nuevos": 0,
                "errores_validacion": 0
            }
            
            for idx, row in enumerate(rows):
                curp = row.get("Curp") or None
                rfc  = row.get("RFC") or None

                if curp:
                    curp = curp.strip()
                if rfc:
                    rfc = rfc.strip()

                # ==============================
                # Grupo 1 - Beneficiarios
                # ==============================
                sexo = row.get("Sexo")
                id_sexo = sexos_map.get(sexo.upper().rstrip()) if sexo else None

                # ==============================
                # Grupo 2 - Contacto
                # ==============================
                calle = row.get("Calle")
                numero = row.get("Numero")

                estado = row.get("Estado")
                id_estado = estados_map.get(estado.upper().rstrip()) if estado else None
                

                municipio = row.get("Municipio Dirección")
                raw_value = municipios_map.get(municipio.upper().rstrip()) if municipio else None
                id_municipio = raw_value[0] if isinstance(raw_value, list) else raw_value

                estado_civil = row.get("Estado Civil")
                id_estado_civil = estados_civiles_map.get(estado_civil.upper().rstrip()) if estado_civil else None

                telefono = row.get("Telefono")
                telefono_2 = row.get("Telefono 2")
                correo = row.get("Correo")
                monto = row.get("Monto")

                colonia = row.get("Colonia")
                colonia = colonia.upper().rstrip() if colonia else None
                
                regimen_capital     = ExcelService.limpiar_texto(row.get("Regimen Capital"))
                actividad           = ExcelService.limpiar_texto(row.get("Actividad"))
                nombre_comercial    = ExcelService.limpiar_texto(row.get("Nombre Comercial"))
                razon_social        = ExcelService.limpiar_texto(row.get("Razón Social"))
                
                # ==============================
                # Grupo 3 - Apoyos
                # ==============================
                validacion_errores = {}
                msg_error = {}

                dependencia = row.get("Dependencia")
                id_dependencia = dependencias_map.get(dependencia.upper().rstrip()) if dependencia else None
    

                if id_dependencia != id_dependencia_user:
                    Logger.add_to_log("warn", "No puedes cargar archivos de esa dependencia")
                    validacion_errores["Dependecia"] = row.get("Dependencia")
                    

                programa = row.get("Programa")
                id_programa = programas_map.get((programa.upper().rstrip(), id_dependencia)) if programa and id_dependencia else None

                subprograma = row.get("Subprograma")
                id_subprograma = subprograma_map.get((subprograma.upper().rstrip(), id_programa)) if subprograma and id_programa else None
                
                
                componente = row.get("Componente")
                id_componente = componentes_map.get((componente.upper().rstrip(), id_subprograma)) if componente and id_subprograma else None

                accion = row.get("Accion")
                id_acciones = acciones_map.get(accion.upper().rstrip()) if accion else None

                tipo_beneficio = row.get("Tipo de Beneficio")
                id_tipo_beneficiario = tipos_beneficiarios_map.get(tipo_beneficio.upper().rstrip()) if tipo_beneficio else None

                # FECHA REGISTRO
                fecha_plantilla = row.get("Fecha de Registro")
                fecha_registro_obj = None

                if fecha_plantilla:
                    try:
                        if isinstance(fecha_plantilla, datetime):
                            fecha_registro_obj = fecha_plantilla
                        else:
                            fecha_str = str(fecha_plantilla).strip()
                            if " " in fecha_str:
                                fecha_str = fecha_str.split()[0]

                            formatos_posibles = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"]
                            for formato in formatos_posibles:
                                try:
                                    fecha_registro_obj = datetime.strptime(fecha_str, formato)
                                    break
                                except ValueError:
                                    continue
                    except Exception as e:
                        Logger.add_to_log("error", f"Error procesando Fecha de Registro {fecha_plantilla}: {str(e)}")

                if fecha_registro_obj:
                    row["Fecha de Registro"] = fecha_registro_obj.strftime("%Y-%m-%d")
                else:
                    row["Fecha de Registro"] = None

                # FECHA NACIMIENTO
                fecha_nacimiento_raw = row.get("Fecha de Nacimiento")
                fecha_nacimiento_obj = None

                if fecha_nacimiento_raw:
                    try:
                        if isinstance(fecha_nacimiento_raw, datetime):
                            fecha_nacimiento_obj = fecha_nacimiento_raw
                        else:
                            fecha_str = str(fecha_nacimiento_raw).strip()
                            if " " in fecha_str:
                                fecha_str = fecha_str.split()[0]

                            formatos_posibles = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"]
                            for formato in formatos_posibles:
                                try:
                                    fecha_nacimiento_obj = datetime.strptime(fecha_str, formato)
                                    break
                                except ValueError:
                                    continue
                    except Exception as e:
                        Logger.add_to_log("error", f"Error procesando Fecha de Nacimiento {fecha_nacimiento_raw}: {str(e)}")

                if fecha_nacimiento_obj:
                    row["Fecha de Nacimiento"] = fecha_nacimiento_obj.strftime("%Y-%m-%d")
                    fecha_nacimiento = row["Fecha de Nacimiento"]
                else:
                    row["Fecha de Nacimiento"] = None
                    fecha_nacimiento = None

                # ==============================
                # VALIDACIONES
                # ==============================
                
                fecha = fecha_registro_obj

                if not fecha_plantilla:
                    validacion_errores["Fecha de Registro"] = "Celda vacía"
                elif not fecha:
                    validacion_errores["Fecha de Registro"] = "Error en formato"

                # Carpeta beneficiarios
                id_carpeta_beneficiario = None
                if fecha:
                    mes = fecha.month
                    anio = fecha.year

                    carpeta_info = carpetas_beneficiarios_map.get((mes, anio, id_dependencia_user))
    
                   
                    if not carpeta_info:
                        validacion_errores["Carpeta de Beneficiarios"] = f"No existe carpeta para {mes}/{anio}"
                    else:
                        estado_carpeta = carpeta_info.get("estado")
                        if estado_carpeta == "Publicado":
                            validacion_errores["Carpeta de Beneficiarios"] = (
                                f"La carpeta {mes}/{anio} ya está PUBLICADA y no puede recibir registros."
                            )
                        else:
                            id_carpeta_beneficiario = carpeta_info.get("id")
                
                #if(curp is None and rfc is None) or len(curp.strip()) != 18:
                rfc  = (row.get("RFC") or "").strip().upper()
                curp = (row.get("Curp") or "").strip().upper()

                # Validar que al menos uno exista
                if not rfc and not curp:
                    msg_error["General"] = "Debe proporcionar RFC o CURP."
                    validacion_errores["RFC"] = row.get("RFC")
                    validacion_errores["Curp"] = row.get("Curp")

                # ============================
                # Validar RFC si viene
                # ============================
                if rfc:
                    if len(rfc) not in (12, 13):
                        validacion_errores["RFC"] = rfc
                        msg_error["RFC"] = "RFC inválido. Debe tener 12 (moral) o 13 (física)."

                # ============================
                # Validar CURP si viene
                # ============================
                if curp:
                    if len(curp) != 18:
                        validacion_errores["Curp"] = curp
                        msg_error["Curp"] = "CURP inválida. Debe tener 18 caracteres."
                # if rfc and regimen_capital == "":
                #     validacion_errores["Regimen Capital"] = row.get("Regimen Capital")
                #     msg_error["Regimen Capital"] = "El campo Región Capital es obligatorio para personas físicas y morales."
                  
                # if rfc and actividad == "":
                #     validacion_errores["Actividad"] = row.get("Actividad")
                #     msg_error["Actividad"] = "El campo Actividad es obligatorio para personas físicas y morales."
                    
                # if rfc and nombre_comercial == "":
                #     validacion_errores["Nombre Comercial"] = row.get("Nombre Comercial")
                #     msg_error["Nombre Comercial"] = "El campo Nombre Comercial es obligatorio para personas físicas y morales."
                
                # if rfc and razon_social == "":
                #     validacion_errores["Razón Social"] = row.get("Razón Social")
                #     msg_error["Razón Social"] = "El campo Razón Social es obligatorio para personas físicas y morales."                
                       
                if not fecha_nacimiento:
                    if not row["fecha_nac_vacia_original"]:
                        validacion_errores["Fecha de Nacimiento"] = "Error en formato"
                        msg_error["Fecha de Nacimiento"] = "El formato de fecha no es el correcto"
                if not id_sexo:
                    if not row["sexo_vacio_original"]:
                        validacion_errores["Sexo"] = row.get("Sexo")
                        msg_error["Sexo"] = "Error en sexo ingresado"
                        
                if calle is None or calle.strip() == "":
                    validacion_errores["Calle"] = "Celda vacía"

                if numero is None or numero.strip() == "":
                    validacion_errores["Número"] = "Celda vacía"

                if not id_estado_civil:
                    if not row["estado_civil_vacio_original"]:
                        validacion_errores["Estado Civil"] = row.get("Estado Civil")

                if not id_estado:
                    validacion_errores["Estado"] = row.get("Estado")

                if not id_municipio:
                    validacion_errores["Municipio Dirección"] = row.get("Municipio Dirección")

                if not colonia:
                    validacion_errores["Colonia"] = "Celda vacía"
                if telefono == 0 or telefono == '0':
                    row["Telefono"] = "0000000000"
                if not telefono:
                    validacion_errores["Telefono"] = "Celda vacía"
                
                # elif len(telefono) != 10:
                #     validacion_errores["Telefono"] = "El teléfono principal debe tener 10 dígitos."
                
                if not telefono_2 :
                    row["Telefono 2"] = 1111111111
                # elif  telefono_2 and len(telefono_2) != 10:
                #     validacion_errores["Telefono 2"] = row["Telefono 2"]
                #     msg_error["Telefono 2"] = "Error en el segundo numero telefónico"
               
                    
                if not correo:
                    validacion_errores["Correo"] = "Celda vacía"

                if not monto:
                    validacion_errores["Monto"] = "Celda vacía"

                if not id_tipo_beneficiario:
                    validacion_errores["Tipo de Beneficio"] = row.get("Tipo de Beneficio")

                if not id_dependencia:
                    validacion_errores["Dependecia"] = row.get("Dependencia")

                if not id_programa:
                    validacion_errores["Programa"] = row.get("Programa")

                if not id_subprograma:
                    validacion_errores["Subprograma"] = row.get("Subprograma")

                if not id_componente:
                    validacion_errores["Componente"] = row.get("Componente")

                if not id_acciones:
                    validacion_errores["Accion"] = row.get("Accion")
                
                if validacion_errores:
                    stats["errores_validacion"] += 1
                   
                    for validador in validacion_errores:
                        
                        error_detail = {
                            "row_index": idx + 2,
                            "curp": row.get("Curp"),
                            "nombre_completo": f"{row.get('Nombre', '')} {row.get('Apellido Paterno', '')} {row.get('Apellido Materno', '')}".strip(),
                            "error": msg_error.get(validador, "Error de validación en campos obligatorios"),
                            "campos_invalidos": validador,
                            "valor": validacion_errores[validador],
                            "data": row
                        }
                        
                        
                        rows_errors.append(error_detail)
                    continue
                
                # ==========================================================
                # ✅ NUEVO COMPORTAMIENTO:
                #   SIEMPRE crear beneficiario nuevo (sin buscar en BD, sin cache)
                # ==========================================================
                
                id_beneficiario = ExcelService.generate_sequential_uuid()
                id_contacto_temp = ExcelService.generate_sequential_uuid()
                id_apoyo_temp = ExcelService.generate_sequential_uuid()

                stats["beneficiarios_nuevos"] += 1

                # BENEFICIARIO
                nuevo_beneficiario = {
                    "id": id_beneficiario,
                    "creador": id_user,
                    "modificador": id_user,
                }
                for excel_col in Config.GROUP_ONE_KEYS:
                    db_col = Config.COLUMN_MAP_GROUP_ONE.get(excel_col, excel_col)
                    nuevo_beneficiario[db_col] = row.get(excel_col)

                nuevo_beneficiario["idSexo"] = id_sexo
                nuevo_beneficiario["sexo"] = sexo
                
                nuevo_beneficiario["CURP"] = ExcelService.limpiar_curp(curp) if curp else None
                nuevo_beneficiario["RFC"] = rfc if rfc else None
                
                #beneficiarios_to_insert.append(nuevo_beneficiario)
                
                #apoyosIntegral_to_insert.append(nuevo_beneficiario)

                # CONTACTO
                contacto_data = {
                    "id": id_contacto_temp,
                    "creador": id_user,
                    "modificador": id_user,
                }
                for excel_col in Config.GROUP_TWO_KEYS:
                    if excel_col in Config.COLUMN_MAP_GROUP_TWO:
                        db_col = Config.COLUMN_MAP_GROUP_TWO[excel_col]
                        contacto_data[db_col] = row.get(excel_col)

                contacto_data["idEstado"] = id_estado
                if id_estado:
                    contacto_data["estado"] = estado
                else:
                    contacto_data["estado"] = ""
                    
                contacto_data["idMunicipio"] = str(id_municipio) if id_municipio else None
                if id_municipio:
                    contacto_data["municipio"] = municipio
                else:
                    contacto_data["municipio"] = ""
                    
                contacto_data["colonia"] = colonia
                contacto_data["idEstadoCivil"] = id_estado_civil
                if id_estado_civil:
                    contacto_data["estadoCivil"] = estado_civil
                else:
                    contacto_data["estadoCivil"] = ""
                    
                # APOYO
                apoyo_data = {
                    "id": id_apoyo_temp,
                    "idBeneficiario": id_beneficiario,
                    "idContacto": id_contacto_temp,
                    "creador": id_user,
                    "modificador": id_user,
                }
                for excel_col in Config.GROUP_TREE_KEYS:
                    db_col = Config.COLUMN_MAP_GROUP_TREE.get(excel_col, excel_col)
                    apoyo_data[db_col] = row.get(excel_col)

                apoyo_data["idDependencia"] = id_dependencia
                if id_dependencia:
                    apoyo_data["dependencia"] = dependencia
                else:
                    apoyo_data["dependencia"] = ""
                    
                apoyo_data["idPrograma"] = id_programa
                if id_programa:
                    apoyo_data["programa"] = programa
                else:
                    apoyo_data["programa"] = ""
                    
                apoyo_data["idSubprograma"] = id_subprograma
                if id_subprograma:
                    apoyo_data["subprograma"] = subprograma
                else:
                    apoyo_data["componente"] = ""
                    
                apoyo_data["idComponente"] = id_componente
                if id_componente:
                    apoyo_data["componente"] = componente
                else:
                    apoyo_data["componente"] = ""
                    
                apoyo_data["idAccion"] = id_acciones
                if id_acciones :
                    apoyo_data["accion"] = accion
                else:
                    apoyo_data["accion"] = ""
                    
                apoyo_data["idTipoBeneficio"] = id_tipo_beneficiario
                if id_tipo_beneficiario:
                    apoyo_data["tipoBeneficio"] = tipo_beneficio
                else:
                    contacto_data["tipoBeneficio"] = ""
                    
                apoyo_data["idCarpetaBeneficiarios"] = id_carpeta_beneficiario
                apoyo_data["idHistorialCarga"] = id_historia_carga

                relaciones.append({
                    "row_index": idx + 2,
                    "id_beneficiario": id_beneficiario,
                    "id_contacto": id_contacto_temp,
                    "id_apoyo": id_apoyo_temp,
                    "es_beneficiario_nuevo": True,
                    "origen_beneficiario": "nuevo",
                    "beneficiario_data": nuevo_beneficiario,
                    "contacto_data": contacto_data,
                    "apoyo_data": apoyo_data,
                    "curp": curp,
                    "rfc": rfc,
                    "nombre_completo": f"{row.get('Nombre',' ')} {row.get('Apellido Paterno','')} {row.get('Apellido Materno','')}".strip()
                })

            # Estadística
            Logger.add_to_log("info", "")
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", "📊 ESTADÍSTICAS DE FASE 1:")
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", f"  Total de filas procesadas: {stats['total_filas']}")
            Logger.add_to_log("info", f"  ✨ Beneficiarios NUEVOS: {stats['beneficiarios_nuevos']}")
            Logger.add_to_log("info", f"  ⚠️  Errores de validación: {stats['errores_validacion']}")
            Logger.add_to_log("info", f"  📝 Relaciones válidas creadas: {len(relaciones)}")
            Logger.add_to_log("info", "=" * 60)
            Logger.add_to_log("info", "")

            if rows_errors:
                Logger.add_to_log("error", "REPORTE DE ERRORES DE VALIDACIÓN")
                Logger.add_to_log("error", "-" * 60)

                for error in rows_errors[:10]:
                    Logger.add_to_log("error", f"  Fila {error['row_index']}: {error.get('nombre_completo', 'N/A')}")
                    Logger.add_to_log("error", f"    CURP: {error.get('curp', 'N/A')}")
                    Logger.add_to_log("error", f"    Error: {error['error']}")
                    Logger.add_to_log("error", f"    Campos inválidos: {error['campos_invalidos']}")
                    Logger.add_to_log("error", "")

                if len(rows_errors) > 10:
                    Logger.add_to_log("error", f"  ... y {len(rows_errors) - 10} errores más")

                return jsonify({
                    "success": False,
                    "message": "No se encontraron registros validos para procesar",
                    "data": {
                        "total_filas": stats["total_filas"],
                        "errores": stats["errores_validacion"],
                        "errores_detalle": rows_errors
                    },
                    "error": "Sin datos válidos"
                }), 400

            Logger.add_to_log("info", "INICIANDO INSERCIÓN EN BASE DE DATOS ...")

            if not relaciones:
                Logger.add_to_log("warn", "No hay datos validos para insertar")
                return jsonify({
                    "success": False,
                    "message": "No se encontraron registros validos para procesar",
                    "data": {
                        "total_filas": stats["total_filas"],
                        "errores": stats["errores_validacion"],
                        "errores_detalle": rows_errors
                    },
                    "error": "Sin datos válidos"
                }), 400

           

            # Preparar contactos/apoyos
            Logger.add_to_log("info", "Preparando lista de contactos y apoyos ...")
            contactos_to_insert = []
            apoyos_to_insert = []
            
            for relacion in relaciones:

                registro_integral = {
                    **relacion["beneficiario_data"],
                    **relacion["contacto_data"],
                    **relacion["apoyo_data"]
                }
                
                apoyosIntegral_to_insert.append(registro_integral)

            Logger.add_to_log("info", f"{len(contactos_to_insert)} contactos preparados")
            Logger.add_to_log("info", f"{len(apoyos_to_insert)} apoyos preparados")
            
            
             # Insert beneficiarios
            if apoyosIntegral_to_insert:
                try:
                    Logger.add_to_log("info", f"💾 🗄️ Insertando {len(apoyosIntegral_to_insert)} Apoyos Integrales nuevos ...")
                    ApoyoIntegralService.bulk_insert(apoyosIntegral_to_insert, batch_size=5000, commit_every_batches=1)
                    
                    HistoriaCargaService.updateEstatusCarga(id_historia_carga, estatus="EXITOSA")
                    
                    Logger.add_to_log("debug", f"ID Carga: {id_historia_carga}")
                    Logger.add_to_log("info", f"✅ 💾 {len(apoyosIntegral_to_insert)} beneficiarios insertados exitosamente")
                except Exception as e:
                    Logger.add_to_log("error", "❌ 💾 ERROR AL INSERTAR APOYOS INTEGRAL")
                    Logger.add_to_log("error", f"Detalles: {str(e)}")
                    Logger.add_to_log("error", traceback.format_exc())
                    return jsonify({
                        "success": False,
                        "message": "Error al insertar APOYOS INTEGRAL",
                        "data": {
                            "fase_fallida": "Insercion de Apoyos Integral",
                            "beneficiarios_intentados": len(beneficiarios_to_insert)
                        },
                        "error": str(e)
                    }), 500
            else:
                Logger.add_to_log("info", "✅ 💾 No hay beneficiarios nuevos para insertar")

        

          
            # ✅ OK
            return jsonify({
                "success": True,
                "message": "Carga masiva finalizada",
                "data": {
                    "total_filas": stats["total_filas"],
                    "beneficiarios_nuevos": stats["beneficiarios_nuevos"],
                    "errores_validacion": stats["errores_validacion"],
                    "relaciones": len(relaciones)
                },
                "error": None
            }), 200

        except Exception as ex:
            return jsonify({
                "success": False,
                "message": "Error crítico en el proceso de carga masiva",
                "data": None,
                "error": {
                    "type": type(ex).__name__,
                    "message": str(ex),
                    "traceback": traceback.format_exc()
                }
            }), 500

        finally:
            # ✅ SIEMPRE liberar el job cache (aunque haya return o error)
            cache_service.delete_job_cache(job.job_id)
            

    @staticmethod
    def generate_template(catalogos):
        wb = Workbook()
        ws = wb.active
        ws.title = "Beneficiarios"

        # ---------- Hoja oculta con catálogos ----------
        ws_cat = wb.create_sheet("Catalogos")
        col = 1

        # Rango para DataValidation (con "=") y rangos crudos para fórmulas
        dv_ranges = {}       # p.ej. "=Catalogos!$A$2:$A$100"
        lookup_ranges = {}   # p.ej. ("Catalogos!$A$2:$A$100","Catalogos!$B$2:$B$100","Catalogos!$A$2:$B$100")

        for key, values in catalogos.items():
            if not values:
                continue

            # Columna de nombres
            name_col_letter = get_column_letter(col)
            # Columna de IDs (adyacente)
            id_col_letter = get_column_letter(col + 1)

            ws_cat.cell(1, col, key)
            ws_cat.cell(1, col + 1, f"{key}_ID")

            for i, v in enumerate(values, start=2):
                ws_cat.cell(i, col, v["nombre"])
                ws_cat.cell(i, col + 1, v["id"])

            end_row = len(values) + 1

            # Para DV (debe llevar "=")
            dv_ranges[key] = f"=Catalogos!${name_col_letter}$2:${name_col_letter}${end_row}"

            # Para fórmulas (sin "=")
            names_raw = f"Catalogos!${name_col_letter}$2:${name_col_letter}${end_row}"
            ids_raw   = f"Catalogos!${id_col_letter}$2:${id_col_letter}${end_row}"
            table_raw = f"Catalogos!${name_col_letter}$2:${id_col_letter}${end_row}"
            lookup_ranges[key] = (names_raw, ids_raw, table_raw)

            col += 2

        # ---------- Encabezados visibles (una sola fila) ----------
        headers = [
            "Curp", "Nombre", "Apellido Paterno", "Apellido Materno",
            "Fecha de Nacimiento", "Estado", "Estado Civil", "Sexo",
            "Calle", "Numero", "Colonia", "Municipio Dirección",
            "Telefono", "Telefono 2", "Correo", "Dependencia","Programa", "Subprograma", "Componente",
            "Accion", "Fecha de Registro", "Monto", "Tipo de Beneficio",
            "RFC", "Regimen Capital", "Actividad", "Nombre Comercial",
            "Razón Social"
        ]
        ws.append(headers)

        # Formato de cabecera
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        border_style = Side(style="thin", color="000000")
        for c in range(1, len(headers) + 1):
            cell = ws.cell(1, c)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = Border(top=border_style, bottom=border_style, left=border_style, right=border_style)
            ws.column_dimensions[get_column_letter(c)].width = 18
        ws.freeze_panes = "A2"

        # Campos que llevan listas (catálogos)
        catalog_fields = {
            "Estado": "Estado",
            "Municipio Dirección": "Municipio",
            "Sexo": "Sexo",
            "Estado Civil": "EstadoCivil",
            "Programa": "Programa",
            "Subprograma": "Subprograma",
            "Componente": "Componente",
            "Accion": "Accion",
            "Tipo de Beneficio": "TipoBeneficio",
            "Dependencia": "Dependencia",
            "Colonia": "Colonia"
        }

        # ---------- Data Validation (listas) ----------
        MAX_ROWS = 10000
        for idx, h in enumerate(headers, start=1):
            if h in catalog_fields:
                key = catalog_fields[h]
                if key in dv_ranges:
                    col_letter = get_column_letter(idx)
                    dv = DataValidation(type="list", formula1=dv_ranges[key], allow_blank=True)
                    ws.add_data_validation(dv)
                    dv.add(f"{col_letter}2:{col_letter}{MAX_ROWS + 1}")

        # ---------- Agregar columnas ID (al final) ----------
        id_headers = [f"{catalog_fields[h]}_ID" for h in headers if h in catalog_fields]
        start_id_col = len(headers) + 1
        for i, idh in enumerate(id_headers):
            ws.cell(1, start_id_col + i, idh)
            cell = ws.cell(1, start_id_col + i)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = Border(top=border_style, bottom=border_style, left=border_style, right=border_style)
            ws.column_dimensions[get_column_letter(start_id_col + i)].width = 18

        # ---------- Fórmulas automáticas para _ID (XLOOKUP + fallback VLOOKUP) ----------
        # Para cada campo con catálogo, ubicamos columna de nombre y columna de ID
        for h in headers:
            if h in catalog_fields:
                cat_key = catalog_fields[h]
                if cat_key in lookup_ranges:
                    names_raw, ids_raw, table_raw = lookup_ranges[cat_key]
                    # Columna del valor seleccionado (catálogo)
                    name_col_idx = headers.index(h) + 1
                    name_col_letter = get_column_letter(name_col_idx)
                    # Columna del ID destino
                    id_header = f"{cat_key}_ID"
                    id_col_idx = start_id_col + id_headers.index(id_header)
                    id_col_letter = get_column_letter(id_col_idx)

                    # Fórmula: intenta XLOOKUP; si no existe, usa VLOOKUP con tabla de 2 columnas contiguas
                    base_formula = (
                        f'IFERROR('
                        f'XLOOKUP({name_col_letter}{{ROW}}, {names_raw}, {ids_raw}, ""),'
                        f'IFERROR(VLOOKUP({name_col_letter}{{ROW}}, {table_raw}, 2, FALSE), "")'
                        f')'
                    )

                    for r in range(2, MAX_ROWS + 2):
                        ws[f"{id_col_letter}{r}"] = f"={base_formula.replace('{ROW}', str(r))}"

        # ---------- Ocultar hoja catálogos ----------
        ws_cat.sheet_state = "hidden"

        # Mostrar/ocultar columnas ID según variable
        show_ids = os.getenv("SHOW_IDS", "false").lower() == "true"
        if not show_ids:
            for i in range(len(id_headers)):
                ws.column_dimensions[get_column_letter(start_id_col + i)].hidden = True

        return wb