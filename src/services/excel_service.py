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

import re
from datetime import datetime

import os
from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.workbook.defined_name import DefinedName

class ExcelService:  
    
    @staticmethod
    def process_file(file, id_user, id_dependencia_user):
        try:
            # ==== INICIO ====
            Logger.add_to_log("info", "== INICIO DE CARGA MASIVA ==")
            Logger.add_to_log("info", f"Usuario: {id_user}")

            # Lectura del archivo
            data = pl.read_excel(
                io.BytesIO(file.read()),
                schema_overrides=Config.CELLS_DATA_TYPES,
                infer_schema_length=10000
            )

            # Eliminar filas vacías
            data = data.filter(
                pl.any_horizontal(
                    pl.when(pl.col(c).is_not_null() & (pl.col(c).cast(pl.Utf8).str.strip_chars() != ""))
                    .then(True)
                    .otherwise(False)
                    for c in data.columns
                )
            )

            # Normalización de fechas y campos nulos
            data = data.with_columns(
                pl.col("Fecha de Nacimiento").is_null().alias("fecha_nac_vacia_original"),
                pl.col("Fecha de Nacimiento")
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
                .dt.strftime("%d/%m/%Y")
                .alias("Fecha de Nacimiento")
            )

            data = data.with_columns([
                pl.col("Estado Civil").is_null().alias("estado_civil_vacio_original"),
                pl.col("Sexo").is_null().alias("sexo_vacio_original"),
            ])

            rows = data.to_dicts()
            Logger.add_to_log("info", f"Archivo leído correctamente con {len(rows)} filas y {len(data.columns)} columnas")

            # ==== CARGA DE CATÁLOGOS ====
            sexos_map = SearchService.get_sexo_map()
            estados_map = SearchService.get_estado_map()
            municipios_map = SearchService.get_municipio_map()
            colonias_map = SearchService.get_colonia_map()
            estados_civiles_map = SearchService.get_estado_civil_map()
            dependencias_map = SearchService.get_dependencias_map()
            programas_map = SearchService.get_programas_map()
            subprograma_map = SearchService.get_subprogramas_map()
            componentes_map = SearchService.get_componentes_map()
            acciones_map = SearchService.get_acciones_map()
            tipos_beneficiarios_map = SearchService.get_tipos_beneficiarios_map()
            beneficiario_map = SearchService.get_beneficiarios_map()
            carpetas_beneficiarios_map = SearchService.get_carpeta_beneficiarios_map()
            Logger.add_to_log("info", "Esto es la carpeta Beneficiarios")
            Logger.add_to_log("info", carpetas_beneficiarios_map)

            Logger.add_to_log("info", "Catálogos cargados correctamente")

            # ==== VARIABLES BASE ====
            beneficiarios_to_insert = []
            relaciones = []
            rows_errors = []
            cache_beneficiarios_excel = {}
            stats = {
                'total_filas': len(rows),
                'beneficiarios_nuevos': 0,
                'beneficiarios_existentes_db': 0,
                'duplicados_en_excel': 0,
                'errores_validacion': 0
            }

            # ==== PROCESAMIENTO DE FILAS ====
            Logger.add_to_log("info", "Procesando filas del Excel...")
            for idx, row in enumerate(rows):
                try:
                    curp = row.get('Curp') or None
                    rfc = row.get('RFC') or None

                    if curp:
                        curp = curp.strip()
                    if rfc:
                        rfc = rfc.strip()

                    id_sexo = sexos_map.get(row.get('Sexo'))
                    calle = row.get('Calle')
                    numero = row.get('Numero')
                    id_estado = estados_map.get(row.get('Estado (catálogo)'))
                    id_municipio = municipios_map.get(row.get('Municipio Dirección (catálogo)').rstrip() if row.get('Municipio Dirección (catálogo)') else None)
                    id_colonia = colonias_map.get(row.get('Colonia').rstrip() if row.get('Colonia') else None)
                    id_estado_civil = estados_civiles_map.get(row.get('Estado Civil'))
                    telefono = row.get('Telefono')
                    telefono_2 = row.get('Telefono 2')
                    correo = row.get('Correo')
                    monto = row.get('Monto')
                    id_dependencia = dependencias_map.get(row.get('Dependencia'))
                    id_dependencia = dependencias_map.get(row.get('Dependencia').rstrip())
                    
                    if id_dependencia != id_dependencia_user:
                        Logger.add_to_log("warn", "No puedes cargar archivos de esa dependencia")
                        return jsonify({
                            'success': False,
                            'message': 'No tienes permisos para cargar archivos de esta dependencia',
                            'data': {'errores_detalle': 'tissss'},
                            'error': 'Sin datos válidos',
                            'error_dependencia': True 
                        }), 400
                
                  
                    id_programa = programas_map.get((row.get('Programa'), id_dependencia)) if id_dependencia else None
                    id_subprograma = subprograma_map.get((row.get('Subprograma'), id_programa) if id_programa else None)
                    id_componente = componentes_map.get((row.get('Componente'), id_subprograma)) if id_subprograma else None
                    id_acciones = acciones_map.get(row.get('Accion'))
                    id_tipo_beneficiario = tipos_beneficiarios_map.get(row.get('Tipo de Beneficio'))

                    fecha_plantilla = row.get('Fecha de Registro')
                    fecha_nacimiento = None
                    if not fecha_nacimiento:
                        row['Fecha de Nacimiento'] = datetime.strptime(
                            row['Fecha de Nacimiento'],
                            '%d/%m/%Y'
                        ).strftime('%Y-%m-%d')

                    fecha_nacimiento = row['Fecha de Nacimiento']

                    validacion_errores = {}
                    msg_error = ""
                    fecha = None

                    # ==== VALIDACIONES ====
                    if not fecha_plantilla:
                        validacion_errores['Fecha de Registro'] = 'Celda vacía'
                    else:
                        if isinstance(fecha_plantilla, str):
                            fecha_str = fecha_plantilla
                            fecha = datetime.strptime(fecha_str, "%d/%m/%Y")
                        else:
                            fecha = fecha_plantilla

                    if fecha:
                        mes = fecha.month
                        anio = fecha.year
                        Logger.add_to_log("info", f"Mes:{mes} Anio:{anio}")
                        id_carpeta_beneficiario = carpetas_beneficiarios_map.get((mes, anio))
                        Logger.add_to_log("info", f"Id Carpeta Beneficiario {id_carpeta_beneficiario}")
                    else:
                        validacion_errores['Fecha de Registro'] = 'Error en formato'

                    if not id_carpeta_beneficiario:
                        validacion_errores['Carpeta de Beneficiarios'] = f"No existe carpeta para {mes}/{anio}"

                    if (len(curp or '') > 18 or len(curp or '') < 18) and curp is not None:
                        validacion_errores['Curp'] = row.get('Curp')
                        msg_error = "Curp inválida. Debe tener 18 caracteres."

                    if not fecha_nacimiento:
                        if not row["fecha_nac_vacia_original"]:
                            validacion_errores['Fecha de Nacimiento'] = 'Error en formato'

                    if not id_sexo:
                        if not row["sexo_vacio_original"]:
                            validacion_errores['Sexo'] = row.get('Sexo')

                    if calle is None or calle.strip() == "":
                        validacion_errores['Calle'] = 'Celda vacía'

                    if numero is None or numero.strip() == "":
                        validacion_errores['Número'] = 'Celda vacía'

                    if not id_estado_civil:
                        if not row["estado_civil_vacio_original"]:
                            validacion_errores['Estado Civil'] = row.get('Estado Civil')

                    if not id_estado:
                        validacion_errores['Estado'] = row.get('Estado (catálogo)')

                    if not id_municipio:
                        validacion_errores['Municipio'] = row.get('Municipio Dirección (catálogo)')

                    if not id_colonia:
                        validacion_errores['Colonia'] = row.get('Colonia')

                    if not telefono:
                        validacion_errores['Telefono'] = 'Celda vacía'

                    if not telefono_2:
                        validacion_errores['Telefono 2'] = 'Celda vacía'

                    if not correo:
                        validacion_errores['Correo'] = 'Celda vacía'

                    if not monto:
                        validacion_errores['Monto'] = 'Celda vacía'

                    if not id_tipo_beneficiario:
                        validacion_errores['Tipo de Beneficio'] = row.get('Tipo de Beneficio')

                    if not id_dependencia:
                        validacion_errores['Dependencia'] = row.get('Dependencia')

                    if not id_programa:
                        validacion_errores['Programa'] = row.get('Programa')

                    if not id_subprograma:
                        validacion_errores['Subprograma'] = row.get('Subprograma')

                    if not id_componente:
                        validacion_errores['Componente'] = row.get('Componente')

                    if not id_acciones:
                        validacion_errores['Accion'] = row.get('Accion')

                    if validacion_errores:
                        stats['errores_validacion'] += 1
                        rows_errors.append({
                            'row_index': idx + 2,
                            'curp': row.get('Curp'),
                            'error': msg_error or 'Error de validación en campos obligatorios',
                            'campos_invalidos': validacion_errores,
                            'data': row
                        })
                        Logger.add_to_log('warn', f'Fila {idx + 2} rechazada - Faltan: {", ".join(validacion_errores.keys())}')
                        continue

                    # ==== CREACIÓN DE OBJETOS ====
                    id_beneficiario = None
                    es_nuevo = False
                    origen = ""

                    key_beneficiario = (curp, rfc)
                    if key_beneficiario in cache_beneficiarios_excel:
                        id_beneficiario = cache_beneficiarios_excel[key_beneficiario]
                        stats['duplicados_en_excel'] += 1
                        origen = 'cache_excel'
                    else:
                        id_beneficiario = beneficiario_map.get((curp, rfc))
                        if id_beneficiario:
                            origen = 'db'
                            stats['beneficiarios_existentes_db'] += 1

                    if not id_beneficiario:
                        id_beneficiario = str(uuid.uuid4())
                        es_nuevo = True
                        origen = "nuevo"
                        stats['beneficiarios_nuevos'] += 1
                        beneficiarios_to_insert.append({
                            'id': id_beneficiario,
                            'creador': id_user,
                            'modificador': id_user,
                            'idSexo': id_sexo
                        })
                        cache_beneficiarios_excel[(curp, rfc)] = id_beneficiario

                    id_contacto_temp = str(uuid.uuid4())
                    id_apoyo_temp = str(uuid.uuid4())

                    contacto_data = {
                        'id': id_contacto_temp,
                        'creador': id_user,
                        'modificador': id_user,
                        'idEstado': id_estado,
                        'idMunicipio': str(id_municipio[1]) if id_municipio else None,
                        'idColonia': str(id_colonia[0]) if id_colonia else None,
                        'idEstadoCivil': id_estado_civil
                    }

                    apoyo_data = {
                        'id': id_apoyo_temp,
                        'idBeneficiario': id_beneficiario,
                        'idContacto': id_contacto_temp,
                        'creador': id_user,
                        'modificador': id_user,
                        'idDependencia': id_dependencia,
                        'idPrograma': id_programa,
                        'idSubprograma': id_subprograma,
                        'idComponente': id_componente,
                        'idAccion': id_acciones,
                        'idTipoBeneficio': id_tipo_beneficiario,
                        'idCarpetaBeneficiarios': id_carpeta_beneficiario
                    }

                    relaciones.append({
                        'row_index': idx + 2,
                        'id_beneficiario': id_beneficiario,
                        'id_contacto': id_contacto_temp,
                        'id_apoyo': id_apoyo_temp,
                        'es_beneficiario_nuevo': es_nuevo,
                        'origen_beneficiario': origen,
                        'contacto_data': contacto_data,
                        'apoyo_data': apoyo_data
                    })

                except Exception as fila_error:
                    rows_errors.append({'row_index': idx + 2, 'error': str(fila_error)})
                    Logger.add_to_log("error", f"Error en fila {idx + 2}: {fila_error}")

            # ==== ESTADÍSTICAS ====
            Logger.add_to_log("info", f"📊 Total filas: {stats['total_filas']}")
            Logger.add_to_log("info", f"✨ Beneficiarios nuevos: {stats['beneficiarios_nuevos']}")
            Logger.add_to_log("info", f"✓ Existentes en BD: {stats['beneficiarios_existentes_db']}")
            Logger.add_to_log("info", f"♻️ Duplicados en Excel: {stats['duplicados_en_excel']}")
            Logger.add_to_log("info", f"⚠️ Errores validación: {stats['errores_validacion']}")

            if not relaciones:
                Logger.add_to_log("warn", "No se encontraron registros válidos para procesar")
                return jsonify({
                    'success': False,
                    'message': 'No se encontraron registros válidos para procesar',
                    'data': {'errores_detalle': rows_errors},
                    'error': 'Sin datos válidos'
                }), 400

            # ==== INSERCIÓN ====
            Logger.add_to_log("info", "== INICIANDO INSERCIÓN EN BD ==")

            try:
                if beneficiarios_to_insert:
                    Logger.add_to_log("info", f"Insertando {len(beneficiarios_to_insert)} beneficiarios nuevos...")
                    BeneficiariosService.bulk_insert(beneficiarios_to_insert)
                    Logger.add_to_log("info", "Beneficiarios insertados exitosamente")

                contactos_to_insert = [r['contacto_data'] for r in relaciones]
                if contactos_to_insert:
                    Logger.add_to_log("info", f"Insertando {len(contactos_to_insert)} contactos...")
                    ContactosService.bulk_insert(contactos_to_insert)
                    Logger.add_to_log("info", "Contactos insertados exitosamente")

                apoyos_to_insert = [r['apoyo_data'] for r in relaciones]
                if apoyos_to_insert:
                    Logger.add_to_log("info", f"Insertando {len(apoyos_to_insert)} apoyos...")
                    ApoyosService.bulk_insert(apoyos_to_insert)
                    Logger.add_to_log("info", "Apoyos insertados exitosamente")

            except Exception as e:
                Logger.add_to_log("error", f"Error en inserción: {e}")
                return jsonify({'success': False, 'message': 'Error en inserción', 'error': str(e)}), 500

            Logger.add_to_log("info", "== PROCESO COMPLETADO CON ÉXITO ==")

        except Exception as ex:
            Logger.add_to_log("error", f"Error crítico en carga masiva: {ex}")
            return jsonify({
                'success': False,
                'message': 'Error crítico en el proceso de carga masiva',
                'error': {
                    'type': type(ex).__name__,
                    'message': str(ex),
                    'traceback': traceback.format_exc()
                }
            }), 500


    
    
    @staticmethod
    def generate_template(catalogos):
        wb = Workbook()
        ws = wb.active
        ws.title = "Beneficiarios"

        # ---------- Hoja oculta con catálogos ----------
        ws_cat = wb.create_sheet("Catalogos")
        col = 1

        dv_names = {}       # "Programa" -> "=CAT_Programa"
        lookup_ranges = {}  # "Programa" -> (names_raw, ids_raw, table_raw)

        def make_defined_name(key: str) -> str:
            base = re.sub(r'[^A-Za-z0-9_]', '_', key)
            if not re.match(r'^[A-Za-z_]', base):
                base = f"CAT_{base}"
            return f"CAT_{base}"

        # Crear DefinedNames y llenar hoja "Catalogos"
        for key, values in catalogos.items():
            values = values or []

            name_col = get_column_letter(col)
            id_col = get_column_letter(col + 1)

            ws_cat.cell(1, col, key)
            ws_cat.cell(1, col + 1, f"{key}_ID")

            for i, v in enumerate(values, start=2):
                ws_cat.cell(i, col, v.get("nombre", ""))
                ws_cat.cell(i, col + 1, v.get("id", ""))

            end_row = max(2, len(values) + 1)
            names_raw = f"Catalogos!${name_col}$2:${name_col}${end_row}"
            ids_raw = f"Catalogos!${id_col}$2:${id_col}${end_row}"
            table_raw = f"Catalogos!${name_col}$2:${id_col}${end_row}"
            lookup_ranges[key] = (names_raw, ids_raw, table_raw)

            # Crear rango con nombre (DefinedName)
            defined = make_defined_name(key)
            ref_text = names_raw
            wb.defined_names.add(DefinedName(name=defined, attr_text=ref_text))

            # Asignar fórmula de validación
            dv_names[key] = f"={defined}"
            col += 2

        # ---------- Encabezados ----------
        headers = [
            "Curp", "Nombre", "Apellido paterno", "Apellido Materno",
            "Fecha de Nacimiento", "Estado (catálogo)", "Estado Civil", "Sexo",
            "Calle", "Numero", "Colonia", "Municipio Dirección (catálogo)",
            "Telefono", "Telefono 2", "Correo",
            "Programa", "Subprograma", "Componente", "Accion",
            "Fecha de Registro", "Monto", "Tipo de Beneficio",
            "RFC", "Regimen Capital", "Actividad", "Nombre Comercial",
            "Razón Social", "Localidad", "Dependencia"
        ]
        ws.append(headers)

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

        # ---------- Campos con listas ----------
        catalog_fields = {
            "Estado (catálogo)": "Estado",
            "Municipio Dirección (catálogo)": "Municipio",
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

        # ---------- DataValidation ----------
        MAX_ROWS = 10000
        for idx, h in enumerate(headers, start=1):
            if h in catalog_fields:
                key = catalog_fields[h]
                if key in dv_names:
                    col_letter = get_column_letter(idx)
                    dv = DataValidation(type="list", formula1=dv_names[key], allow_blank=True)
                    ws.add_data_validation(dv)
                    dv.add(f"{col_letter}2:{col_letter}{MAX_ROWS + 1}")

        # ---------- Columnas de ID ----------
        id_headers = [f"{catalog_fields[h]}_ID" for h in headers if h in catalog_fields]
        start_col = len(headers) + 1
        for i, idh in enumerate(id_headers):
            ws.cell(1, start_col + i, idh)
            cell = ws.cell(1, start_col + i)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = Border(top=border_style, bottom=border_style, left=border_style, right=border_style)
            ws.column_dimensions[get_column_letter(start_col + i)].width = 18

        # ---------- Fórmulas automáticas para IDs ----------
        for h in headers:
            if h in catalog_fields:
                cat_key = catalog_fields[h]
                if cat_key in lookup_ranges:
                    names_raw, ids_raw, table_raw = lookup_ranges[cat_key]
                    name_col_idx = headers.index(h) + 1
                    name_col_letter = get_column_letter(name_col_idx)
                    id_header = f"{cat_key}_ID"
                    id_col_idx = start_col + id_headers.index(id_header)
                    id_col_letter = get_column_letter(id_col_idx)

                    base_formula = (
                        f'IFERROR('
                        f'XLOOKUP({name_col_letter}{{ROW}}, {names_raw}, {ids_raw}, ""),'
                        f'IFERROR(VLOOKUP({name_col_letter}{{ROW}}, {table_raw}, 2, FALSE), "")'
                        f')'
                    )
                    for r in range(2, MAX_ROWS + 2):
                        ws[f"{id_col_letter}{r}"] = f"={base_formula.replace('{ROW}', str(r))}"

        ws_cat.sheet_state = "hidden"

        # ---------- Mostrar u ocultar IDs ----------
        show_ids = os.getenv("SHOW_IDS", "false").lower() == "true"
        if not show_ids:
            for i in range(len(id_headers)):
                ws.column_dimensions[get_column_letter(start_col + i)].hidden = True

        return wb