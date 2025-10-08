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

import os
from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side


class ExcelService:
    
    
    
    @staticmethod
    def process_file(file):
        try:
            
            
            # Lectura de excel desde la memoria 
            data = pl.read_excel(io.BytesIO(file.read()))
            Logger.add_to_log("info",data)
            rows = data.to_dicts()
            
            # Datos Agrupados del Excel
            group_one_df    = data.select(Config.GROUP_ONE_KEYS).to_dict()
            Logger.add_to_log("info",group_one_df);
            group_two_df    = data.select(Config.GROUP_TWO_KEYS).to_dict()
            Logger.add_to_log("info",group_two_df);
            group_tree_df   = data.select(Config.GROUP_TREE_KEYS).to_dict()
            Logger.add_to_log("info",group_tree_df);
            
            # Grupo uno
            sexos_map               = SearchService.get_sexo_map()
            
            # Grupo dos
            estados_map             = SearchService.get_estado_map()
            municipios_map          = SearchService.get_municipio_map()
            colonias_map            = SearchService.get_colonia_map()
            estados_civiles_map     = SearchService.get_estado_civil_map()
            
            # Grupo tres
            dependencias_map            = SearchService.get_dependencias_map()
            programas_map               = SearchService.get_programas_map()
            componentes_map             = SearchService.get_componentes_map()
            beneficiario_map            = SearchService.get_beneficiarios_map()
            acciones_map                = SearchService.get_acciones_map()       
            tipos_beneficiarios_map     = SearchService.get_tipos_beneficiarios_map()
            
            carpetas_beneficiarios_map  = SearchService.get_carpeta_beneficiarios_map()
     
            Logger.add_to_log("info", f"Carpetas Beneficiarios\n {carpetas_beneficiarios_map}")
            
            #Lista de los nuevos registros
            new_beneficiarios   = []
            new_contacto        = []
            new_apoyos          = []
            
            rows_errors     = []
            rows_goods      = []
            
            for row in rows:
                # Busqueda
                id_sexo         = sexos_map.get(row['Sexo']) 
                
                id_estado       = estados_map.get(row['Estado (catálogo)'])
                id_municipio    = municipios_map.get(row['Municipio Dirección (catálogo)'])
                id_colonia      = colonias_map.get(row['Colonia'])
                id_estado_civil = estados_civiles_map.get(row['Estado Civil'])
                
                id_dependencia          = dependencias_map.get(row['Dependencia'])
                id_programa             = programas_map.get((row['Programa'],id_dependencia))
                id_componente           = componentes_map.get((row['Componente'], id_programa))
                id_acciones             = acciones_map.get(row['Accion'])
                id_tipo_beneficiario    = tipos_beneficiarios_map.get(row['Tipo de Beneficio'])
                
                id_archivo_benefisiario = None
                
                if id_dependencia:
                    id_archivo_benefisiario = carpetas_beneficiarios_map.get()
                
                Logger.add_to_log("info", f"ID Dependecas\n {id_dependencia}")
                Logger.add_to_log("info", f"ID Aciones\n {id_acciones}")
                Logger.add_to_log("info", f"ID Tipos de Beneficiarios\n {id_tipo_beneficiario}")
                # Faltan 3 mas
                #Logger.add_to_log("info", f"row\n {row}")
                #Logger.add_to_log("info", f"Aciones\n {id_acciones} - {row['Accion']}")
                
                row['Sexo']                             = id_sexo
                
                row['Estado (catálogo)']                = id_estado
                row['Municipio Dirección (catálogo)']   = str(id_municipio[1]) if id_municipio else None
                row['Colonia']                          = str(id_colonia[0]) if id_colonia else None
                row['Estado Civil']                     = id_estado_civil
                
                row['Dependencia']                      = id_dependencia
                row['Programa']                         = id_programa
                row['Componente']                       = id_componente
                row['Accion']                           = id_acciones
                row['Tipo de Beneficio']                = id_tipo_beneficiario
            
                
                
                
                Logger.add_to_log("info",f"Municipio Dirección (catálogo) {row['Municipio Dirección (catálogo)']}")
                # if row['Municipio Dirección (catálogo)'][1] == row['Estado (catálogo)']:
                #     Logger.add_to_log("info", "SI")
                #     Logger.add_to_log("debug", f"Municipio: {row['Municipio Dirección (catálogo)']}")
                # else:
                #     Logger.add_to_log("info", "No")
                #     Logger.add_to_log("debug", f"Municipio: {row['Municipio Dirección (catálogo)']}")
                
                
                if not id_dependencia:
                    Logger.add_to_log("warn", f"Dependecia no encontrada - {row['Dependencia']}" )
                    rows_errors.append(row)
                    continue
                
                if not id_programa:
                    #Logger.add_to_log('warn', f"Programa no encontrado - {row['Programa']}")
                    rows_errors.append(row)
                    continue
                
                if not id_componente:
                    #Logger.add_to_log('warn', f"Componente no encontrado - {row['Componente']}")
                    rows_errors.append(row)
                    continue
                    
                #rows_goods.append(row)
                
                # Verificar existencia de beneficiario
                curp                = row['Curp'] or None
                rfc                 = row['RFC'] or None
                
                id_beneficiario     = None
                
                if curp and rfc:
                    id_beneficiario = beneficiario_map.get((curp,rfc))
                    
                elif curp and not rfc:
                    id_beneficiario =  next(
                        (id_ben for(c,_), id_ben in beneficiario_map.items() if c == curp),
                        None
                    )         
                            
                elif rfc and not curp:
                    #Logger.add_to_log("info", "1 Un Dato pasado: RFC")
                    id_beneficiario = next(
                        (id_ben for (_, r), id_ben in beneficiario_map.items() if r == rfc),
                        None
                    )
                    
                else:
                    Logger.add_to_log("info","No se encontro, se dara de alta")
                
                if not id_beneficiario:
                    
                    id_beneficiario = str(uuid.uuid4())
                    
                    new_beneficiarios.append({
                        "id": id_beneficiario,
                        **{k: row[k] for k in Config.GROUP_ONE_KEYS}
                    })

                else:   
                    Logger.add_to_log("info", f"Ya existe, no se dara de alta y el ID del usuario:{id_beneficiario}")
                
                
                
                id_contacto = str(uuid.uuid4())
                # new_contacto.append({
                #     'id': id_contacto,
                #     **{k: row[k] for k in Config.GROUP_TWO_KEYS}
                # })
               
                new_contacto.append({
                    'id': id_contacto,
                    **{Config.COLUMN_MAP_GROUP_TWO[k]:row[k] for k in group_two_df if k in Config.COLUMN_MAP_GROUP_TWO}
                })
                
                Logger.add_to_log("info",f"Contacto \n{new_contacto}")
                
                id_apoyo = str(uuid.uuid4())
                Logger.add_to_log("info",f"Apoyo\n{row}")
                new_apoyos.append({
                    'id':id_apoyo,
                    'idBeneficiario':id_beneficiario,
                    'idContacto':id_contacto,
                    **{k:row[k] for k in group_tree_df}
                })
                
                
            
            new_beneficiarios_renamed = [
                {Config.COLUMN_MAP_GROUP_ONE.get(k,k): v for k, v in row.items()}
                for row in new_beneficiarios
            ]
            
            new_apoyos_renamed = [
                {Config.COLUMN_MAP_GROUP_TREE.get(k,k): v for k, v in row.items()}
                for row in new_apoyos
            ]
            
            Logger.add_to_log("info",f"Apoyo Nuevo\n{new_apoyos_renamed}")
            #Logger.add_to_log("debug", f"Contacto {new_contacto}")
            # Logger.add_to_log("debug", new_beneficiarios_renamed)
            
            # new_contacto_renamed = [
            #     {Config.COLUMN_MAP_GROUP_TWO.get(k,k): v for k, v in row.items()}
            #     for row in new_contacto
            # ]
            
            
            # if rows_errors:
            #     #Logger.add_to_log("info", f"Se encontraron {len(rows_errors)} registros con errores")
            #     for e in rows_errors:
            #         Logger.add_to_log("info", str(e))

            if new_beneficiarios_renamed:
                # Insert de los datos
                #BeneficiariosService.bulk_insert(new_beneficiarios_renamed)
                Logger.add_to_log("info", f"Estos son los beneficiarios que se darna de alta \n {new_beneficiarios_renamed}")
                
                if new_contacto:
                    #ContactosService.bluk_insert(new_contacto)
                    Logger.add_to_log("info", f"Antes \n{new_contacto}")
                    
                    if new_apoyos_renamed:
                        #ApoyosService.bulk_insert(new_apoyos_renamed)
                        Logger.add_to_log("info", f"Estos son los apoyos que se darna de alta \n{new_apoyos}")
                
            return jsonify({
                'success': True,
                'message': 'Info to Excel File',
                'data': [],
                'error' : None
            }),200
        except Exception as ex:
                Logger.add_to_log("error", str(ex))
                Logger.add_to_log("error", traceback.format_exc())
            
                return jsonify({'message': "ERROR", 'success': False}),500
        pass

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
            "Curp", "Nombre", "Apellido paterno", "Apellido Materno",
            "Fecha de Nacimiento", "Estado (catálogo)", "Estado Civil", "Sexo",
            "Calle", "Numero", "Colonia", "Municipio Dirección (catálogo)",
            "Telefono", "Telefono 2", "Correo", "Programa", "Componente",
            "Accion", "Fecha de Registro", "Monto", "Tipo de Beneficio",
            "RFC", "Regimen Capital", "Actividad", "Nombre Comercial",
            "Razón Social", "Localidad", "Dependencia", "Subprograma"
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
            "Estado (catálogo)": "Estado",
            "Municipio Dirección (catálogo)": "Municipio",
            "Sexo": "Sexo",
            "Estado Civil": "EstadoCivil",
            "Programa": "Programa",
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