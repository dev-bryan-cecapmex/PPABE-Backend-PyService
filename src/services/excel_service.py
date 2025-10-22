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