from flask import Blueprint, jsonify, request, send_file
#import polars as pl 
from io import BytesIO

from src.services.datos_plantilla_service import CatalogosService 
#import uuid

#from config import Config

#from ..services.beneficiarios_service   import BeneficiariosService
from ...services.excel_service           import ExcelService

#from ..services.search_service          import SearchService

import traceback

#Logger
from ...utils.Logger import Logger

private_bp = Blueprint('private',__name__)


@private_bp.route("/download_template", methods=["POST"])
def getTemplate():
    try:
        body = request.get_json()
        id_dependencia = body.get("idDependencia")
        anio = body.get("anio")

        if not id_dependencia or not anio:
            return jsonify({
                "success": False,
                "message": "idDependencia y anio son requeridos",
                "data": {},
                "error": None
            }), 400

        # 🧩 Obtener catálogos desde el servicio con la nueva jerarquía
        catalogos = {
            "Estado": [{"id": e.id, "nombre": e.nombre} for e in CatalogosService.get_estados()],
            "Municipio": [{"id": m.id, "nombre": m.nombre} for m in CatalogosService.get_municipios()],
            "EstadoCivil": [{"id": ec.id, "nombre": ec.nombre} for ec in CatalogosService.get_estados_civiles()],
            "Sexo": [{"id": s.id, "nombre": s.nombre} for s in CatalogosService.get_sexos()],
            "Dependencia": (
                [{"id": d.id, "nombre": d.nombre}] if (d := CatalogosService.get_dependencia(id_dependencia)) else []
            ),

            # 🔽 Nueva jerarquía
            "Programa": [{"id": p.id, "nombre": p.nombre} for p in CatalogosService.get_programas(id_dependencia, anio)],
            "Subprograma": [{"id": sp.id, "nombre": sp.nombre} for sp in CatalogosService.get_subprogramas(id_dependencia, anio)],
            "Componente": [{"id": c.id, "nombre": c.nombre} for c in CatalogosService.get_componentes(id_dependencia, anio)],

            "Accion": [{"id": a.id, "nombre": a.nombre} for a in CatalogosService.get_acciones()],
            "TipoBeneficio": [{"id": tb.id, "nombre": tb.nombre} for tb in CatalogosService.get_tipos_beneficios()],
            "Colonia": [{"id": c.id, "nombre": c.nombre} for c in CatalogosService.get_colonias()]
        }

        # 🧾 Generar Excel con los catálogos listados
        wb = ExcelService.generate_template(catalogos)

        # 🪣 Guardar en memoria y devolver como descarga
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        return send_file(
            output,
            as_attachment=True,
            download_name="PlantillaBeneficiarios.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as ex:
        return jsonify({
            "success": False,
            "message": "Error al generar catálogos",
            "data": {},
            "error": str(ex)
        }), 500
