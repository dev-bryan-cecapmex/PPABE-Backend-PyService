from flask import Blueprint, jsonify
from ...services.anio_service import AnioService
from ...services.search_service import SearchService

import traceback

#Logger
from ...utils.Logger import Logger

public_bp = Blueprint('public', __name__)


@public_bp.route("/anios", methods=["GET"])
def get_anios():
    try:
        componentes = SearchService.get_componentes_map()
        
        Logger.add_to_log("info", componentes)
        
        return jsonify({
            "success": True,
            "data": "subprogramas"
        })
    except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
        
            return jsonify({'message': "ERROR", 'success': False})

@public_bp.route("/test")
def test():
    return jsonify({
        'success': True,
        'message': 'Sección pública OK',
        'data': {},
        'error' : None
    })


