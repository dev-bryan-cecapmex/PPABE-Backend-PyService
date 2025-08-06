from flask import Blueprint, jsonify
from ..services.anio_service import AnioServicie

public_bp = Blueprint('public', __name__)


@public_bp.route("/anios", methods=["GET"])
def get_anios():
    anios = AnioServicie.get_all()
    return jsonify({
        "success": True,
        "data": anios
    })

@public_bp.route("/test")
def test():
    return jsonify({
        'success': True,
        'message': 'Sección pública OK',
        'data': {},
        'error' : None
    })


