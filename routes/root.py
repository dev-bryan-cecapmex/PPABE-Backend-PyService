
from flask import Blueprint, jsonify

root_bp = Blueprint('root',__name__)

@root_bp.route('/')
def index():
    return jsonify({
            'success': True,
            'message': 'API funcionando correctamente',
            'data': {},
            'error' : None
        }),200