from flask import Blueprint, jsonify

#Logger
from ..utils import Logger

private_bp = Blueprint('private',__name__)

@private_bp.route('/test')
def test():
    return jsonify({
        'success': True,
        'message': 'Sección Privado OK',
        'data': {},
        'error' : None
    })     