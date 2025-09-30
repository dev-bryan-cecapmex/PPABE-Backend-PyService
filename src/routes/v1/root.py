
from flask import Blueprint, jsonify

root_bp = Blueprint('root',__name__)

    
@root_bp.get('/healthz')
def healthz():
    return jsonify(
        success=True,
        message="ok",
        data={"service": "ppabe", "api_version": "v1"},
        error=None
    ),200
    
    
