from flask import Blueprint, jsonify, request
#import polars as pl 
import io 
#import uuid

#from config import Config

#from ..services.beneficiarios_service   import BeneficiariosService
from ...services.excel_service           import ExcelService
#from ..services.search_service          import SearchService

import traceback

#Logger
from ...utils.Logger import Logger

private_bp = Blueprint('private',__name__)

@private_bp.route('/test')
def test():
    return jsonify({
        'success': True,
        'message': 'Sección Privado OK',
        'data': {},
        'error' : None
    })     
    
    
@private_bp.route('/uploader_file', methods=["POST"])
def uploader_file():
    
    if 'file' not in request.files:
        Logger.add_to_log("error", "No se encontro un archivo en la peticion")
        return jsonify({
            'success': False,
            'message': 'File not in the peticion',
            'data': {},
            'error' : None
        }),400
        
    #file = request.files['file']
   
    try:
       
        ExcelService.process_file(request.files['file'])

        return jsonify({
            'success': True,
            'message': 'Info to Excel File',
            'data': "Accepted",
            'error' : None
        }),200
    except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
        
            return jsonify({'message': "ERROR", 'success': False}),500