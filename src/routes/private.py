from flask import Blueprint, jsonify, request
import polars as pl 
import io 

from ..services.beneficiarios_service   import BeneficiariosService
from ..services.search_service          import SearchService

import traceback

#Logger
from ..utils.Logger import Logger

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
def uploder_file():
    
    if 'file' not in request.files:
        Logger.add_to_log("error", "No se encontro un archivo en la peticion")
        return jsonify({
            'success': False,
            'message': 'File not in the peticion',
            'data': {},
            'error' : None
        }),400
        
    file = request.files['file']
    
    try:
        # Lectura de excel desde la memoria 
        df = pl.read_excel(io.BytesIO(file.read()))
        
        dataExcel = df.to_dicts()
        
        data = pl.DataFrame(dataExcel)
        
        group_one_keys      = ['Curp','RFC','Nombre','Apellido paterno','Apellido Materno','Fecha de Nacimiento']
        group_dow_keys      = ['Correo','Telefono','Telefono 2','Estado (catálogo)','Municipio Dirección (catálogo)','Colonia','Calle','Numero']
        group_tree_keys     = ['Dependencia','Programa','Componente','Accion','Tipo de Beneficio','Monto']
        
        results = [
            [
                {k: row[k] for k in group_one_keys},
                {k: row[k] for k in group_dow_keys},
                {k: row[k] for k in group_tree_keys}
            ]
            for row in data.to_dicts()
        ]
        # Insert in teble Beneficiarios
        # for row in results: 
        #     BeneficiariosService.add_beneficiario(row[0])
            
        Logger.add_to_log("info", results)
        
        for row in results:
            id_dependecia = SearchService.search_dependencia(row[2]['Dependencia'])
            Logger.add_to_log("info", row[2]['Dependencia'])
            Logger.add_to_log("info", id_dependecia)
            
            if id_dependecia:
                id_programa = SearchService.search_programas(row[2]['Programa'], id_dependecia)
                Logger.add_to_log("info", row[2]['Programa'])
                Logger.add_to_log("info", id_programa)
                
                
            
        
        return jsonify({
            'success': True,
            'message': 'Info to Excel File',
            'data': results,
            'error' : None
        }),200
    except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
        
            return jsonify({'message': "ERROR", 'success': False}),500