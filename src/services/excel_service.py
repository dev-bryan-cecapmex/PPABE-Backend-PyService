from ..database.connection      import db
from flask                      import jsonify
from config                     import Config

import io 

#Logger
from ..utils.Logger                     import Logger

#Mapeos
from ..utils.Mapeo                      import Mapeo

from ..services.beneficiarios_service   import BeneficiariosService
from ..services.search_service          import SearchService

import traceback

import polars as pl 
import uuid




class ExcelService:
    @staticmethod
    def process_file(file):
        try:
            # Lectura de excel desde la memoria 
            data = pl.read_excel(io.BytesIO(file.read()))
            
            rows = data.to_dicts()
            
            # Datos Agrupados del Excel
            group_one_df    = data.select(Config.GROUP_ONE_KEYS).to_dict()
            group_tow_df    = data.select(Config.GROUP_TOW_KEYS).to_dict()
            group_tree_df   = data.select(Config.GROUP_TREE_KEYS).to_dict()
            
            results         = list(zip(group_one_df,group_tow_df,group_tree_df))
            
            sexo_map        = SearchService.get_sexo_map()
            
            # Datos de la DB
            dependencias_map    = SearchService.get_dependencias_map()
            programas_map       = SearchService.get_programas_map()
            componentes_map     = SearchService.get_componentes_map()
            beneficiario_map    = SearchService.get_beneficiarios_map()
            # Logger.add_to_log("info", "Esto son los Beneficiarios")
            # Logger.add_to_log("info", beneficiario_map)
            
            #Lista de los nuevos registros
            new_beneficiarios   = []
            new_contacto        = []
            new_apoyos          = []
            
            rows_errors     = []
            rows_goods      = []
            
            for row in rows:
                # Busqueda de id de Sexo
                id_sexo         = sexo_map.get(row['Sexo']) 
                row['Sexo']     = id_sexo
                
                id_dependencia  = dependencias_map.get(row['Dependencia'])
                
                if not id_dependencia:
                    #Logger.add_to_log("warn", f"Dependecia no encontrada - {row['Dependencia']}" )
                    rows_errors.append(row)
                    continue
                    
                id_programa = programas_map.get((row['Programa'],id_dependencia))
                
                if not id_programa:
                    #Logger.add_to_log('warn', f"Programa no encontrado - {row['Programa']}")
                    rows_errors.append(row)
                    continue
                
                id_componente = componentes_map.get((row['Componente'], id_programa))
                
                if not id_componente:
                    #Logger.add_to_log('warn', f"Componente no encontrado - {row['Componente']}")
                    rows_errors.append(row)
                    continue
                    
                #rows_goods.append(row)
                
                # Verificar existencia de beneficiario
                curp                = row['Curp'] or None
                rfc                 = row['RFC'] or None
                
                id_beneficiario     = None
                id_new_beneficiario = None
                
                #Logger.add_to_log("info",f"x CURP {curp} - RFC {rfc}")
                
                if curp and rfc:
                    #Logger.add_to_log("info", "1 Dos datos pasados por parametro: CRUP - RFC")
                    id_beneficiario = beneficiario_map.get((curp,rfc))
                    #Logger.add_to_log("warn", id_beneficiario)
                    
                    
                # key_benef = (row["Curp"], row["RFC"])
                elif curp and not rfc:
                    #Logger.add_to_log("info", "1 Un Dato pasado: CURP")
                    id_beneficiario =  next(
                        (id_ben for(c,_), id_ben in beneficiario_map.items() if c == curp),
                        None
                    )                 
                    #Logger.add_to_log("warn", id_beneficiario)
                    
                
                # Logger.add_to_log("info",beneficiario_map)
                # id_beneficiario = beneficiario_map.get(key_benef)
                
                elif rfc and not curp:
                    #Logger.add_to_log("info", "1 Un Dato pasado: RFC")
                    id_beneficiario = next(
                        (id_ben for (_, r), id_ben in beneficiario_map.items() if r == rfc),
                        None
                    )
                    #Logger.add_to_log("warn", id_beneficiario)
                    
                else:
                    Logger.add_to_log("info","No se encontro, se dara de alta")
                
                if not id_beneficiario:
                    
                    id_beneficiario = str(uuid.uuid4())
                    
                    new_beneficiarios.append({
                        "id": id_beneficiario,
                        **{k: row[k] for k in Config.GROUP_ONE_KEYS}
                    })
                    
                    #Logger.add_to_log("critical", new_beneficiarios)
                    
                else:   
                    Logger.add_to_log("info", f"Ya existe, no se dara de alta y el ID del usuario:{id_beneficiario}")
                
                
                
                id_contacto = str(uuid.uuid4())
                new_contacto.append({
                    'id': id_contacto,
                    **{Config.COLUMN_MAP[k]:row[k] for k in group_tow_df if k in Config.COLUMN_MAP}
                })
                
                id_apoyo = str(uuid.uuid4())
                new_apoyos.append({
                    'id':id_apoyo,
                    'idBeneficiario':id_beneficiario,
                    'idContacto':id_contacto,
                    **{k:row[k] for k in group_tree_df}
                })

            
            new_beneficiarios_renamed = [
                {Config.COLUMN_MAP.get(k,k): v for k, v in row.items()}
                for row in new_beneficiarios
            ]
            # Logger.add_to_log("debug", new_beneficiarios)
            # Logger.add_to_log("debug", new_beneficiarios_renamed)
            
            
            if rows_errors:
                #Logger.add_to_log("info", f"Se encontraron {len(rows_errors)} registros con errores")
                for e in rows_errors:
                    Logger.add_to_log("info", str(e))

            if new_beneficiarios_renamed:
                # Insert de los datos
                BeneficiariosService.bulk_insert(new_beneficiarios_renamed)
                Logger.add_to_log("info", f"Estos son los beneficiarios que se darna de alta \n {new_beneficiarios_renamed}")
                
            if new_contacto:
                Logger.add_to_log("info", f"Estos son los contacto que se darna de alta \n{new_contacto}")
                
            if new_apoyos:
                Logger.add_to_log("info", f"Estos son los apoyos que se darna de alta \n{new_apoyos}")
                
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