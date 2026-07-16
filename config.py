
import os 
import polars as pl 

class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY")
    SQLALCHEMY_DATABASE_URI = (f"mysql+pymysql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@" f"{os.environ.get('DB_HOST')}/{os.environ.get('DB_NAME')}")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    GROUP_ONE_KEYS = ['Curp','Nombre','Apellido Paterno','Apellido Materno','Fecha de Nacimiento','Sexo','RFC',"Regimen Capital","Actividad","Nombre Comercial","Razón Social"]
    COLUMN_MAP_GROUP_ONE = {
        "Curp": "CURP",
        "Nombre": "nombre",
        "Apellido Paterno": "aPaterno",
        "Apellido Materno": "aMaterno",
        "Fecha de Nacimiento": "fNacimiento",
        "Sexo": "idSexo",
        "RFC": "RFC",
        "Regimen Capital":"regimenCapital",
        "Actividad":"actividad",
        "Nombre Comercial":"nombreComercial",
        "Razón Social":"razonSocial",
    }
    GROUP_TWO_KEYS = ['Correo','Telefono','Telefono 2','Estado','Estado Civil','Municipio Dirección','Colonia','Calle','Numero']
    COLUMN_MAP_GROUP_TWO = {
        'Correo':'correo',
        'Telefono':'telefono1',
        'Telefono 2':'telefono2',
        'Estado':'idEstado',
        'Municipio Dirección':'idMunicipio',
        'Colonia':'idColonia',
        'Calle':'calle',
        'Numero':'numero',
        'Estado Civil':'idEstadoCivil'
    }
    GROUP_TREE_KEYS = ['Dependencia','Programa','Componente','Accion','Tipo de Beneficio','Fecha de Registro','Monto']
    COLUMN_MAP_GROUP_TREE = {
        'Dependencia':'idDependencia',
        'Programa':'idPrograma',
        'Componente':'idComponente',
        'Accion':'idAccion',
        'Tipo de Beneficio':'idTipoBeneficio',
        'Fecha de Registro': 'fRegistro',
        'Monto':'monto'
    }

    
     # Lista de orígenes permitidos (convertimos el texto en lista)
    IP_SERVER_FRONT = os.getenv("IP_SERVER_FRONT", "").split(",") if os.getenv("IP_SERVER_FRONT") else []
    CELLS_DATA_TYPES = {
        "Curp" : pl.Utf8,
        "Nombre" : pl.Utf8,
        "Apellido Paterno" :pl.Utf8,
        "Apellido Materno" :pl.Utf8,
        "Fecha de Nacimiento":pl.Utf8,
        "Estado" :pl.Utf8,
        "Estado Civil" :pl.Utf8,
        "Sexo":pl.Utf8,
        "Calle":pl.Utf8,
        "Numero" :pl.Utf8,
        "Colonia":pl.Utf8,
        "Municipio Dirección":pl.Utf8,
        "Telefono" :pl.Utf8,
        "Telefono 2" :pl.Utf8,
        "Correo":pl.Utf8,
        "Programa":pl.Utf8,
        "Componente":pl.Utf8,
        "Accion":pl.Utf8,
        "Fecha de Registro":pl.Utf8,
        "Monto":pl.Utf8,
        "Tipo de Beneficio":pl.Utf8,
        "RFC":pl.Utf8,
        "Regimen Capital":pl.Utf8,
        "Actividad":pl.Utf8,
        "Nombre Comercial":pl.Utf8,
        "Razón Social":pl.Utf8,
        "Dependencia":pl.Utf8,
        "Subprograma":pl.Utf8,
    }

    # ── ApoyoIntegral ─────────────────────────────────────────
    APOYO_INTEGRAL_KEYS = [
        'Curp', 
        'Nombre', 
        'Apellido Paterno', 
        'Apellido Materno',
        'Fecha de Nacimiento', 
        'Estado Civil', 
        'Sexo',
        'Estado', 
        'Municipio Dirección',
        'Colonia', 
        'Calle', 
        'Numero', 
        'Telefono', 
        'Telefono 2', 
        'Correo',
        'Dependencia', 
        'Programa', 
        'Subprograma',
        'Componente', 
        'Accion',
        'Fecha de Registro', 
        'Monto', 
        'Tipo de Beneficio',
        'RFC',
        'Regimen Capital', 
        'Actividad', 
        'Nombre Comercial', 
        'Razón Social',
        'Nombre Captura',
        'Nombre Valida'
    ]

    COLUMN_MAP_APOYO_INTEGRAL = {
        "Curp":                             "CURP",
        "Nombre":                           "nombre",
        "Apellido Paterno":                 "aPaterno",
        "Apellido Materno":                 "aMaterno",
        "Fecha de Nacimiento":              "fNacimiento",
        "Estado Civil":                     "estado_civil",
        "Sexo":                             "idSexo",           # lookup catálogo → también llena `sexo`
        "Estado":                           "idEstado",         # lookup catálogo → también llena `estado`
        "Municipio Dirección":   "idMunicipio",      # lookup catálogo → también llena `municipio`
        "Colonia":                          "colonia",
        "Calle":                            "calle",
        "Numero":                           "numero",
        "Telefono":                         "telefono1",
        "Telefono 2":                       "telefono2",
        "Correo":                           "correo",
        "Dependencia":                      "idDependencia",    # lookup catálogo → también llena `dependencia`
        "Programa":                         "idPrograma",       # lookup catálogo → también llena `programa`
        "Subprograma":                      "idSubprograma",    # lookup catálogo → también llena `subprograma`
        "Componente":                       "idComponente",     # lookup catálogo → también llena `componente`
        "Accion":                           "idAccion",         # lookup catálogo → también llena `accion`
        "Fecha de Registro":                "fRegistro",
        "Monto":                            "monto",
        "Tipo de Beneficio":                "idTipoBeneficio",  # lookup catálogo → también llena `tipoBeneficio`
        "RFC":                              "RFC",
        "Regimen Capital":                  "regimenCapital",
        "Actividad":                        "actividad",
        "Nombre Comercial":                 "nombreComercial",
        "Razón Social":                     "razonSocial",                 
        "Nombre Captura":                   "creador",    
        "Nombre Valida":                    "modificador",          
    }



    # Lista de columnas 
    CAMPOS_OBLIGATORIOS = [
        "Curp", 
        "Nombre", 
        "Apellido Paterno", 
        "Apellido Materno",
        "Fecha de Nacimiento", 
        "Estado", 
        "Estado Civil", 
        "Sexo",
        "Calle", 
        "Numero", 
        "Colonia", 
        "Municipio Dirección",
        "Telefono", 
        "Telefono 2", 
        "Correo",
        "Programa", 
        "Componente", 
        "Accion",
        "Fecha de Registro", 
        "Monto", 
        "Tipo de Beneficio",
        "RFC", 
        "Regimen Capital", 
        "Actividad", 
        "Nombre Comercial",
        "Razón Social", 
        "Dependencia", 
        "Subprograma"
    ]
