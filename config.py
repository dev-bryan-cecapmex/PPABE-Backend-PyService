
import os 

class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY")
    SQLALCHEMY_DATABASE_URI = (f"mysql+pymysql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@" f"{os.environ.get('DB_HOST')}/{os.environ.get('DB_NAME')}")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    GROUP_ONE_KEYS = ['Curp','Nombre','Apellido paterno','Apellido Materno','Fecha de Nacimiento','Sexo','RFC',"Regimen Capital","Actividad","Nombre Comercial","Razón Social"]
    COLUMN_MAP_GROUP_ONE = {
        "Curp": "CURP",
        "Nombre": "nombre",
        "Apellido paterno": "aPaterno",
        "Apellido Materno": "aMaterno",
        "Fecha de Nacimiento": "fNacimiento",
        "Sexo": "idSexo",
        "RFC": "RFC",
        "Regimen Capital":"regimenCapital",
        "Actividad":"actividad",
        "Nombre Comercial":"nombreComercial",
        "Razón Social":"razonSocial",
    }
    GROUP_TWO_KEYS = ['Correo','Telefono','Telefono 2','Estado (catálogo)','Municipio Dirección (catálogo)','Colonia','Calle','Numero']
    COLUMN_MAP_GROUP_TWO = {
        'Correo':'correo',
        'Telefono':'telefono1',
        'Telefono 2':'telefono2',
        'Estado (catálogo)':'idEstado',
        'Municipio Dirección (catálogo)':'idMunicipio',
        'Colonia':'idColonia',
        'Calle':'calle',
        'Numero':'numero'
    }
    GROUP_TREE_KEYS = ['Dependencia','Programa','Componente','Accion','Tipo de Beneficio','Monto']
    COLUMN_MAP_GROUP_TREE = {
        'Dependencia':'idDependencia',
        'Programa':'idPrograma',
        'Componente':'idComponente',
        'Accion':'idAccion',
        'Tipo de Beneficio':'idTipoBeneficio',
        'Monto':'monto'
    }
    
     # Lista de orígenes permitidos (convertimos el texto en lista)
    IP_SERVER_FRONT = os.getenv("IP_SERVER_FRONT", "").split(",") if os.getenv("IP_SERVER_FRONT") else []
