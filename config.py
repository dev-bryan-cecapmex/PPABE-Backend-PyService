
import os 
import polars as pl 

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
    GROUP_TWO_KEYS = ['Correo','Telefono','Telefono 2','Estado (catálogo)','Estado Civil','Municipio Dirección (catálogo)','Colonia','Calle','Numero']
    COLUMN_MAP_GROUP_TWO = {
        'Correo':'correo',
        'Telefono':'telefono1',
        'Telefono 2':'telefono2',
        'Estado (catálogo)':'idEstado',
        'Municipio Dirección (catálogo)':'idMunicipio',
        'Colonia':'idColonia',
        'Calle':'calle',
        'Numero':'numero',
        'Estado Civil':'idEstadoCivil'
    }
    GROUP_TREE_KEYS = ['Dependencia']
    COLUMN_MAP_GROUP_TREE = {
        'Dependencia':'idDependencia'

    }
    
     # Lista de orígenes permitidos (convertimos el texto en lista)
    IP_SERVER_FRONT = os.getenv("IP_SERVER_FRONT", "").split(",") if os.getenv("IP_SERVER_FRONT") else []
    CELLS_DATA_TYPES = {
        "Curp" : pl.Utf8,
        "Nombre" : pl.Utf8,
        "Apellido paterno" :pl.Utf8,
        "Apellido Materno" :pl.Utf8,
        "Fecha de Nacimiento":pl.Utf8,
        "Estado (catálogo)" :pl.Utf8,
        "Estado Civil" :pl.Utf8,
        "Sexo":pl.Utf8,
        "Calle":pl.Utf8,
        "Numero" :pl.Utf8,
        "Colonia":pl.Utf8,
        "Municipio Dirección (catálogo)":pl.Utf8,
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
        "Localidad":pl.Utf8,
        "Dependencia":pl.Utf8,
        "Subprograma":pl.Utf8,
    }
