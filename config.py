
import os 

class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY")
    SQLALCHEMY_DATABASE_URI = (f"mysql+pymysql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@" f"{os.environ.get('DB_HOST')}/{os.environ.get('DB_NAME')}")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    GROUP_ONE_KEYS = ['Curp','RFC','Nombre','Apellido paterno','Apellido Materno','Fecha de Nacimiento']
    GROUP_TOW_KEYS = ['Correo','Telefono','Telefono 2','Estado (catálogo)','Municipio Dirección (catálogo)','Colonia','Calle','Numero']
    GROUP_TREE_KEYS = ['Dependencia','Programa','Componente','Accion','Tipo de Beneficio','Monto']
    