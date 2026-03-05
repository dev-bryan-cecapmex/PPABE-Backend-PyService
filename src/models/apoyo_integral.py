from ..database.connection import db


# class ApoyoIntegral(db.Model):
#     __tablename__ = 'ApoyoIntegral'

#     id = db.Column(db.String(36), primary_key=True)

#     CURP = db.Column(db.String(18), nullable=True)
#     RFC = db.Column(db.String(13), nullable=True)
#     regimenCapital = db.Column(db.String(255), nullable=True)
#     actividad = db.Column(db.String(255), nullable=True)
#     nombreComercial = db.Column(db.String(255), nullable=True)
#     razonSocial = db.Column(db.String(255), nullable=True)
#     sexo = db.Column(db.String(50), nullable=True)
#     nombre = db.Column(db.String(255), nullable=True)
#     aPaterno = db.Column(db.String(255), nullable=True)
#     aMaterno = db.Column(db.String(255), nullable=True)
#     fNacimiento = db.Column(db.Date, nullable=True)

#     correo = db.Column(db.String(255), nullable=False)
#     telefono1 = db.Column(db.String(13), nullable=False)
#     telefono2 = db.Column(db.String(13), nullable=False)
#     estado = db.Column(db.String(150), nullable=False)
#     municipio = db.Column(db.String(150), nullable=False)
#     calle = db.Column(db.Text, nullable=False)
#     colonia = db.Column(db.Text, nullable=False)
#     numero = db.Column(db.String(255), nullable=False, default='')
#     estado_civil = db.Column(db.String(150), nullable=True)

#     beneficiario = db.Column(db.String(36), nullable=False)
#     contacto = db.Column(db.String(36), nullable=False)
#     dependencia = db.Column(db.String(36), nullable=False)
#     programa = db.Column(db.String(36), nullable=False)
#     subprograma = db.Column(db.String(36), nullable=False)
#     componente = db.Column(db.String(36), nullable=False)
#     accion = db.Column(db.String(36), nullable=False)
#     tipoBeneficio = db.Column(db.String(36), nullable=False)
#     carpetaBeneficiarios = db.Column(db.String(36), nullable=False)

#     monto = db.Column(db.String(255), nullable=False)
#     fRegistro = db.Column(db.Date)

#     idHistorialCarga = db.Column(
#         db.String(36),
#         db.ForeignKey('HistorialCarga.id'),
#         nullable=True
#     )

#     creador = db.Column(db.String(36), nullable=True)
#     modificador = db.Column(db.String(36), nullable=True)
#     fCreacion = db.Column(db.Date)
#     fModificacion = db.Column(db.Date)
#     deleted = db.Column(db.Boolean, nullable=False, default=False)

#     def to_dict(self):
#         return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class ApoyoIntegral(db.Model):
    __tablename__ = 'ApoyoIntegral'
    
    id                      = db.Column(db.String(36), primary_key=True)
    
    CURP                    = db.Column(db.String(18), nullable=True)
    RFC                     = db.Column(db.String(13), nullable=True)
    regimenCapital          = db.Column(db.String(255), nullable=True)
    actividad               = db.Column(db.String(255), nullable=True)
    nombreComercial         = db.Column(db.String(255), nullable=True)
    razonSocial             = db.Column(db.String(255), nullable=True)
    idSexo                  = db.Column(db.String(36), nullable=True)
    sexo                    = db.Column(db.String(50), nullable=True)
    nombre                  = db.Column(db.String(255), nullable=True)
    aPaterno                = db.Column(db.String(255), nullable=True)
    aMaterno                = db.Column(db.String(255), nullable=True)
    fNacimiento             = db.Column(db.Date, nullable=True)
    correo                  = db.Column(db.String(255), nullable=True)
    telefono1               = db.Column(db.String(13), nullable=True)
    telefono2               = db.Column(db.String(13), nullable=True)
    idEstado                = db.Column(db.String(36), nullable=True)
    estado                  = db.Column(db.String(255), nullable=True)
    idMunicipio             = db.Column(db.String(36), nullable=True)
    municipio               = db.Column(db.String(255), nullable=True)
    calle                   = db.Column(db.Text, nullable=True)
    colonia                 = db.Column(db.Text, nullable=True)
    numero                  = db.Column(db.String(255), nullable=True, default='')
    idEstadoCivil           = db.Column(db.String(36), nullable=True)
    estadoCivil            = db.Column(db.String(255), nullable=True)
    idDependencia           = db.Column(db.String(36), nullable=True)
    dependencia             = db.Column(db.String(255), nullable=True)
    idPrograma              = db.Column(db.String(36), nullable=True)
    programa                = db.Column(db.String(255), nullable=True)
    idSubprograma           = db.Column(db.String(36), nullable=True)
    subprograma             = db.Column(db.String(255), nullable=True)
    idComponente            = db.Column(db.String(36), nullable=True)
    componente              = db.Column(db.String(255), nullable=True)
    idAccion                = db.Column(db.String(36), nullable=True)
    accion                  = db.Column(db.String(255), nullable=True)
    idTipoBeneficio         = db.Column(db.String(36), nullable=True)
    tipoBeneficio           = db.Column(db.String(255), nullable=True)
    idCarpetaBeneficiarios  = db.Column(db.String(36), nullable=True)
    monto                   = db.Column(db.String(255), nullable=True)
    fRegistro               = db.Column(db.Date, nullable=True, server_default=db.func.current_timestamp())
    idHistorialCarga        = db.Column(db.String(36), db.ForeignKey('HistorialCarga.id'), nullable=True)
    creador                 = db.Column(db.String(36), nullable=True)
    modificador             = db.Column(db.String(36), nullable=True)
    fCreacion               = db.Column(db.DateTime, nullable=True, server_default=db.func.current_timestamp())
    fModificacion           = db.Column(db.DateTime, nullable=True, server_default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())
    deleted                 = db.Column(db.SmallInteger, nullable=True, default=0)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    