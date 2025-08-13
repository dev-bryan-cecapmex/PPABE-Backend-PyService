from database.connection import db

class Apoyos(db.Model):
    __tablename__ = 'apoyos'
    
    id                          = db.Column(db.String(36), primary_key = True)
    idBeneficiario              = db.Column(db.String(36))
    idContacto                  = db.Column(db.String(36))
    idDependencia               = db.Column(db.String(36))
    idPrograma                  = db.Column(db.String(36))
    idComponente                = db.Column(db.String(36))
    idAccion                    = db.Column(db.String(36))
    idTipoBeneficio             = db.Column(db.String(36))
    idCarpetaBeneficiarios      = db.Column(db.String(36))
    monto                       = db.Column(db.String(255))
    fRegistro                   = db.Column(db.Date)
    creador                     = db.Column(db.String(36))
    modificador                 = db.Column(db.String(36))
    fCreacion                   = db.Column(db.Date)
    fModificacion               = db.Column(db.Date)
    deleted                     = db.Column(db.Boolean)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
