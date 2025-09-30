from ..database.connection import db

class CarpetaBeneficiarios(db.Model):
    __tablename__ = 'carpetabeneficiarios'
    
    id              = db.Column(db.String(50), primary_key = True)
    idDependencia   = db.Column(db.String(50), unique=True, nullable=False)
    mes             = db.Column(db.Integer)
    anio            = db.Column(db.Integer)
    #deleted         = db.Column(db.String(1))
    
    deleted = db.Column(db.Boolean, nullable=False, default=False)

    # `id` CHAR(50) NOT NULL DEFAULT uuid() COLLATE 'utf8mb4_spanish_ci',
	# `idDependencia` CHAR(50) NOT NULL COLLATE 'utf8mb4_spanish_ci',
	# `mes` INT(2) NOT NULL,
	# `anio` INT(2) NOT NULL,
	# `fCreacion` DATETIME NOT NULL DEFAULT current_timestamp(),
	# `creador` CHAR(50) NOT NULL COLLATE 'utf8mb4_spanish_ci',
	# `estado` ENUM('En Proceso','En Validacion','Publicado') NOT NULL DEFAULT 'En Proceso' COLLATE 'utf8mb4_spanish_ci',
	# `deleted` TINYINT(1) NOT NULL DEFAULT '0',
 
    def to_dict(self):
        return { c.name: getattr(self, c.name) for c in self.__table__.columns}