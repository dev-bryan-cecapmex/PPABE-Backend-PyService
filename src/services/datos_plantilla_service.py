from ..database.connection import db
from ..models.estados import Estados
from ..models.municipios import Municipios
from ..models.estados_civiles import EstadosCiviles
from ..models.sexos import Sexos
from ..models.dependencias import Dependencias
from ..models.programas import Programas
from ..models.subprogramas import Subprogramas   # 👈 nuevo import
from ..models.componentes import Componentes
from ..models.acciones import Acciones
from ..models.tipos_beneficios import TiposBeneficiarios
from ..models.colonias import Colonias
from ..models.dependenciaprogramaanio import DependenciaProgramaAnio


class CatalogosService:

    @staticmethod
    def get_estados():
        return db.session.query(Estados.id, Estados.nombre).filter(Estados.deleted == "0").all()

    @staticmethod
    def get_municipios():
        return db.session.query(Municipios.id, Municipios.nombre).filter(Municipios.deleted == "0").all()

    @staticmethod
    def get_estados_civiles():
        return db.session.query(EstadosCiviles.id, EstadosCiviles.nombre).filter(EstadosCiviles.deleted == "0").all()

    @staticmethod
    def get_sexos():
        return db.session.query(Sexos.id, Sexos.nombre).filter(Sexos.deleted == "0").all()

    @staticmethod
    def get_dependencia(id_dependencia):
        return db.session.query(Dependencias.id, Dependencias.nombre).filter(Dependencias.id == id_dependencia).first()

    @staticmethod
    def get_programas(id_dependencia, anio):
        return (
            db.session.query(Programas.id, Programas.nombre)
            .join(
                DependenciaProgramaAnio,
                Programas.idDependencia == DependenciaProgramaAnio.idDependencia
            )
            .filter(
                Programas.idDependencia == id_dependencia,
                Programas.deleted == False,
                DependenciaProgramaAnio.anio == anio
            )
            .all()
        )

    # 🆕 NUEVO: Subprogramas entre Programa y Componente
    @staticmethod
    def get_subprogramas(id_dependencia, anio):
        return (
            db.session.query(Subprogramas.id, Subprogramas.nombre)
            .join(Programas, Subprogramas.idPrograma == Programas.id)
            .join(
                DependenciaProgramaAnio,
                Programas.idDependencia == DependenciaProgramaAnio.idDependencia
            )
            .filter(
                DependenciaProgramaAnio.idDependencia == id_dependencia,
                DependenciaProgramaAnio.anio == anio,
                Subprogramas.deleted == False
            )
            .all()
        )

    # 🔧 Modificado: ahora se filtra por idSubPrograma
    @staticmethod
    def get_componentes(id_dependencia, anio):
        return (
            db.session.query(Componentes.id, Componentes.nombre)
            .join(Subprogramas, Componentes.idSubPrograma == Subprogramas.id)
            .join(Programas, Subprogramas.idPrograma == Programas.id)
            .join(
                DependenciaProgramaAnio,
                Programas.idDependencia == DependenciaProgramaAnio.idDependencia
            )
            .filter(
                DependenciaProgramaAnio.idDependencia == id_dependencia,
                DependenciaProgramaAnio.anio == anio,
                Componentes.deleted == False
            )
            .all()
        )

    @staticmethod
    def get_acciones():
        return db.session.query(Acciones.id, Acciones.nombre).filter(Acciones.deleted == "0").all()

    @staticmethod
    def get_tipos_beneficios():
        return db.session.query(TiposBeneficiarios.id, TiposBeneficiarios.nombre).filter(TiposBeneficiarios.deleted == "0").all()

    @staticmethod
    def get_colonias():
        return db.session.query(Colonias.id, Colonias.nombre).filter(Colonias.deleted == "0").all()