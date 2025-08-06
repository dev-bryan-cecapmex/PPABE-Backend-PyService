
from models.anio import Anio
from database.connection import db

class AnioServicie:
    @staticmethod
    def get_all():
        anios = Anio.query.all()
        return [e.to_dict() for e in anios]