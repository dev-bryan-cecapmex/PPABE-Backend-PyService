# from ..database.connection import db
# from ..models.apoyos import Apoyos
# 
# # Logger
# from ..utils.Logger import Logger
# 
# class ApoyosService:
#     @staticmethod
#     def bulk_insert(rows):
#         try:
#             db.session.bulk_insert_mappings(Apoyos,rows)
#             db.session.commit()
#             return {"Nuevos Apoyos": len(rows)}
#         except Exception as ex:
#             db.session.rollback()
#             Logger.add_to_log("error", f"Error bulk_insert beneficiarios: {ex}")
from sqlalchemy import text
from ..database.connection import db
from ..models.apoyos import Apoyos
from ..utils.Logger import Logger


class ApoyosService:
    @staticmethod
    def bulk_insert(rows, batch_size=5000):
        if not rows:
            return {"Nuevos Apoyos": 0}

        conn = None
        total = 0

        try:
            # Fuerza a que la sesión use una conexión y sea la misma
            conn = db.session.connection()

            # ✅ Desactivar checks SOLO en esta sesión/conexión
            conn.execute(text("SET SESSION foreign_key_checks = 0"))
            conn.execute(text("SET SESSION unique_checks = 0"))

            for i in range(0, len(rows), batch_size):
                chunk = rows[i:i + batch_size]
                db.session.bulk_insert_mappings(Apoyos, chunk)
                db.session.flush()  # envía el batch sin cerrar la transacción
                total += len(chunk)

                Logger.add_to_log(
                    "info",
                    f"Apoyos: batch {(i // batch_size) + 1} (+{len(chunk)}) total={total}"
                )

            db.session.commit()
            return {"Nuevos Apoyos": total}

        except Exception as ex:
            db.session.rollback()
            Logger.add_to_log("error", f"Error bulk_insert apoyos: {ex}")
            raise

        finally:
            # ✅ IMPORTANTÍSIMO: restaurar antes de regresar la conexión al pool
            try:
                if conn is None:
                    conn = db.session.connection()
                conn.execute(text("SET SESSION foreign_key_checks = 1"))
                conn.execute(text("SET SESSION unique_checks = 1"))
                db.session.commit()
            except Exception:
                db.session.rollback()
