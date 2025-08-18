class Mapeo():
    def map_row(id_value:str,row:dict,colums:dict):
        mapped = {colums[k]: row.get(k) for k in colums.keys() if k in row}
        return { "id": id_value, **mapped }