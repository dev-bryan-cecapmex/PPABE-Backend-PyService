class Mapeo():
    def map_row(columns_map, datos, messy_dictionary ):
        _datos = [
            {columns_map.get(k,k) : v for k, v in datos.items()}
            for datos in messy_dictionary
        ]
        return _datos