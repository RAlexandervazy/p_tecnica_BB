from pyspark.sql import functions as F

class RawDetails:
    """
    Clase que contiene los métodos que brindan detalles generales de un insumo raw
    Métodos
    crude_stage_details() : Brinda detalles generales de un df: Registros, columnas
    """
    def __init__(self, df_raw = None, llave = []):
        """
        Constructor de la clase

        Args:
        df_raw (dataframe) : Dataframe de donde se tomará la información a mostrar.
        llave (lista) : Lista de campos que en conjunto formen la llave principal del dataframe (validación de casos unicos).
        """
        self.df_raw = df_raw
        self.llave = llave

    def crude_stage_details(self):
        """
        Método que brinda información de un dataframe como: 
        -conteo de registros
        -Numero de columnas
        -Schema raw del dataframe
        -Detalle de casos nulos por campo
        -Cantidad de registros unicos (validado acorde al campo llave)
        """
        print(f" {self.df_raw.count()} registros ") # Registros
        print(f" {len(self.df_raw.columns)} columnas ") # Columnas o conteo de columnas?

        print("Detalles de schema raw") # Detalle de schema
        self.df_raw.printSchema()

        print("Conteo de nulos por campo")
        for i in self.df_raw.columns:
            print(f"{i}:" ,self.df_raw.where(F.col(f"{i}").isNull()).count())

        print(f"""
        Registros unicos por campo/s llave)
        {self.df_raw.select(*self.llave).distinct().count()}
        """)
        
        
        
        

        