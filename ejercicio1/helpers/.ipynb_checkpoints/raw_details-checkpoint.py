from pyspark.sql import functions as F

class RawDetails:
    """
    
    
    """
    def __init__(self, df_raw = None, llave = []):
        self.df_raw = df_raw
        self.llave = llave

    def crude_stage_details(self):
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
        
        
        
        

        