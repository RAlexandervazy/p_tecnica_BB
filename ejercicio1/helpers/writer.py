import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("prueba_técnica")

class Writer:
    """
    Clase que contiene los diferentes métodos de escritura

    Metodos
    processed_df() : Escribe el DataFrame en formato Parquet.
    """
    def __init__(self, df = None):
        """
        Constructor de la clase
        
        Args:
        -------
        df (dataframe): Dataframe a escribir.
        
        """
        self.df = df

    def processed_df(self):
        """
        Método que contiene la instrucción de escritura, tomará el DF recibido por el constructor
        y lo escribirá en la carpeta processed.
        """
        self.df.coalesce(1).write.mode("overwrite").parquet("processed")
        return print("El df ha sido escrito correctamente en la carpeta 'processed'")
