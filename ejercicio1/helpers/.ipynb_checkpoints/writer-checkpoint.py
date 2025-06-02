import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("prueba_técnica")

class Writer:
    """
    Clase que contiene los diferentes métodos de escritura

    Metodos
    processed_df() : Escribe el DataFrame en formato Parquet.
    """
    def __init__(self):
        """
        Constructor de la clase

        """
    def processed_df(self, df, path):
        """
        Método que contiene la instrucción de escritura, tomará el DF recibido por el constructor
        y lo escribirá en la carpeta path.

        Args:
        df (dataframe): Dataframe a escribir.
        path (string) : Ruta en donde será escrito el dataframe
        """
        df.coalesce(1).write.mode("overwrite").parquet(path)
        return print(f"El df ha sido escrito correctamente en la ruta {path}")
