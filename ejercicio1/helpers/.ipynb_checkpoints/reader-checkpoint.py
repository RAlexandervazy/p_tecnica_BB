import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("prueba_técnica")

class Reader:
    """
    Clase que contiene los métodos para diferentes tipos de lectura.
    Métodos
    reader_process() : Realiza la léctura de un insumo en formato csv
                       se establece por default header = True  sep = ','
    """
    def __init__(self, spark, reader_type, path):
        """
        Constructor de la clase

        Args
        spark : Sesión de spark.
        reader_type (string): casuística que define el tipo de lectura que será realizada. 
        path (string): Cadena de texto correspondiente a la ruta en donde se encuentra el archivo que será leido.
        """
        self.spark = spark
        self.reader_type =  reader_type
        self.path = path


    def reader_process(self):
        """
        Método que contiene la lógica necesaria para las diferentes casuisticas de lectura mediante
        la validación de los atributos: reader_type y path. 
        """
        try:
            if self.reader_type == "csv":
                df = self.spark.read.csv(self.path, header= True, sep = ',')
                logger.info("Lectura correcta para una ruta csv")
                df.show(5,False)
                return df
        except Exception as e:
            logger.info(f"Error al generar la lectura...{e}")
        
        