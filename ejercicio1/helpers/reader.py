import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("prueba_técnica")

class Reader:
    def __init__(self, spark, reader_type, path):
        self.spark = spark
        self.reader_type =  reader_type
        self.path = path


    def reader_process(self):
        try:
            if self.reader_type == "csv":
                df = self.spark.read.csv(self.path, header= True, sep = ',')
                logger.info("Lectura correcta para una ruta csv")
                df.show(5,False)
                return df
        except Exception as e:
            logger.info(f"Error al generar la lectura...{e}")
        
        