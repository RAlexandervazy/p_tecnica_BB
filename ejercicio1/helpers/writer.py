import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("prueba_técnica")

class Writer:
    def __init__(self, df = None):
        self.df = df

    def processed_df(self):
        self.df.coalesce(1).write.mode("overwrite").parquet("processed")
        return print("El df ha sido escrito correctamente en la carpeta 'processed'")
