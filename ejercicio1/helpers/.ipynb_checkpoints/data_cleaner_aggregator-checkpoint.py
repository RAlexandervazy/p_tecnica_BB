from pyspark.sql import functions as F
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("prueba_técnica")

class DataCleanerAggregator:
    """
    Clase que contiene las transformaciones necesarias para generar un dataframe final que cumpla
    con las reglas de negocio requeridas. 

    Métodos
    final_sales_details() : Selecciona unicamente los campos necesarios para el consumo de la información,
                            genera los casteos necesarios para estandarizar la información y genera el campo
                            con detalles de venta por cliente.
    """
    def __init__(self, df=None):
        """
        Constructor de la clase

        Args
        df (dataframe) : dataframe al que se le aplicarán las reglas de negocio.  
        """
        self.df = df

    def final_sales_details(self):
        """
        Método que permite mantener unicamente los campos escenciales, darles un casteo definido
        así como la aplicación de agrupaciones o transformaciones necesarias para obtener el 
        número de ventas por día por cliente. 
        """
        try:
            self.df = (
                self.df.withColumn(
                    "purchase_date",
                    F.when(
                        F.col("created_at").rlike(r"^\d{8}$"),
                        F.to_date(F.col("created_at"), "yyyyMMdd"),
                    )
                    .when(
                        F.col("created_at").rlike(r"^\d{4}-\d{2}-\d{2}T.*"),
                        F.to_date(
                            F.substring(F.col("created_at"), 1, 10), "yyyy-MM-dd"
                        ),
                    )
                    .when(
                        F.col("created_at").rlike(r"^\d{4}-\d{2}-\d{2}$"),
                        F.to_date(F.col("created_at"), "yyyy-MM-dd"),
                    )
                    .otherwise(None),
                )
                .selectExpr(
                    "CAST(name as string) as cliente_name",
                    "purchase_date",
                    "CAST(amount as double) as sale_amount",
                )
                .groupBy("cliente_name", "purchase_date")
                .agg(F.sum("sale_amount").alias("total_sales_amount"))
            ).orderBy("purchase_date")
            logger.info(f" Se ha generado el DF con los detalles de venta por cliente con {self.df.count()} registros")
            
        except Exception as e:
            logger.info(" No fue posible generar el dataframe con el detalle de ventas...")
        return self.df