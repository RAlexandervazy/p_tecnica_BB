from pyspark.sql import functions as F
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("prueba_técnica")

class DataCleanerAggregator:
    def __init__(self, df=None):
        self.df = df

    def final_sales_details(self):
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