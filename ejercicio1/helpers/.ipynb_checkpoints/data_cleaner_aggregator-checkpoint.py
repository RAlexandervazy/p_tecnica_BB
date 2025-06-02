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

    company_clasification_by_stddev() : Selecciona unicamente los campos necesarios para calcular el performance
                                        por comportamiento de pagos para cada una de las empresas.
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
            df = (
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
            logger.info(f" Se ha generado el DF con los detalles de venta por cliente con {df.count()} registros")
            
        except Exception as e:
            logger.info(" No fue posible generar el dataframe con el detalle de ventas...")
        return df


    def company_clasification_by_stddev(self):
        """
        Método que clásifica a las empresas de acuerdo al comportamiento de sus pagos, 
        medido acorde a la desviación estandar.
        """
        try:
            df = (
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
                ).withColumn(
                    "pay_date",
                    F.when(
                        F.col("paid_at").rlike(r"^\d{8}$"),
                        F.to_date(F.col("paid_at"), "yyyyMMdd"),
                    )
                    .when(
                        F.col("paid_at").rlike(r"^\d{4}-\d{2}-\d{2}T.*"),
                        F.to_date(
                            F.substring(F.col("paid_at"), 1, 10), "yyyy-MM-dd"
                        ),
                    )
                    .when(
                        F.col("paid_at").rlike(r"^\d{4}-\d{2}-\d{2}$"),
                        F.to_date(F.col("paid_at"), "yyyy-MM-dd"),
                    )
                    .otherwise(None),
                ).withColumn(
                    "payment_delay_days",
                    F.datediff(F.col("pay_date"), F.col("purchase_date"))
                ).selectExpr(
                    "CAST(company_id as string) as company_id",
                    "CAST(status as string) as status",
                    "purchase_date",
                    "pay_date",
                    "payment_delay_days",
                ).where("status = 'paid'")).groupBy("company_id").agg(
                    F.stddev("payment_delay_days").alias("stddev_days_to_pay")
                ).select(
                    "company_id",
                    "stddev_days_to_pay",
                    F.when(F.col("stddev_days_to_pay") < 10, "Alta Regularidad")
                    .when((F.col("stddev_days_to_pay") >= 10) | (F.col("stddev_days_to_pay") <= 30), "Moderada Regularidad")
                    .when(F.col("stddev_days_to_pay") > 30, "Irregular")
                    .otherwise("No clasificable").alias("payment_pattern")
                )
            logger.info(f" Se ha generado el DF con la clasificación de empresas {df.count()} registros")
            
        except Exception as e:
            logger.info(" No fue posible generar el dataframe con la clasificación de compañias...")
        return df