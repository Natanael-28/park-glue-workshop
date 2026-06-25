# scripts/job_silver_to_gold.py
# Glue Job: Silver → Gold
# Lee el Parquet limpio de Silver y construye un modelo estrella:
#   - dim_product   (stock_code, description, product_sk)
#   - dim_customer  (customer_id, country, customer_sk)
#   - dim_date      (date, year, month, day, weekday, date_sk)
#   - fact_sales    (invoice, product_sk, customer_sk, date_sk,
#                    quantity, unit_price, total_amount, is_return)

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Inicialización ──────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
logger = glue_context.get_logger()

SILVER_PATH = f"s3://{BUCKET}/silver/"
GOLD_PATH   = f"s3://{BUCKET}/gold/"

logger.info(f"STEP 0 — Job Silver→Gold iniciado. Bucket: {BUCKET}")

try:
    # ── STEP 1: Leer Silver ─────────────────────────────────────────────────
    logger.info("STEP 1 — Leyendo Parquet desde Silver...")
    df = spark.read.parquet(SILVER_PATH)
    total = df.count()
    logger.info(f"STEP 1 — Filas leídas: {total:,}")

except Exception as e:
    logger.error(f"STEP 1 — Error leyendo Silver: {e}")
    raise

try:
    # ── STEP 2: Dimensión Producto ──────────────────────────────────────────
    logger.info("STEP 2 — Construyendo dim_product...")
    dim_product = (
        df
        .select("stock_code", "description")
        .dropDuplicates(["stock_code"])
        .withColumn(
            "product_sk",
            F.row_number().over(Window.orderBy("stock_code"))
        )
    )
    product_count = dim_product.count()
    logger.info(f"STEP 2 — dim_product: {product_count:,} productos únicos.")

    dim_product.write.mode("overwrite").parquet(f"{GOLD_PATH}dim_product/")
    logger.info("STEP 2 — dim_product escrita.")

except Exception as e:
    logger.error(f"STEP 2 — Error en dim_product: {e}")
    raise

try:
    # ── STEP 3: Dimensión Cliente ───────────────────────────────────────────
    logger.info("STEP 3 — Construyendo dim_customer...")
    dim_customer = (
        df
        .select("customer_id", "country")
        .dropDuplicates(["customer_id"])
        .withColumn(
            "customer_sk",
            F.row_number().over(Window.orderBy("customer_id"))
        )
    )
    customer_count = dim_customer.count()
    logger.info(f"STEP 3 — dim_customer: {customer_count:,} clientes únicos.")

    dim_customer.write.mode("overwrite").parquet(f"{GOLD_PATH}dim_customer/")
    logger.info("STEP 3 — dim_customer escrita.")

except Exception as e:
    logger.error(f"STEP 3 — Error en dim_customer: {e}")
    raise

try:
    # ── STEP 4: Dimensión Fecha ─────────────────────────────────────────────
    logger.info("STEP 4 — Construyendo dim_date...")
    dim_date = (
        df
        .select(F.to_date("invoice_date").alias("date"))
        .dropDuplicates(["date"])
        .withColumn("year",    F.year("date"))
        .withColumn("month",   F.month("date"))
        .withColumn("day",     F.dayofmonth("date"))
        .withColumn("weekday", F.date_format("date", "EEEE"))
        .withColumn(
            "date_sk",
            F.date_format("date", "yyyyMMdd").cast("int")
        )
    )
    date_count = dim_date.count()
    logger.info(f"STEP 4 — dim_date: {date_count:,} fechas únicas.")

    dim_date.write.mode("overwrite").parquet(f"{GOLD_PATH}dim_date/")
    logger.info("STEP 4 — dim_date escrita.")

except Exception as e:
    logger.error(f"STEP 4 — Error en dim_date: {e}")
    raise

try:
    # ── STEP 5: Tabla de hechos ─────────────────────────────────────────────
    logger.info("STEP 5 — Construyendo fact_sales con broadcast joins...")

    # Preparar la columna de fecha en el DataFrame principal
    df_with_date = df.withColumn("sale_date", F.to_date("invoice_date"))

    # Broadcast join con dim_product
    fact = df_with_date.join(
        F.broadcast(dim_product.select("stock_code", "product_sk")),
        on="stock_code",
        how="left"
    )

    # Broadcast join con dim_customer
    fact = fact.join(
        F.broadcast(dim_customer.select("customer_id", "customer_sk")),
        on="customer_id",
        how="left"
    )

    # Broadcast join con dim_date
    fact = fact.join(
        F.broadcast(dim_date.select(F.col("date").alias("sale_date"), "date_sk")),
        on="sale_date",
        how="left"
    )

    # Seleccionar columnas finales
    fact_sales = fact.select(
        F.col("invoice"),
        F.col("product_sk"),
        F.col("customer_sk"),
        F.col("date_sk"),
        F.col("quantity"),
        F.col("price").alias("unit_price"),
        F.col("total_amount"),
        F.col("is_return"),
    )

    fact_count = fact_sales.count()
    logger.info(f"STEP 5 — fact_sales: {fact_count:,} filas.")

    fact_sales.write.mode("overwrite").parquet(f"{GOLD_PATH}fact_sales/")
    logger.info("STEP 5 — fact_sales escrita.")

except Exception as e:
    logger.error(f"STEP 5 — Error en fact_sales: {e}")
    raise

logger.info("Pipeline Silver → Gold completado exitosamente.")
logger.info(f"  dim_product:  {product_count:,} filas")
logger.info(f"  dim_customer: {customer_count:,} filas")
logger.info(f"  dim_date:     {date_count:,} filas")
logger.info(f"  fact_sales:   {fact_count:,} filas")
