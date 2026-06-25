# scripts/job_bronze_to_silver.py
# Glue Job: Bronze → Silver
# Lee los CSV crudos de la capa Bronze, limpia, tipifica y guarda como Parquet
# particionado por año en la capa Silver.

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

# ── Inicialización ──────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
logger = glue_context.get_logger()

INPUT_PATH  = f"s3://{BUCKET}/bronze/"
OUTPUT_PATH = f"s3://{BUCKET}/silver/"

logger.info(f"STEP 0 — Job iniciado. Bucket: {BUCKET}")

try:
    # ── STEP 1: Leer CSV crudos ─────────────────────────────────────────────
    logger.info("STEP 1 — Leyendo CSV desde Bronze...")

    schema = StructType([
        StructField("Invoice",     StringType(),    True),
        StructField("StockCode",   StringType(),    True),
        StructField("Description", StringType(),    True),
        StructField("Quantity",    IntegerType(),   True),
        StructField("InvoiceDate", StringType(),    True),
        StructField("Price",       DoubleType(),    True),
        StructField("Customer ID", DoubleType(),    True),
        StructField("Country",     StringType(),    True),
    ])

    df = spark.read.csv(INPUT_PATH, header=True, schema=schema)
    raw_count = df.count()
    logger.info(f"STEP 1 — Filas leídas: {raw_count:,}")

except Exception as e:
    logger.error(f"STEP 1 — Error leyendo CSV: {e}")
    raise

try:
    # ── STEP 2: Eliminar filas sin Invoice o StockCode ──────────────────────
    logger.info("STEP 2 — Eliminando filas sin Invoice o StockCode...")
    df = df.dropna(subset=["Invoice", "StockCode"])
    after_drop = df.count()
    logger.info(f"STEP 2 — Filas restantes: {after_drop:,} (eliminadas: {raw_count - after_drop:,})")

except Exception as e:
    logger.error(f"STEP 2 — Error en limpieza de nulos: {e}")
    raise

try:
    # ── STEP 3: Castear tipos ───────────────────────────────────────────────
    logger.info("STEP 3 — Casteando tipos de columnas...")
    df = (
        df
        .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
        .withColumn("Customer ID", F.col("Customer ID").cast(IntegerType()))
        .withColumnRenamed("Customer ID", "customer_id")
        .withColumnRenamed("Invoice",     "invoice")
        .withColumnRenamed("StockCode",   "stock_code")
        .withColumnRenamed("Description", "description")
        .withColumnRenamed("Quantity",    "quantity")
        .withColumnRenamed("InvoiceDate", "invoice_date")
        .withColumnRenamed("Price",       "price")
        .withColumnRenamed("Country",     "country")
    )
    logger.info("STEP 3 — Tipos casteados y columnas renombradas a snake_case.")

except Exception as e:
    logger.error(f"STEP 3 — Error casteando tipos: {e}")
    raise

try:
    # ── STEP 4: Trim de strings ─────────────────────────────────────────────
    logger.info("STEP 4 — Aplicando trim a columnas de texto...")
    for col_name in ["invoice", "stock_code", "description", "country"]:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    logger.info("STEP 4 — Trim aplicado.")

except Exception as e:
    logger.error(f"STEP 4 — Error en trim: {e}")
    raise

try:
    # ── STEP 5: Marcar devoluciones ─────────────────────────────────────────
    logger.info("STEP 5 — Marcando devoluciones (invoice empieza con 'C')...")
    df = df.withColumn("is_return", F.col("invoice").startswith("C"))
    returns_count = df.filter(F.col("is_return")).count()
    logger.info(f"STEP 5 — Devoluciones detectadas: {returns_count:,}")

except Exception as e:
    logger.error(f"STEP 5 — Error marcando devoluciones: {e}")
    raise

try:
    # ── STEP 6: Calcular total_amount ───────────────────────────────────────
    logger.info("STEP 6 — Calculando total_amount = quantity * price...")
    df = df.withColumn("total_amount", F.round(F.col("quantity") * F.col("price"), 2))
    logger.info("STEP 6 — Columna total_amount creada.")

except Exception as e:
    logger.error(f"STEP 6 — Error calculando total_amount: {e}")
    raise

try:
    # ── STEP 7: Extraer año y escribir Parquet particionado ─────────────────
    logger.info("STEP 7 — Extrayendo año y escribiendo Parquet en Silver...")
    df = df.withColumn("year", F.year("invoice_date"))

    # Eliminar filas donde invoice_date sea null (no se pudo parsear)
    df = df.filter(F.col("invoice_date").isNotNull())

    final_count = df.count()
    logger.info(f"STEP 7 — Filas finales a escribir: {final_count:,}")

    (
        df
        .repartition("year")
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(OUTPUT_PATH)
    )

    logger.info(f"STEP 7 — Parquet escrito en {OUTPUT_PATH}")
    logger.info("Pipeline Bronze → Silver completado exitosamente.")

except Exception as e:
    logger.error(f"STEP 7 — Error escribiendo Parquet: {e}")
    raise
