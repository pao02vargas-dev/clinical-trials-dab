# =====================================================================
# transformations.py — SDP Medallion: Clinical Trials & Biotech Pharma
# Paola Vargas — Databricks Free Edition (serverless)
# Python (no SQL) porque IDENTIFIER() no es soportado dentro de STREAM()
# ni en AUTO CDC; con spark.conf.get() + f-strings es 100% parametrizable.
# =====================================================================
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

CATALOG     = spark.conf.get("catalog")
SCHEMA      = spark.conf.get("schema")
FACT_SOURCE = spark.conf.get("fact_source_table")

def t(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"

# =====================================================================
# BRONZE — ingesta cruda
# =====================================================================
@dlt.table(
    name=t("trials_bronze"),
    comment="Bronze: hecho de ensayos clínicos (Marketplace, solo lectura)",
)
def trials_bronze():
    return (
        spark.read.table(FACT_SOURCE)
        .withColumn("_ingested_at", F.current_timestamp())
    )

SPONSOR_CDC_SCHEMA = StructType([
    StructField("sponsor_id",           StringType()),
    StructField("sponsor_name",         StringType()),
    StructField("country",              StringType()),
    StructField("certification_level",  StringType()),
    StructField("updated_at",          TimestampType()),
    StructField("operation_type",      StringType()),
])

@dlt.table(
    name=t("research_sponsors_bronze"),
    comment="Bronze: feed CDC de patrocinadores (Auto Loader JSON sobre volume cdc_source)",
)
def research_sponsors_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SPONSOR_CDC_SCHEMA)
        .load(f"/Volumes/{CATALOG}/{SCHEMA}/cdc_source")
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )

# =====================================================================
# SILVER — hecho tipado/normalizado + dimensión SCD Type 2
# EXPECTATIONS (justificación):
#  * expect_or_fail "valid_trial_id": clave crítica del ensayo; si llega
#    nula el dato no es confiable y el pipeline se detiene (fail-fast).
#  * expect (WARN) "positive_duration": duraciones <= 0 son anomalías
#    que se reportan pero NO se descartan (aportan valor analítico).
#  * expect_or_drop "valid_sponsor": sin Applicant no se puede unir a la
#    dimensión de certificación; se descarta y queda contabilizado.
# =====================================================================
@dlt.expect_or_fail("valid_trial_id", "trial_id IS NOT NULL")
@dlt.expect("positive_duration", "duration_days > 0")
@dlt.expect_or_drop("valid_sponsor", "sponsor_name IS NOT NULL")
@dlt.table(
    name=t("trials_silver"),
    comment="Silver: ensayos clínicos tipados, normalizados y con columna derivada duration_days",
)
def trials_silver():
    src = dlt.read(t("trials_bronze"))
    def to_date_safe(c):
        # Fechas string; tolera ISO (yyyy-MM-dd) y MM/dd/yyyy
        return F.coalesce(F.to_date(F.col(c)), F.to_date(F.col(c), "MM/dd/yyyy"))
    return (
        src.select(
            F.coalesce(F.trim(F.col("Number")), F.col("ID")).cast("string").alias("trial_id"),
            F.trim(F.col("Applicant")).alias("sponsor_name"),
            F.upper(F.trim(F.col("Document_Type"))).alias("phase"),   # PMR / PMC
            F.upper(F.trim(F.col("Status"))).alias("status"),
            F.trim(F.col("Product")).alias("product"),
            F.trim(F.col("NDA_Number")).alias("nda_number"),
            F.upper(F.trim(F.col("CDER_or_CBER"))).alias("fda_center"),
            to_date_safe("NDA_BLA_Approval_Date").alias("start_date"),
            to_date_safe("Final_Report_Due_Date").alias("end_date"),
            F.datediff(to_date_safe("Final_Report_Due_Date"),
                       to_date_safe("NDA_BLA_Approval_Date")).alias("duration_days"),
        )
    )

# Dimensión SCD Type 2 con AUTO CDC (equivalente SQL: "FLOW AUTO CDC ...
# KEYS ... SEQUENCE BY ... STORED AS SCD TYPE 2").
def build_research_sponsors_scd2():
    dlt.create_streaming_table(
        name=t("research_sponsors_scd2"),
        comment="Silver: dimensión Research Sponsors SCD Type 2 (historia completa)",
        expect_all_or_drop={
            "valid_sponsor_key": "sponsor_id IS NOT NULL",
            "valid_event_ts":    "updated_at IS NOT NULL",
        },
    )
    dlt.create_auto_cdc_flow(
        target=t("research_sponsors_scd2"),
        source=t("research_sponsors_bronze"),
        keys=["sponsor_id"],
        sequence_by=F.col("updated_at"),
        apply_as_deletes=F.expr("operation_type = 'DELETE'"),
        except_column_list=["operation_type", "_source_file"],
        stored_as_scd_type=2,
    )

build_research_sponsors_scd2()

# =====================================================================
# GOLD — duración media y número de ensayos por fase (tipo de ensayo)
# y nivel de certificación del patrocinador, con la versión VIGENTE SCD2.
# Nota: el dataset FDA no reporta participantes; num_trials sustituye
# la métrica de volumen.
# =====================================================================
@dlt.table(
    name=t("trials_gold"),
    comment="Gold: duración media y número de ensayos por fase x nivel de certificación",
)
def trials_gold():
    fact = spark.read.table(t("trials_silver"))
    dim_current = (
        spark.read.table(t("research_sponsors_scd2"))
        .where(F.col("__END_AT").isNull())      # patrocinador vigente
    )
    return (
        fact.join(dim_current, on="sponsor_name", how="left")
        .groupBy("phase", "certification_level")
        .agg(
            F.round(F.avg("duration_days"), 1).alias("avg_duration_days"),
            F.countDistinct("trial_id").alias("num_trials"),
        )
    )
