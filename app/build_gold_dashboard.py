"""Prépare deux tables Delta Gold pour les nouvelles vues géographiques Streamlit.
À lancer dans le même environnement Spark/Delta/HDFS que les notebooks J3.
Les tables Gold existantes et Silver ne sont jamais modifiées.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "/lakehouse/silver/flights_enriched"
AIRPORT_PATH = "/lakehouse/gold/flights_airport_daily_map"
ROUTES_PATH = "/lakehouse/gold/flights_routes_daily_map"

spark = SparkSession.builder.appName("J3_Gold_Geo_Dashboard").getOrCreate()
silver = spark.read.format("delta").load(SILVER_PATH)
required = {
    "flight_date",
    "origin_airport",
    "destination_airport",
    "cancelled",
    "arrival_delay",
    "origin_latitude",
    "origin_longitude",
    "destination_latitude",
    "destination_longitude",
}
missing = required - set(silver.columns)
if missing:
    raise ValueError(
        f"Colonnes manquantes dans Silver : {sorted(missing)}. Vérifier le schéma des coordonnées."
    )

# Conserver une ligne par vol (la couche Silver est déjà dédoublonnée).
base = (
    silver.select(
        "flight_date",
        "origin_airport",
        "destination_airport",
        "cancelled",
        "arrival_delay",
        "origin_latitude",
        "origin_longitude",
        "destination_latitude",
        "destination_longitude",
    )
    .withColumn("observed", ((F.col("cancelled") == 0) & F.col("arrival_delay").isNotNull()).cast("long"))
    .withColumn("delayed", ((F.col("cancelled") == 0) & (F.col("arrival_delay") >= 15)).cast("long"))
)

# Les coordonnées du référentiel doivent être uniques et cohérentes par aéroport.
# Un regroupement de coordonnées évite les jointures N-à-M.
airports = (
    base.groupBy("flight_date", "origin_airport")
    .agg(
        F.count("*").alias("scheduled_flights"),
        F.sum("observed").alias("observed_flights"),
        F.sum("delayed").alias("delayed_flights"),
        F.sum((F.col("cancelled") == 1).cast("long")).alias("cancelled_flights"),
        F.first("origin_latitude", ignorenulls=True).cast("double").alias("latitude"),
        F.first("origin_longitude", ignorenulls=True).cast("double").alias("longitude"),
    )
    .withColumn(
        "delay_rate",
        F.when(F.col("observed_flights") > 0, 100.0 * F.col("delayed_flights") / F.col("observed_flights")),
    )
    .filter(F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180))
)

routes = (
    base.groupBy("flight_date", "origin_airport", "destination_airport")
    .agg(
        F.count("*").alias("scheduled_flights"),
        F.sum("observed").alias("observed_flights"),
        F.sum("delayed").alias("delayed_flights"),
        F.first("origin_latitude", ignorenulls=True).cast("double").alias("origin_latitude"),
        F.first("origin_longitude", ignorenulls=True).cast("double").alias("origin_longitude"),
        F.first("destination_latitude", ignorenulls=True).cast("double").alias("destination_latitude"),
        F.first("destination_longitude", ignorenulls=True).cast("double").alias("destination_longitude"),
    )
    .withColumn(
        "delay_rate",
        F.when(F.col("observed_flights") > 0, 100.0 * F.col("delayed_flights") / F.col("observed_flights")),
    )
    .filter(
        F.col("origin_latitude").between(-90, 90)
        & F.col("origin_longitude").between(-180, 180)
        & F.col("destination_latitude").between(-90, 90)
        & F.col("destination_longitude").between(-180, 180)
    )
)

for dataframe, path in [(airports, AIRPORT_PATH), (routes, ROUTES_PATH)]:
    dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    print(f"{path}: {spark.read.format('delta').load(path).count():,} lignes")

print("Tables géographiques Gold prêtes. Silver et autres Gold inchangées.")
