from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator



spark = SparkSession.builder \
    .appName("ThreatDetectionML") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

print("[INFO] Loading data from HDFS...")




df = spark.read.option("header", True).csv(
    "hdfs://hadoop-master:9000/data/cybersecurity/logs/*/*/*/*.csv"
)

df = df.dropna()

# Convert numeric column
df = df.withColumn(
    "bytes_transferred",
    col("bytes_transferred").cast("double")
)

df = df.limit(100000)

print("[INFO] Dataset loaded")
print("[INFO] Columns:", df.columns)


print("[INFO] Threat label distribution:")
df.groupBy("threat_label").count().show()


protocol_indexer = StringIndexer(
    inputCol="protocol",
    outputCol="protocol_index",
    handleInvalid="keep"
)

action_indexer = StringIndexer(
    inputCol="action",
    outputCol="action_index",
    handleInvalid="keep"
)

label_indexer = StringIndexer(
    inputCol="threat_label",
    outputCol="label",
    handleInvalid="keep"
)

assembler = VectorAssembler(
    inputCols=[
        "protocol_index",
        "action_index",
        "bytes_transferred"
    ],
    outputCol="features"
)



train, test = df.randomSplit([0.8, 0.2], seed=42)



temp_pipeline = Pipeline(stages=[
    protocol_indexer,
    action_indexer,
    label_indexer,
    assembler
])

temp_model = temp_pipeline.fit(train)

train_prepared = temp_model.transform(train)
test_prepared = temp_model.transform(test)



train_prepared = train_prepared.withColumn(
    "classWeight",
    when(col("label") == 0, 1.0)      # benign
    .when(col("label") == 1, 8.0)     # suspicious
    .otherwise(15.0)                  # malicious
)



rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    weightCol="classWeight",
    numTrees=100,
    maxDepth=12
)

print("[INFO] Training model...")

model = rf.fit(train_prepared)

predictions = model.transform(test_prepared)



accuracy_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

f1_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

precision_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

recall_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedRecall"
)

accuracy = accuracy_eval.evaluate(predictions)
f1 = f1_eval.evaluate(predictions)
precision = precision_eval.evaluate(predictions)
recall = recall_eval.evaluate(predictions)


print("\n Model trained successfully!")

print(f"Accuracy:  {accuracy:.4f}")
print(f" F1 Score:  {f1:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall:    {recall:.4f}")

# Show prediction distribution
print("\n[INFO] Prediction distribution:")
predictions.groupBy("prediction").count().show()

# Confusion-style view
print("\n[INFO] Label vs Prediction:")
predictions.groupBy(
    "threat_label",
    "prediction"
).count().show()

# Sample predictions
print("\n[INFO] Sample predictions:")

predictions.select(
    "protocol",
    "action",
    "bytes_transferred",
    "threat_label",
    "prediction"
).show(20, truncate=False)

pipeline_model_path = "hdfs://hadoop-master:9000/models/cybersecurity_rf_model"

print("[INFO] Saving FULL pipeline model...")

model.write().overwrite().save(pipeline_model_path)

print("[INFO] Full PipelineModel saved to HDFS")



spark.stop()
