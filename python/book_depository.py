from pyspark.sql import functions as f
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col, format_number, regexp_replace, when, split, explode , avg

# Get dataset from GCS
file_path = "gs://databricks_chanont/raw/book_depository/dataset.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)

# Drop unused column
df_clean = df.drop("categories", "description", "edition","edition-statement","for-ages","illustrations-note","image-checksum","image-path","image-url","imprint","index-date","isbn10","isbn13","lang","publication-place","rating-count","title","url","weight")

# Convert data type
#df_clean = df_clean.withColumn("id", col("id").cast("double"))
df_clean = df_clean.withColumn("dimension-x", col("dimension-x").cast("double"))
df_clean = df_clean.withColumn("dimension-y", col("dimension-y").cast("double"))
df_clean = df_clean.withColumn("dimension-z", col("dimension-z").cast("double"))
df_clean = df_clean.withColumn("rating-avg", col("rating-avg").cast("double"))
df_clean = df_clean.withColumn("publication-date",
                        f.to_timestamp(col("publication-date"), 'dd/MM/yyyy HH:mm')
                        )

# Drop missing values
df_clean = df_clean.dropna(subset=["authors", "bestsellers-rank","dimension-x","dimension-y","dimension-z","format"])

# Clean thd id column

# Convert values ​​in column from String to Double.
df_id_converted = df_clean.withColumn("id_double", col("id").cast("double"))

# Convert Double back to String using format_number
df_no_commas = df_id_converted.withColumn("id_string", format_number("id_double", 0))

# Remove commas
df_final_id = df_no_commas.withColumn("id_string", regexp_replace("id_string", ",", ""))

# Drop duplicate columns
df_clean = df_final_id.drop("id","id_double").withColumnRenamed('id_string', 'id')

# Filter only 13 numbers 0-9 characters
df_clean = df_clean.filter(df_clean["id"].rlike("^[0-9]{13}"))

# Filter numbers 0-9 up to 10 characters
df_clean = df_clean.filter(df_clean["bestsellers-rank"].rlike("^[0-9]{1,10}"))
#df_incorrect_userid = df_clean.subtract(df_correct_rank)
#df_incorrect_userid.show(10)

# Clean thd publication-date column

# Replace null values ​​with 1000-01-01
df_clean = df_clean.withColumn("publication-date", when(col("publication-date").isNull(), '1000-01-01').otherwise(col("publication-date")))

# Clean thd rating-avg column

# Delete Outliers
df_clean = df_clean.filter(df_clean['rating-avg'] < '50')

# Get avg of The rating-avg column
average_rating = df_clean.select(avg("rating-avg")).collect()[0][0]

# Replace null values ​​with average_rating
df_clean = df_clean.withColumn("rating-avg", when(col("rating-avg").isNull(), average_rating).otherwise(col("rating-avg")))

# Clean thd authors column
df_clean_authors = df_clean.withColumn("authors_clean", regexp_replace("authors", "[\\[\\]]", ""))
df_clean_authors_split = df_clean_authors.withColumn("authors_array", split(col("authors_clean"), ", "))
df_exploded_authors = df_clean_authors_split.withColumn("author_exploded", explode(col("authors_array")))
df_clean = df_exploded_authors.drop("authors_clean","authors_array")

# JOIN DATA 

# Get data from GCS
formats_file_path = "gs://databricks_chanont/raw/book_depository/formats.csv"
authors_file_path = "gs://databricks_chanont/raw/book_depository/authors.csv"

df_format = spark.read.csv(formats_file_path, header=True, inferSchema=True)
df_author = spark.read.csv(authors_file_path, header=True, inferSchema=True)

df_author = df_author.dropna(subset=["author_id","author_name"])
df_format = df_format.dropna(subset=["format_id","format_name"])

df_joined = df_clean.join(df_format,df_clean["format"] == df_format["format_id"], how="inner")
df_final_joined = df_joined.join(df_author,df_joined["author_exploded"] == df_author["author_id"], how="inner")

# Drop duplicate columns
df_final_joined = df_final_joined.drop("authors_clean","authors_array","format")

# Save & Upload to GCS
output_path = "gs://databricks_chanont/base/book_depository.csv"
df_final_joined.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", "|").csv(output_path)
