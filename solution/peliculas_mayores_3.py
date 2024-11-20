from pyspark.sql import SparkSession

# Usamos el SparkSession preexistente de Databricks
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Cargamos el archivo de ratings
ratings_rdd = sc.textFile("/FileStore/ratings.txt")

# Paso 1: Dividir las líneas por "::" y seleccionar el id de la película y la calificación
ratings_rdd = ratings_rdd.map(lambda x: x.split("::"))

# Paso 2: Crear un RDD de tuplas (película_id, calificación)
movie_ratings_rdd = ratings_rdd.map(lambda x: (int(x[0]), float(x[2])))

# Paso 3: Sumar las calificaciones y contar las votaciones por película
sum_ratings = movie_ratings_rdd.reduceByKey(lambda x, y: x + y)
count_ratings = movie_ratings_rdd.mapValues(lambda x: 1).reduceByKey(lambda x, y: x + y)

# Paso 4: Unir las sumas y los conteos para calcular la calificación media
average_ratings = sum_ratings.join(count_ratings).mapValues(lambda x: x[0] / x[1])

# Paso 5: Filtrar las películas cuya calificación media sea superior a 3
movies_above_3 = average_ratings.filter(lambda x: x[1] > 3)

# Paso 6: Mostrar las películas cuya calificación media es superior a 3
print("Películas con calificación media superior a 3:")
for movie, avg_rating in movies_above_3.collect():
    print(f"Película {movie}: {avg_rating:.2f}")
