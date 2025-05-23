//COMANDOS A INTRODUCIR EN SPARK-SHELL

import org.apache.spark.sql.functions._
import spark.implicits._

val fileName = "311_2.csv"
val df = spark.read.option("header", "true").option("inferSchema", "true").csv(fileName)

val colSinEspaciosDF = df.columns.foldLeft(df) { (tempDF, colName) => tempDF.withColumnRenamed(colName, colName.replace(" ", "_"))}

val sinNulosDF = colSinEspaciosDF.na.drop(Seq("Created_Date", "Complaint_Type", "Borough"))

val timestampDF = sinNulosDF.withColumn("Created_Date", to_timestamp(col("Created_Date"), "MM/dd/yyyy hh:mm:ss a"))

val mayusDF = timestampDF.withColumn("Borough", upper(col("Borough")))

val barriosMayus = mayusDF.select("Borough").distinct()

barriosMayus.show()

val incPorBarrioYTipo = mayusDF.groupBy("Borough", "Complaint_Type").count()

incPorBarrioYTipo.orderBy(desc("count")).show(25, truncate = false)

val cleanDF = colSinEspaciosDF.na.drop(Seq("Created_Date", "Resolution_Action_Updated_Date", "Borough"))
  .withColumn("Created_Date", to_timestamp(col("Created_Date"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("Resolution_Action_Updated_Date", to_timestamp(col("Resolution_Action_Updated_Date"), "MM/dd/yyyy hh:mm:ss a"))

val timeDiffDF = cleanDF
  .withColumn("diff_seconds", unix_timestamp(col("Resolution_Action_Updated_Date")) - unix_timestamp(col("Created_Date")))
  .filter(col("diff_seconds") > 0)

val promedioDF = timeDiffDF
  .groupBy("Borough")
  .agg(avg("diff_seconds").alias("avg_seconds"))

val resultadoFormateadoDF = promedioDF
  .withColumn("dias", floor(col("avg_seconds") / 86400))
  .withColumn("horas", floor((col("avg_seconds") % 86400) / 3600))
  .withColumn("minutos", floor((col("avg_seconds") % 3600) / 60))
  .withColumn("tiempo_medio_de_resolucion",
    concat_ws(" ",
      col("dias").cast("int"), lit("dias"),
      col("horas").cast("int"), lit("horas"),
      col("minutos").cast("int"), lit("minutos")
    )
  )
  .select("Borough", "tiempo_medio_de_resolucion")
  .orderBy(desc("dias"))

resultadoFormateadoDF.show(truncate = false)
