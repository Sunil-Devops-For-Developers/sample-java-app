package ScalaProgramme
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{expr, col, column}

object CH5_BasicStructuredOperation {
  def main(args:Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
                  .builder()
                  .appName("BasicStructuredOperation")
                  .master("local[*]")
                  .config("spark.sql.warehouse.dir", "file:///tmp")
                  .getOrCreate()
    //val df = spark.read.json("G:/Study/Spark/Best Books/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json")
    //df.printSchema()
                  val myManualSchema = new StructType(Array(
new StructField("DEST_COUNTRY_NAME", StringType, true),
new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
new StructField("count", LongType, false
//Metadata.fromJson("{\"hello\":\"world\"}")
)
))
  val df = spark.read.format("json").schema(myManualSchema).load("G:/Study/Spark/Best Books/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json")
  //df.printSchema()
  val myManualSchema1 = new StructType(Array(
  new StructField("some", StringType, true),
  new StructField("col", StringType, true),
  new StructField("names", LongType, false)))
  val myRows = Seq(Row("Hello", null, 1L))
  val myRDD = spark.sparkContext.parallelize(myRows)
  val myDf = spark.createDataFrame(myRDD, myManualSchema1)
  System.out.println("Number of partitions : "+ myRDD.getNumPartitions);
  //myDf.show()
  //myDf.printSchema()
  
  //df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show()
  //df.show()
  import org.apache.spark.sql.functions.lit
  df.select(expr("*"), lit(1).as("One")).show()
//  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)
//  df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).show()
//  df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").show()
  //df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
  val schema = df.schema
val newRows = Seq(
Row("New Country", "Other Country", 5L),
Row("New Country 2", "Other Country 3", 1L)
)
val parallelizedRows = spark.sparkContext.parallelize(newRows)
val newDF = spark.createDataFrame(parallelizedRows, schema)
df.union(newDF)
.where("count >= 1")
.filter(col("ORIGIN_COUNTRY_NAME") =!= "United States")
.show(1000) // get all of them and we'll see our new rows at the end
  }
  
}