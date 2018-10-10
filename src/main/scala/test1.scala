import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession



object test1  {
  def main(args: Array[String]): Unit = {
    val filePath = getClass.getResource("/test.txt").getPath
    //val logFile = "/Users/tittasin/IdeaProjects/Spark_testing_space/src/main/resource/test.txt"
    val spark = SparkSession.builder.appName("Simple Application").master("local")getOrCreate()
    val logData = spark.read.textFile(filePath)
    println(logData.count())
    val format = new SimpleDateFormat("dd-MM-yyyy")
    println(format.format(Calendar.getInstance().getTime()))
  }
}

object test2  {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local")getOrCreate()
    import spark.implicits._
    val df1 = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
    df1.show()
  }
}

object generate_ddl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local")getOrCreate()
    val df = spark.read.parquet("/Users/tittasin/Downloads/part-00000-84a15747-a270-4495-b86e-68975bc87345.c000.snappy.parquet")

    def creatingTableDDL(tableName:String, df:DataFrame): String={
      val cols = df.dtypes
      var ddl1 = "CREATE EXTERNAL TABLE "+tableName + " ("
      //looks at the datatypes and columns names and puts them into a string
      val colCreate = (for (c <-cols) yield(c._1+" "+c._2.replace("Type",""))).mkString(", ")
      ddl1 += colCreate + ") STORED AS PARQUET LOCATION '/wherever/you/store/the/data/'"
      ddl1
    }
    val test_tableDDL=creatingTableDDL("test_table",df)
    println(test_tableDDL)
  }
}