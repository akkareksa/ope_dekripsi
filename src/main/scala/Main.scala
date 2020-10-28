import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, row_number}
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("Dekripsi Big Data")
    val sc = new SparkContext(conf)
    //    val cipherPath = "C:/input/key15/ciphertext11.csv"
    //    val keyPath = "C:/input/key15/key115.csv"
    //    val outputPath = "C:/input/output"
    val cipherPath = args(0)
    val keyPath = args(1)
    val outputPath = args(2)


    val spark = SparkSession
      .builder()
      .appName("Dekripsi Big Data")
      .getOrCreate()
    val ciphertext = spark.read.option("header","true").csv(cipherPath).withColumn("id",row_number().over(Window.orderBy(lit(0)))-1)
    val key = spark.read.option("header","true").csv(keyPath)
    key.createOrReplaceTempView("keyTable")
    spark.udf.register( "decrypt", decrypt _ )
    spark.udf.register("decryptBelows", decryptBelows _)
    spark.udf.register( "decryptUppers", decryptUppers _ )

    val cipherKey1 = ciphertext.crossJoin(key)
    cipherKey1.createOrReplaceTempView("tempTable")
    val cipherKey2 = spark.sql("SELECT id,cast(value as double), cast(minValue as double),cast(maxValue as double),cast(minEncryptedValue as double),cast(maxEncryptedValue as double) FROM tempTable")
    cipherKey2.createOrReplaceTempView("tempTable")
    val cipherKey3 = spark.sql("SELECT id, value, minValue, maxValue, minEncryptedValue, maxEncryptedValue FROM tempTable WHERE value>= minEncryptedValue AND value <= maxEncryptedValue ORDER BY id ASC ")
    cipherKey3.createOrReplaceTempView("tempTable")
    val cipherKey4 = spark.sql("SELECT id, value as encryptedValue, decrypt(minValue,maxValue,minEncryptedValue,maxEncryptedValue,value) as decryptedValue FROM tempTable ORDER BY id ASC").dropDuplicates("id")

    // BUAT OUT RANGE
    val outRangeKeyBelow = spark.sql("SELECT * FROM keyTable ORDER BY bucketId ASC LIMIT 1")
    val outRangeRecordBelow = ciphertext.crossJoin(outRangeKeyBelow)
    outRangeRecordBelow.createOrReplaceTempView("outRangeTableBelow")
    val outRangeTableBelow = spark.sql("SELECT id,cast(value as double), cast(minValue as double),cast(maxValue as double),cast(minEncryptedValue as double),cast(maxEncryptedValue as double), cast(p0b as double), cast(p1b as double), cast(f0b as double), cast(f1b as double),cast(p0u as double), cast(p1u as double), cast(f0u as double), cast(f1u as double) FROM outRangeTableBelow ")
    outRangeTableBelow.createOrReplaceTempView("outRangeTableBelow")

    val outRangeKeyUpper = spark.sql("SELECT * FROM keyTable ORDER BY bucketId DESC LIMIT 1")
    val outRangeRecordUpper = ciphertext.crossJoin(outRangeKeyUpper)
    outRangeRecordUpper.createOrReplaceTempView("outRangeTableUpper")
    val outRangeTableUpper = spark.sql("SELECT id,cast(value as double), cast(minValue as double),cast(maxValue as double),cast(minEncryptedValue as double),cast(maxEncryptedValue as double), cast(p0b as double), cast(p1b as double), cast(f0b as double), cast(f1b as double),cast(p0u as double), cast(p1u as double), cast(f0u as double), cast(f1u as double) FROM outRangeTableUpper ")
    outRangeTableUpper.createOrReplaceTempView("outRangeTableUpper")

    // below
    val cipherBelow_1 = spark.sql("SELECT * FROM outRangeTableBelow WHERE value < minEncryptedValue")
    cipherBelow_1.createOrReplaceTempView("belowTable")
    val decryptBelow = spark.sql("SELECT id, value as encryptedValue, decryptBelows(value,p0b,f0b,p1b,f1b,minEncryptedValue)as value FROM belowTable")
    decryptBelow.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      //      .option("path",outputPath)
      .mode("overwrite")
      .csv("D:/compile/decryptBelow")

    //upper

    val cipherUpper_1 = spark.sql("SELECT * FROM outRangeTableUpper WHERE value> maxEncryptedValue")
    cipherUpper_1.createOrReplaceTempView("upperTable")
    cipherUpper_1.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      //      .option("path",outputPath)
      .mode("overwrite")
      .csv("D:/compile/upper_1")

    val decryptUpper = spark.sql("SELECT id, value as encryptedValue, decryptUppers(value,p0u,f0u,p1u,f1u,maxEncryptedValue)as value FROM upperTable")

    val final_1 = cipherKey4.union(decryptBelow)
    val final_2 = final_1.union(decryptUpper)
    final_2.createOrReplaceTempView("finalTable")
    val final_3 = spark.sql("SELECT * FROM finalTable ORDER By id ASC")

    final_3.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      //      .option("path",outputPath)
      .mode("overwrite")
      .csv(outputPath)

    sc.stop()
  }

  def decrypt(minValue:Double,maxValue:Double, minEncryptedValue:Double, maxEncryptedValue:Double, encryptedValue:Double):Long = {
    return Math.round(minValue+(encryptedValue-minEncryptedValue)/(maxEncryptedValue-minEncryptedValue)*(maxValue-minValue))
  }

  def decryptBelows(encryptValue:Double,p0b:Double,f0b:Double,p1b:Double,f1b:Double,minEncryptedValue:Double):Double = {
    var z = (f1b-f0b) / (p1b-p0b)
    val result = (encryptValue-minEncryptedValue)/z
    print("result: ",result)
    print("encryptValue", encryptValue)
    print("minEncryptValue:", minEncryptedValue)
    print("z: ",z)
    print("p1b: ",p1b)
    print("p0b",p0b)
    return Math.round(result)
  }

  def decryptUppers(encryptValue:Double,p0u:Double,f0u:Double,p1u:Double,f1u:Double,maxEncryptedValue:Double):Double = {
    var z = (f1u-f0u) / (p1u-p0u)
    val result = (encryptValue-maxEncryptedValue)/z
    return Math.round(result)
  }
}
