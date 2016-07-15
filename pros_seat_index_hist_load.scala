:paste
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import com.databricks.spark.csv.CsvParser
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};


sc.addJar("/opt/hadoop-jars/commons-csv-1.2.jar")
sc.addJar("/opt/hadoop-jars/spark-csv_2.10-1.3.0.jar")

val ShortDatePattern = """_(\d{6}).csv"""

/** Takes an array of filenames and returns individual RDDs for each.
 *  TODO: Create overloaded methods that take filesystem paths and globs.
 */
def rddsFromFiles(filenames: Array[String]): Array[(String, RDD[String])] = filenames.map(filename =>  (filename, sc.textFile(filename)))


/** convenience function for casting strings to dates given a custom format. */
val parseDate = udf((strDate: String, strFormat: String) => new java.sql.Date(new java.text.SimpleDateFormat(strFormat).parse(strDate).getTime))



// Get list of loaded dates.
// NOTE: If this starts slowing down, implement partition pruning on recent
// dates.
val dfMain = sqlContext.table("cis.pros_seat_idx_hist")
val loadedDates = dfMain.select($"snapshot_date").distinct.select(date_format($"snapshot_date", "yyMMdd")).collect().map(_.getString(0))

// Get list of files
val fs = FileSystem.get(new Configuration())

val files = fs.globStatus(new Path("pros/asiamiles_seatIdx_*.csv*"))
    .map(_.getPath.toString)
    .filter(filename => !loadedDates.contains(ShortDatePattern.r.findFirstMatchIn(filename).get.group(1)))

val rdds = rddsFromFiles(files).map{ case (filename, rdd) => rdd.repartition(12).filter(!_.startsWith("Flt Date")).map(filename + "," + _) }
val mergedRDD = sc.union(rdds)

val csvSchema = StructType(Array(
    StructField("filename", StringType, false),
    StructField("seg_dep_date_str", StringType, false),
    StructField("carrier_code", StringType, false),
    StructField("flt_num", IntegerType, false),
    StructField("orig_apt", StringType, false),
    StructField("dest_apt", StringType, false),
    StructField("compt", StringType, false),
    StructField("seat_idx", IntegerType, true)))

val dfCsv = new CsvParser().withSchema(csvSchema).csvRdd(sqlContext, mergedRDD)

val dfStage = dfCsv
    .withColumn("seg_dep_date", parseDate($"seg_dep_date_str", lit("dd-MMM-yy")))
    .withColumn("snapshot_date", parseDate(regexp_extract($"filename", ShortDatePattern, 1), lit("yyMMdd")))
    .withColumn("snapshot_year", year($"snapshot_date").cast("string"))
    .withColumn("snapshot_month", format_string("%02d", month($"snapshot_date")))
    .select("seg_dep_date", "carrier_code", "flt_num", "orig_apt", "dest_apt", "compt", "seat_idx", "snapshot_date", "snapshot_year", "snapshot_month")

dfStage.write.partitionBy("snapshot_year", "snapshot_month").insertInto("cis.pros_seat_idx_hist")

