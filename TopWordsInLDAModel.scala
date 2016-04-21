import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer, IDF,IDFModel, ElementwiseProduct}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.clustering.LocalLDAModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.databricks.spark.csv.CsvContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

val uri:String = "s3://mimic3-project-files/"
val folder:String = "outputs/"

def registerSchema(filename:String, tableName:String,
    tableSchema:StructType,uri:String,sqlContext:SQLContext){

    val table = sqlContext.read.
      format("com.databricks.spark.csv").
      option("header", "true").
      schema(tableSchema).load(uri+filename).cache()
    table.registerTempTable(tableName.toUpperCase)
  }

def registerOutputSchema(filename:String, tableName:String,
    tableSchema:StructType,uri:String,sqlContext:SQLContext){

    val table = sqlContext.read.
      format("com.databricks.spark.csv").
      option("header", "true").
      option("nullValue", "null").
      schema(tableSchema).load(uri+filename).cache()
    table.registerTempTable(tableName.toUpperCase)
  }

val customSchema = StructType(Array(
    StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ADMITTIME", TimestampType, true),
    StructField("DISCHTIME", TimestampType, true),
    StructField("DEATHTIME", TimestampType, true),
    StructField("ADMISSION_TYPE", StringType, true),
    StructField("ADMISSION_LOCATION", StringType, true),
    StructField("DISCHARGE_LOCATION", StringType, true),
    StructField("INSURANCE", StringType, true),
    StructField("LANGUAGE", StringType, true),
    StructField("RELIGION", StringType, true),
    StructField("MARITAL_STATUS", StringType, true),
    StructField("ETHNICITY", StringType, true),
    StructField("EDREGTIME", StringType, true),
    StructField("EDOUTTIME", StringType, true),
    StructField("DIAGNOSIS", StringType, true),
    StructField("HOSPITAL_EXPIRE_FLAG", IntegerType, true),
    StructField("HAS_IOEVENTS_DATA", IntegerType, true),
    StructField("HAS_CHARTEVENTS_DATA", IntegerType, true)))
    
registerSchema("ADMISSIONS.csv","admissions",customSchema,uri,sqlContext)

val patientsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("GENDER", StringType, true),
    StructField("DOB", TimestampType, true),
    StructField("DOD", TimestampType, true),
    StructField("DOD_HOSP", TimestampType, true),
    StructField("DOD_SSN", TimestampType, true),
    StructField("EXPIRE_FLAG", IntegerType, true)))
    
registerSchema("PATIENTS.csv","patients",patientsSchema,uri,sqlContext)

val noteeventsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("CHARTDATE", DateType, true),
    StructField("CHARTTIME", StringType, true),
    StructField("STORETIME", TimestampType, true),
    StructField("CATEGORY", StringType, true),
    StructField("DESCRIPTION", StringType, true),
    StructField("CGID", IntegerType, true),
    StructField("ISERROR", StringType, true),
    StructField("TEXT", StringType, true)))
    
registerSchema("NOTEEVENTS_PROCESSED.csv","noteevents",noteeventsSchema,uri,sqlContext)

val icustaysSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ICUSTAY_ID", IntegerType, true),
    StructField("DBSOURCE", StringType, true),
    StructField("FIRST_CAREUNIT", StringType, true),
    StructField("LAST_CAREUNIT", StringType, true),
    StructField("FIRST_WARDID", IntegerType, true),
    StructField("LAST_WARDID", IntegerType, true),
    StructField("INTIME", TimestampType, true),
    StructField("OUTTIME", TimestampType, true),
    StructField("LOS", DoubleType, true)))
    

registerSchema("ICUSTAYS.csv","icustays",icustaysSchema,uri,sqlContext)

val oasisSchema = StructType(Array(StructField("subject_id", IntegerType, true),
    StructField("hadm_id", IntegerType, true),
    StructField("icustay_id", IntegerType, true),
    StructField("ICUSTAY_AGE_GROUP", StringType, true),
    StructField("hospital_expire_flag", IntegerType, true),
    StructField("icustay_expire_flag", IntegerType, true),
    StructField("OASIS", IntegerType, true),
    StructField("OASIS_PROB", DoubleType, true),
    StructField("age", IntegerType, true),
    StructField("age_score", IntegerType, true),
    StructField("preiculos", IntegerType, true),
    StructField("preiculos_score", IntegerType, true),
    StructField("gcs", DoubleType, true),
    StructField("gcs_score", IntegerType, true),
    StructField("heartrate", DoubleType, true),
    StructField("heartrate_score", IntegerType, true),
    StructField("meanbp", DoubleType, true),
    StructField("meanbp_score", IntegerType, true),
    StructField("resprate", DoubleType, true),
    StructField("resprate_score", IntegerType, true),
    StructField("temp", DoubleType, true),
    StructField("temp_score", IntegerType, true),
    StructField("urineoutput", DoubleType, true),
    StructField("UrineOutput_score", IntegerType, true),
    StructField("mechvent", IntegerType, true),
    StructField("mechvent_score", IntegerType, true),
    StructField("electivesurgery", IntegerType, true),
    StructField("electivesurgery_score", IntegerType, true)))
    
registerOutputSchema("OASIS.csv","oasis",oasisSchema,uri+folder,sqlContext) 

val sapsiiSchema = StructType(Array(StructField("subject_id", IntegerType, true),
    StructField("hadm_id", IntegerType, true),
    StructField("icustay_id", IntegerType, true),
    StructField("SAPSII", IntegerType, true),
    StructField("SAPSII_PROB", DoubleType, true),
    StructField("age_score", IntegerType, true),
    StructField("hr_score", IntegerType, true),
    StructField("sysbp_score", IntegerType, true),
    StructField("temp_score", IntegerType, true),
    StructField("PaO2FiO2_score", IntegerType, true),
    StructField("uo_score", IntegerType, true),
    StructField("bun_score", IntegerType, true),
    StructField("wbc_score", IntegerType, true),
    StructField("potassium_score", IntegerType, true),
    StructField("sodium_score", IntegerType, true),
    StructField("bicarbonate_score", IntegerType, true),
    StructField("bilirubin_score", IntegerType, true),
    StructField("gcs_score", IntegerType, true),
    StructField("comorbidity_score", IntegerType, true),
    StructField("UrineOutput_score", IntegerType, true),
    StructField("admissiontype_score", IntegerType, true)))    

registerOutputSchema("SAPSII.csv","sapsii",sapsiiSchema,uri+folder,sqlContext) 

val sofaSchema = StructType(Array(StructField("subject_id", IntegerType, true),
    StructField("hadm_id", IntegerType, true),
    StructField("icustay_id", IntegerType, true),
    StructField("SOFA", IntegerType, true),
    StructField("respiration", IntegerType, true),
    StructField("coagulation", IntegerType, true),
    StructField("liver", IntegerType, true),
    StructField("cardiovascular", IntegerType, true),
    StructField("cns", IntegerType, true),
    StructField("renal", IntegerType, true)))
registerOutputSchema("SOFA.csv","sofa",sofaSchema,uri+folder,sqlContext) 


val trainer: (Int => Int) = (arg:Int) => 0
val sqlTrainer = udf(trainer)
val tester: (Int => Int) = (arg:Int) => 1
val sqlTester = udf(tester)

val patients = sqlContext.sql("SELECT * FROM patients")
val splits = patients.randomSplit(Array(0.7, 0.3), seed = 11L)  //Seed Set for Reproducibility
val train_patients = splits(0).select("subject_id").withColumn("test",sqlTrainer(col("subject_id")))
val test_patients = splits(1).select("subject_id").withColumn("test",sqlTester(col("subject_id")))

val patient_labels = test_patients.unionAll(train_patients).sort("subject_id").cache()
patient_labels.count()
patient_labels.registerTempTable("patient_labels")

var stopwords1 = sc.textFile(uri+"onixstopwords").collect.toArray
stopwords1 = stopwords1 ++ Array("ml","dl","mg","kg","pm")
val stopwords = stopwords1.toSet
val broadcaststopwords = sc.broadcast(stopwords)

sqlContext.udf.register("cleanString", (s: String) => {
    val words = s.split(" ")
    val filtered_words = words.map(_.toLowerCase()).filter(w => !broadcaststopwords.value.contains(w))
    var result = ""
    if (filtered_words.size > 0){
        result = filtered_words.reduceLeft((x,y)=>x+" "+y)
    }
    result
})
sqlContext.udf.register("combindString",(arrayCol: Seq[String]) => arrayCol.mkString(" "))

val documents = sqlContext.sql("""
SELECT noteevents.subject_id,noteevents.hadm_id,combindString(collect_list(cleanString(regexp_replace(regexp_replace(text,"[^a-zA-Z\\s]",' '),"NEWLINE",' ')))) as Status
FROM noteevents 
JOIN patient_labels
ON patient_labels.subject_id = noteevents.subject_id
AND patient_labels.test = 0
WHERE storetime is not null
AND iserror = '' 
GROUP BY noteevents.subject_id, noteevents.hadm_id
""")

val regexTokenizer = new RegexTokenizer().
    setInputCol("Status").
    setOutputCol("Words").
    setPattern("\\W")

val regexTokenized = regexTokenizer.transform(documents)

val remover = new StopWordsRemover().
    setInputCol("Words").
    setOutputCol("Filtered")

val cleanedTokenized = remover.transform(regexTokenized)
cleanedTokenized.cache()

val cvModel = CountVectorizerModel.load(uri+"models/cvModel")
val ldaModel = LocalLDAModel.load(uri+"models/ldaModel")

val schema = StructType(Array(StructField("Filtered",ArrayType(StringType,true),true)))
val newWordRDD:RDD[String] = cleanedTokenized.select("Filtered").rdd.map{case Row(s:Seq[x]) => s}.flatMap(w => w).map(s => s.toString).distinct()
val newWordArrayRDD:RDD[Row] = newWordRDD.map(w => Row(Array(w)))
val wordDF = sqlContext.createDataFrame(newWordArrayRDD,schema)
val wordCountDF = cvModel.transform(wordDF)
val wordMap = wordCountDF.
    map{case Row(w:Seq[String],v:Vector) => (v.numNonzeros,w.head,v)}.
    filter(_._1 == 1).
    map{case(c,w,v) => (v.argmax,w)}.
    collect.
    toMap


ldaModel.describeTopics(15).select("termIndices").map{case Row(a:Seq[Int]) => a.toArray.map(i => wordMap(i))}.take(35).foreach(s => println(s.toList))

