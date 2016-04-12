
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer, IDF,IDFModel, ElementwiseProduct}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import  org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics


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
    
val admissions = sqlContext.read.
    format("com.databricks.spark.csv").
    option("header", "true").
    schema(customSchema).
    load("/input/ADMISSIONS.csv").cache()
    
admissions.registerTempTable("admissions")

val patientsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("GENDER", StringType, true),
    StructField("DOB", TimestampType, true),
    StructField("DOD", TimestampType, true),
    StructField("DOD_HOSP", TimestampType, true),
    StructField("DOD_SSN", TimestampType, true),
    StructField("EXPIRE_FLAG", IntegerType, true)))
    
val patients = sqlContext.read.
    format("com.databricks.spark.csv").
    option("header", "true").
    schema(patientsSchema).
    load("/input/PATIENTS.csv").cache()
    
patients.registerTempTable("patients")

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
    
val noteevents = sqlContext.read.
    format("com.databricks.spark.csv").
    option("header", "true").
    schema(noteeventsSchema).
    load("/input/NOTEEVENTS_PROCESSED_2.csv").cache()

noteevents.registerTempTable("noteevents")

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
    
val icustays = sqlContext.read.
    format("com.databricks.spark.csv").
    option("header", "true").
    schema(icustaysSchema).
    load("/input/ICUSTAYS.csv").cache()
    
icustays.registerTempTable("icustays")

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
    
val oasis = sqlContext.read.
    format("com.databricks.spark.csv").
    option("header", "true").
    schema(oasisSchema).
    load("/input/OASIS.csv").cache()
    
oasis.registerTempTable("oasis")

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
    
val sapsii = sqlContext.read.
    format("com.databricks.spark.csv").
    option("header", "true").
    schema(sapsiiSchema).
    load("/input/SAPSII.csv").cache()
    
sapsii.registerTempTable("sapsii")

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

val sofa = sqlContext.read.
    format("com.databricks.spark.csv").
    option("header", "true").
    schema(sofaSchema).
    load("/input/SOFA.csv").cache()
    
sofa.registerTempTable("sofa")

admissions.count
patients.count
noteevents.count
icustays.count
oasis.count
sofa.count
sapsii.count

val trainer: (Int => Int) = (arg:Int) => 0
val sqlTrainer = udf(trainer)
val tester: (Int => Int) = (arg:Int) => 1
val sqlTester = udf(tester)

val splits = patients.randomSplit(Array(0.7, 0.3), seed = 11L)
val train_patients = splits(0).select("subject_id").withColumn("test",sqlTrainer(col("subject_id")))
val test_patients = splits(1).select("subject_id").withColumn("test",sqlTester(col("subject_id")))

val patient_labels = test_patients.unionAll(train_patients).sort("subject_id").cache()
patient_labels.count()
patient_labels.registerTempTable("patient_labels")

var stopwords1 = sc.textFile("/input/onixstopwords").collect.toArray
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
""").cache()

val regexTokenizer = new RegexTokenizer().
    setInputCol("Status").
    setOutputCol("Words").
    setPattern("\\W")

val regexTokenized = regexTokenizer.transform(documents)

val remover = new StopWordsRemover().
    setInputCol("Words").
    setOutputCol("Filtered")

val cleanedTokenized = remover.transform(regexTokenized)
/*
val cvModel = CountVectorizerModel.load("/input/models/countVectorizorModel")
val idfModel = IDFModel.load("/input/models/idfModel")
val countTokenized = cvModel.transform(cleanedTokenized)
val idfTokenized = idfModel.transform(countTokenized)
*/

val cvModel: CountVectorizerModel = new CountVectorizer().
    setInputCol("Filtered").
    setOutputCol("CountVector").
    setVocabSize(500000).
    setMinDF(20).
    fit(cleanedTokenized)

val countTokenized = cvModel.transform(cleanedTokenized)

val idf = new IDF().setInputCol("CountVector").setOutputCol("IDFVector")
val idfModel = idf.fit(countTokenized)

val idfTokenized = idfModel.transform(countTokenized)
idfTokenized.cache()
idfTokenized.show()

//idfModel.save("/input/models/idfModel2")
//cvModel.save("/input/models/countVectorizorModel2")

val lda = new LDA().
    setK(35).
    setMaxIter(25).
    setFeaturesCol("IDFVector").
    setDocConcentration(1.0). //Alpha
    setTopicConcentration(0.01). //Beta
    setTopicDistributionCol("TopicVector")
val ldaModel = lda.fit(idfTokenized)

val ldaTokenized = ldaModel.transform(idfTokenized)
ldaTokenized.registerTempTable("ldaTokenized")
ldaTokenized.show()

import org.apache.spark.sql.Row
val schema = StructType(Array(StructField("Filtered",ArrayType(StringType,true),true)))
val newWordRDD:RDD[String] = ldaTokenized.select("Filtered").rdd.map{case Row(s:Seq[x]) => s}.flatMap(w => w).map(s => s.toString).distinct()
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

ldaTokenized.select("TopicVector").rdd.map{ case Row(v:Vector) => (v.argmax,1) }.reduceByKey( _ + _ ).collect().foreach(println)

val firstDayScores = sqlContext.sql("""
with subset as (
    SELECT subject_id, hadm_id, min(intime) as firstin
    FROM icustays GROUP BY subject_id, hadm_id)

SELECT ad.HOSPITAL_EXPIRE_FLAG, ad.subject_id,ad.hadm_id, ad.admission_type,oasis.OASIS,sapsii.SAPSII,sofa.SOFA,pat.GENDER, 
    ((unix_timestamp(ad.admittime)-unix_timestamp(pat.DOB))/(86400*365)) as age,
     ((unix_timestamp(ad.dischtime)-unix_timestamp(ad.admittime))/(86400)) as los FROM admissions ad
LEFT JOIN subset s
ON s.subject_id = ad.subject_id
AND s.hadm_id = ad.hadm_id
LEFT JOIN icustays icu
ON icu.subject_id = s.subject_id
AND icu.hadm_id = s.hadm_id
AND icu.intime = s.firstin
AND icu.intime < ad.admittime + interval '1' day
LEFT JOIN oasis
ON icu.subject_id = oasis.subject_id
AND icu.hadm_id = oasis.hadm_id
AND icu.icustay_id = oasis.icustay_id
LEFT JOIN sapsii
ON icu.subject_id = sapsii.subject_id
AND icu.hadm_id = sapsii.hadm_id
AND icu.icustay_id = sapsii.icustay_id
LEFT JOIN sofa
ON icu.subject_id = sofa.subject_id
AND icu.hadm_id = sofa.hadm_id
AND icu.icustay_id = sofa.icustay_id
JOIN patients pat
ON ad.subject_id = pat.subject_id
""").cache()
firstDayScores.count()
firstDayScores.registerTempTable("firstDayScore")

val dataDf = sqlContext.sql("""
SELECT firstDayScore.subject_id,
       firstDayScore.hadm_id,
        case when ADMISSION_TYPE = 'ELECTIVE' then 1 else 0 end as isElective,
        case when ADMISSION_TYPE = 'NEWBORN' then 1 else 0 end as isNewborn,
        case when ADMISSION_TYPE = 'URGENT' then 1 else 0 end as isUrgent,
        case when ADMISSION_TYPE = 'EMERGENCY' then 1 else 0 end as isEmergency,
        case WHEN GENDER = 'M' then 1 else 0 end as isMale,
        case when GENDER = 'F' then 1 else 0 end as isFemale,
        coalesce(OASIS,0) as oasis_score,
        coalesce(SAPSII,0) as sapsii_score,
        coalesce(SOFA,0) as sofa_score,
        age,
        HOSPITAL_EXPIRE_FLAG as hosp_death,
        test
FROM firstDayScore 
JOIN patient_labels
ON patient_labels.subject_id = firstDayScore.subject_id
""")
dataDf.count()
dataDf.show()
dataDf.registerTempTable("data")

val allData = sqlContext.sql("""
SELECT ldaTokenized.TopicVector, data.* 
FROM ldaTokenized 
JOIN data
ON data.subject_id = ldaTokenized.subject_id
AND data.hadm_id = ldaTokenized.hadm_id
""")
allData.count()
allData.show()


val assembler = new VectorAssembler().
setInputCols(Array("TopicVector", 
    "isElective", 
    "isNewborn",
    "isUrgent",
    "isEmergency",
    "isMale",
    "isFemale",
    "oasis_score",
    "sapsii_score",
    "sofa_score",
    "age")).
setOutputCol("ModelFeatures")

val trainData = assembler.transform(allData)

import org.apache.spark.sql.functions._
val toDouble = udf[Double, String]( _.toDouble)

val trainData2 = trainData.withColumn("Label", toDouble(trainData("hosp_death")))

val lr = new LogisticRegression().
    setMaxIter(10).
    setRegParam(0.0).
    setFeaturesCol("ModelFeatures").
    setLabelCol("Label").
    setPredictionCol("ModelPrediction").
    setProbabilityCol("ModelProbability") 

// Fit the model
val lrModel = lr.fit(trainData2)
val results = lrModel.transform(trainData2)

results.select("Label","ModelProbability").schema.show()

val scoreAndLabel = results.
    select("Label","ModelProbability").
    map{ case Row(l:Double,p:Vector) => (p(1),l) }
val metrics = new BinaryClassificationMetrics(scoreAndLabel)
val auROC = metrics.areaUnderROC()


val testdocuments = sqlContext.sql("""
SELECT noteevents.subject_id,noteevents.hadm_id,combindString(collect_list(cleanString(regexp_replace(regexp_replace(text,"[^a-zA-Z\\s]",' '),"NEWLINE",' ')))) as Status
FROM noteevents 
JOIN patient_labels
ON patient_labels.subject_id = noteevents.subject_id
AND patient_labels.test = 1
WHERE storetime is not null
AND iserror = '' 
GROUP BY noteevents.subject_id, noteevents.hadm_id
""").cache()

val regexTokenizedTest = regexTokenizer.transform(testdocuments)
val cleanedTokenizedTest = remover.transform(regexTokenizedTest)
val countTokenizedTest = cvModel.transform(cleanedTokenizedTest)
val idfTokenizedTest = idfModel.transform(countTokenizedTest)
val ldaTokenizedTest = ldaModel.transform(idfTokenizedTest)
ldaTokenizedTest.registerTempTable("ldaTokenizedTest")

val allDataTest = sqlContext.sql("""
SELECT ldaTokenizedTest.TopicVector, data.* 
FROM ldaTokenizedTest 
JOIN data
ON data.subject_id = ldaTokenizedTest.subject_id
AND data.hadm_id = ldaTokenizedTest.hadm_id
""")

val testData = assembler.transform(allDataTest)
val testData2 = testData.withColumn("Label", toDouble(trainData("hosp_death")))

val testResults = lrModel.transform(testData2)

val testScoreAndLabel = testResults.
    select("Label","ModelProbability").
    map{ case Row(l:Double,p:Vector) => (p(1),l) }
val testMetrics = new BinaryClassificationMetrics(testScoreAndLabel)
val auROC = testMetrics.areaUnderROC()

val roc = testMetrics.roc()
val rocRDD:RDD[Row] = roc.map{ case (fpr:Double,tpr:Double) => Row(fpr:Double,tpr:Double)}

val rocDf = roc.map{ case (fpr:Double,tpr:Double)} => Row(fpr,tpr)).toDF()
val schemaROC = StructType(Array(StructField("FPR",DoubleType),StructField("FPR",DoubleType)))
val rocDf = sqlContext.createDataFrame(rocRDD,schemaROC)

def getAUC( days:Int ) : Double = {
    val time = days*86400
    val query = s"""SELECT noteevents.subject_id,noteevents.hadm_id,combindString(collect_list(cleanString(regexp_replace(regexp_replace(text,"[^a-zA-Z\\s]",' '),"NEWLINE",' ')))) as Status
        FROM noteevents 
        JOIN patient_labels
        ON patient_labels.subject_id = noteevents.subject_id
        AND patient_labels.test = 1
        JOIN admissions 
        ON noteevents.subject_id = admissions.subject_id
        AND noteevents.hadm_id = admissions.hadm_id
        WHERE storetime is not null
        AND iserror = ''
        AND unix_timestamp(admissions.admittime)+${time} < unix_timestamp(admissions.dischtime)
        AND unix_timestamp(storetime) < unix_timestamp(admissions.dischtime)-86400
        AND unix_timestamp(storetime) < unix_timestamp(admissions.admittime)+${time}
        GROUP BY noteevents.subject_id, noteevents.hadm_id
    """
    val testdocuments = sqlContext.sql(query).cache()
    val regexTokenizedTest = regexTokenizer.transform(testdocuments)
    val cleanedTokenizedTest = remover.transform(regexTokenizedTest)
    val countTokenizedTest = cvModel.transform(cleanedTokenizedTest)
    val idfTokenizedTest = idfModel.transform(countTokenizedTest)
    val ldaTokenizedTest = ldaModel.transform(idfTokenizedTest)
    ldaTokenizedTest.registerTempTable("ldaTokenizedTest")

    val allDataTest = sqlContext.sql("""
    SELECT ldaTokenizedTest.TopicVector, data.* 
    FROM ldaTokenizedTest 
    JOIN data
    ON data.subject_id = ldaTokenizedTest.subject_id
    AND data.hadm_id = ldaTokenizedTest.hadm_id
    """)

    val testData = assembler.transform(allDataTest)
    val testData2 = testData.withColumn("Label", toDouble(trainData("hosp_death")))

    val testResults = lrModel.transform(testData2)

    val testScoreAndLabel = testResults.
        select("Label","ModelProbability").
        map{ case Row(l:Double,p:Vector) => (p(1),l) }
    val testMetrics = new BinaryClassificationMetrics(testScoreAndLabel)
    val auROC = testMetrics.areaUnderROC()
    auROC
}

val lrT = new LogisticRegression().
    setMaxIter(10).
    setRegParam(0.0).
    setFeaturesCol("TopicVector").
    setLabelCol("Label").
    setPredictionCol("ModelPrediction").
    setProbabilityCol("ModelProbability") 

// Fit the model
val lrTModel = lrT.fit(trainData2)
val textResults = lrTModel.transform(trainData2)

val textScoreAndLabel = textResults.
    select("Label","ModelProbability").
    map{ case Row(l:Double,p:Vector) => (p(1),l) }
val textMetrics = new BinaryClassificationMetrics(textScoreAndLabel)
val textAuROC = textMetrics.areaUnderROC()

def getTextAUC( days:Int ) : Double = {
    val time = days*86400
    val query = s"""SELECT noteevents.subject_id,noteevents.hadm_id,combindString(collect_list(cleanString(regexp_replace(regexp_replace(text,"[^a-zA-Z\\s]",' '),"NEWLINE",' ')))) as Status
        FROM noteevents 
        JOIN patient_labels
        ON patient_labels.subject_id = noteevents.subject_id
        AND patient_labels.test = 1
        JOIN admissions 
        ON noteevents.subject_id = admissions.subject_id
        AND noteevents.hadm_id = admissions.hadm_id
        WHERE storetime is not null
        AND iserror = ''
        AND unix_timestamp(admissions.admittime)+${time} < unix_timestamp(admissions.dischtime)
        AND unix_timestamp(storetime) < unix_timestamp(admissions.dischtime)-86400
        AND unix_timestamp(storetime) < unix_timestamp(admissions.admittime)+${time}
        GROUP BY noteevents.subject_id, noteevents.hadm_id
    """
    val testdocuments = sqlContext.sql(query).cache()
    val regexTokenizedTest = regexTokenizer.transform(testdocuments)
    val cleanedTokenizedTest = remover.transform(regexTokenizedTest)
    val countTokenizedTest = cvModel.transform(cleanedTokenizedTest)
    val idfTokenizedTest = idfModel.transform(countTokenizedTest)
    val ldaTokenizedTest = ldaModel.transform(idfTokenizedTest)
    ldaTokenizedTest.registerTempTable("ldaTokenizedTest")

    val allDataTest = sqlContext.sql("""
    SELECT ldaTokenizedTest.TopicVector, data.* 
    FROM ldaTokenizedTest 
    JOIN data
    ON data.subject_id = ldaTokenizedTest.subject_id
    AND data.hadm_id = ldaTokenizedTest.hadm_id
    """)

    val testData = assembler.transform(allDataTest)
    val testData2 = testData.withColumn("Label", toDouble(trainData("hosp_death")))

    val testResults = lrTModel.transform(testData2)

    val testScoreAndLabel = testResults.
        select("Label","ModelProbability").
        map{ case Row(l:Double,p:Vector) => (p(1),l) }
    val testMetrics = new BinaryClassificationMetrics(testScoreAndLabel)
    val auROC = testMetrics.areaUnderROC()
    auROC
}




val testdocuments = sqlContext.sql("""
SELECT noteevents.subject_id,noteevents.hadm_id,combindString(collect_list(cleanString(regexp_replace(regexp_replace(text,"[^a-zA-Z\\s]",' '),"NEWLINE",' ')))) as Status
FROM noteevents 
JOIN patient_labels
ON patient_labels.subject_id = noteevents.subject_id
AND patient_labels.test = 1
JOIN admissions 
ON noteevents.subject_id = admissions.subject_id
AND noteevents.hadm_id = admissions.hadm_id
WHERE storetime is not null
AND iserror = '' 
AND unix_timestamp(storetime) < unix_timestamp(admissions.admittime)+86400
GROUP BY noteevents.subject_id, noteevents.hadm_id
""").cache()
val regexTokenizedTest = regexTokenizer.transform(testdocuments)
val cleanedTokenizedTest = remover.transform(regexTokenizedTest)
val countTokenizedTest = cvModel.transform(cleanedTokenizedTest)
val idfTokenizedTest = idfModel.transform(countTokenizedTest)
val ldaTokenizedTest = ldaModel.transform(idfTokenizedTest)
ldaTokenizedTest.registerTempTable("ldaTokenizedTest")

val allDataTest = sqlContext.sql("""
SELECT ldaTokenizedTest.TopicVector, data.* 
FROM ldaTokenizedTest 
JOIN data
ON data.subject_id = ldaTokenizedTest.subject_id
AND data.hadm_id = ldaTokenizedTest.hadm_id
""")

val testData = assembler.transform(allDataTest)
val testData2 = testData.withColumn("Label", toDouble(trainData("hosp_death")))

val testResults = lrModel.transform(testData2)

val testScoreAndLabel = testResults.
    select("Label","ModelProbability").
    map{ case Row(l:Double,p:Vector) => (p(1),l) }
val testMetrics = new BinaryClassificationMetrics(testScoreAndLabel)
val auROC = testMetrics.areaUnderROC()




sqlContext.sql("""
SELECT noteevents.subject_id,noteevents.hadm_id,combindString(collect_list(cleanString(regexp_replace(regexp_replace(text,"[^a-zA-Z\\s]",' '),"NEWLINE",' ')))) as Status
FROM noteevents 
JOIN patient_labels
ON patient_labels.subject_id = noteevents.subject_id
AND patient_labels.test = 0
WHERE storetime is not null
AND iserror = '' 
GROUP BY noteevents.subject_id, noteevents.hadm_id
""").count()



