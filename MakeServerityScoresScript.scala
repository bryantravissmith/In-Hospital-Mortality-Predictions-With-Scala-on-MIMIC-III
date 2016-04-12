import org.apache.spark.sql.SQLContext
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

val calloutSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("SUBMIT_WARDID", IntegerType, true),
    StructField("SUBMIT_CAREUNIT", StringType, true),
    StructField("CURR_WARDID", IntegerType, true),
    StructField("CURR_CAREUNIT", StringType, true),
    StructField("CALLOUT_WARDID", IntegerType, true),
    StructField("CALLOUT_SERVICE", StringType, true),
    StructField("REQUEST_TELE", IntegerType, true),
    StructField("REQUEST_RESP", IntegerType, true),
    StructField("REQUEST_CDIFF", IntegerType, true),
    StructField("REQUEST_MRSA", IntegerType, true),
    StructField("REQUEST_VRE", IntegerType, true),
    StructField("CALLOUT_STATUS", StringType, true),
    StructField("CALLOUT_OUTCOME", StringType, true),
    StructField("DISCHARGE_WARDID", IntegerType, true),
    StructField("ACKNOWLEDGE_STATUS", StringType, true),
    StructField("CREATETIME", TimestampType, true),
    StructField("UPDATETIME", TimestampType, true),
    StructField("OUTCOMETIME", TimestampType, true),
    StructField("FIRSTRESERVATIONTIME", TimestampType, true),
    StructField("CURRENTRESERVATIONTIME", TimestampType, true)))

registerSchema("CALLOUT.csv","callout",calloutSchema,uri,sqlContext)


val caregiversSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("CGID", IntegerType, true),
    StructField("LABEL", StringType, true),
    StructField("DESCRIPTION", StringType, true)))

registerSchema("CAREGIVERS.csv","caregivers",caregiversSchema,uri,sqlContext)

val cpteventsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("COSTCENTER", StringType, true),
    StructField("CHARTDATE", TimestampType, true),
    StructField("CPT_CD", StringType, true),
    StructField("CPT_NUMBER", StringType, true),
    StructField("CPT_SUFFIX", StringType, true),
    StructField("TICKET_ID_SEQ", IntegerType, true),
    StructField("SECTIONHEADER", StringType, true),
    StructField("SUBSECTIONHEADER", StringType, true),
    StructField("DESCRIPTION", StringType, true)))

registerSchema("CPTEVENTS.csv","cptevents",cpteventsSchema,uri,sqlContext)

val datetimeeventsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ICUSTAY_ID", IntegerType, true),
    StructField("ITEMID", IntegerType, true),
    StructField("CHARTTIME", TimestampType, true),
    StructField("STORETIME", TimestampType, true),
    StructField("CGID", IntegerType, true),
    StructField("VALUE", TimestampType, true),
    StructField("VALUEUOM", StringType, true),
    StructField("WARNING", StringType, true),
    StructField("ERROR", StringType, true),
    StructField("RESULTSTATUS", StringType, true),
    StructField("STOPPED", StringType, true)))

registerSchema("DATETIMEEVENTS.csv","datetimeevents",datetimeeventsSchema,uri,sqlContext)

val diagnosesIcdSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("SEQ_NUM", IntegerType, true),
    StructField("ICD9_CODE", StringType, true)))


registerSchema("DIAGNOSES_ICD.csv","diagnoses_icd",diagnosesIcdSchema,uri,sqlContext)

val drgcodesSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("DRG_TYPE", StringType, true),
    StructField("DRG_CODE", IntegerType, true),
    StructField("DESCRIPTION", StringType, true),
    StructField("DRG_SEVERITY", IntegerType, true),
    StructField("DRG_MORTALITY", IntegerType, true)))

registerSchema("DRGCODES.csv","drgcodes",drgcodesSchema,uri,sqlContext)

val dCptSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("SECTIONRANGE", StringType, true),
    StructField("SECTIONHEADER", StringType, true),
    StructField("SUBSECTIONRANGE", StringType, true),
    StructField("SUBSECTIONHEADER", StringType, true),
    StructField("CODESUFFIX", StringType, true),
    StructField("MINCODEINSUBSECTION", IntegerType, true),
    StructField("MAXCODEINSUBSECTION", IntegerType, true)))

registerSchema("D_CPT.csv","d_cpt",dCptSchema,uri,sqlContext)

val dIcdDiagnosesSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("ICD9_CODE", StringType, true),
    StructField("SHORT_TITLE", StringType, true),
    StructField("LONG_TITLE", StringType, true)))

registerSchema("D_ICD_DIAGNOSES.csv","d_icd_diagnoses",dIcdDiagnosesSchema,uri,sqlContext)

val dIcdProceduresSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("ICD9_CODE", StringType, true),
    StructField("SHORT_TITLE", StringType, true),
    StructField("LONG_TITLE", StringType, true)))

registerSchema("D_ICD_PROCEDURES.csv","d_icd_procedures",dIcdProceduresSchema,uri,sqlContext)

val dItemsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("ITEMID", IntegerType, true),
    StructField("LABEL", StringType, true),
    StructField("ABBREVIATION", StringType, true),
    StructField("DBSOURCE", StringType, true),
    StructField("LINKSTO", StringType, true),
    StructField("CATEGORY", StringType, true),
    StructField("UNITNAME", StringType, true),
    StructField("PARAM_TYPE", StringType, true),
    StructField("CONCEPTID", IntegerType, true)))

registerSchema("D_ITEMS.csv","d_items",dItemsSchema,uri,sqlContext)

val dLabItemsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("ITEMID", IntegerType, true),
    StructField("LABEL", StringType, true),
    StructField("FLUID", StringType, true),
    StructField("CATEGORY", StringType, true),
    StructField("LOINC_CODE", StringType, true)))

registerSchema("D_LABITEMS.csv","d_labitems",dLabItemsSchema,uri,sqlContext)

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

val inputeventsCvSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ICUSTAY_ID", IntegerType, true),
    StructField("CHARTTIME", TimestampType, true),
    StructField("ITEMID", IntegerType, true),
    StructField("AMOUNT", DoubleType, true),
    StructField("AMOUNTUOM", StringType, true),
    StructField("RATE", DoubleType, true),
    StructField("RATEUOM", StringType, true),
    StructField("STORETIME", TimestampType, true),
    StructField("CGID", IntegerType, true),
    StructField("ORDERID", IntegerType, true),
    StructField("LINKORDERID", IntegerType, true),
    StructField("STOPPED", StringType, true),
    StructField("NEWBOTTLE", StringType, true),
    StructField("ORIGINALAMOUNT", DoubleType, true),
    StructField("ORIGINALAMOUNTUOM", StringType, true),
    StructField("ORIGINALROUTE", StringType, true),
    StructField("ORIGINALRATE", DoubleType, true),
    StructField("ORIGINALRATEUOM", StringType, true),
    StructField("ORIGINALSITE", StringType, true)))

registerSchema("INPUTEVENTS_CV.csv","inputevents_cv",inputeventsCvSchema,uri,sqlContext)

val inputeventsMvSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ICUSTAY_ID", IntegerType, true),
    StructField("STARTTIME", TimestampType, true),
    StructField("ENDTIME", TimestampType, true),
    StructField("ITEMID", IntegerType, true),
    StructField("AMOUNT", DoubleType, true),
    StructField("AMOUNTUOM", StringType, true),
    StructField("RATE", DoubleType, true),
    StructField("RATEUOM", StringType, true),
    StructField("STORETIME", TimestampType, true),
    StructField("CGID", IntegerType, true),
    StructField("ORDERID", IntegerType, true),
    StructField("LINKORDERID", IntegerType, true),
    StructField("ORDERCATEGORYNAME", StringType, true),
    StructField("SECONDARYORDERCATEGORYNAME", StringType, true),
    StructField("ORDERCOMPONENTTYPEDESCRIPTION", StringType, true),
    StructField("ORDERCATEGORYDESCRIPTION", StringType, true),
    StructField("PATIENTWEIGHT", DoubleType, true),
    StructField("TOTALAMOUNT", DoubleType, true),
    StructField("TOTALAMOUNTUOM", StringType, true),
    StructField("ISOPENBAG", IntegerType, true),
    StructField("CONTINUEINNEXTDEPT", IntegerType, true),
    StructField("CANCELREASON", IntegerType, true),
    StructField("STATUSDESCRIPTION", StringType, true),
    StructField("COMMENTS_EDITEDBY", StringType, true),
    StructField("COMMENTS_CANCELEDBY", StringType, true),
    StructField("COMMENTS_DATE", TimestampType, true),
    StructField("ORIGINALAMOUNT", DoubleType, true),
    StructField("ORIGINALRATE", DoubleType, true)))

registerSchema("INPUTEVENTS_MV.csv","inputevents_mv",inputeventsMvSchema,uri,sqlContext)

val microbiologyeventsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("CHARTDATE", TimestampType, true),
    StructField("CHARTTIME", TimestampType, true),
    StructField("SPEC_ITEMID", IntegerType, true),
    StructField("SPEC_TYPE_DESC", StringType, true),
    StructField("ORG_ITEMID", IntegerType, true),
    StructField("ORG_NAME", StringType, true),
    StructField("ISOLATE_NUM", IntegerType, true),
    StructField("AB_ITEMID", IntegerType, true),
    StructField("AB_NAME", StringType, true),
    StructField("DILUTION_TEXT", StringType, true),
    StructField("DILUTION_COMPARISON", StringType, true),
    StructField("DILUTION_VALUE", StringType, true),
    StructField("INTERPRETATION", StringType, true)))

registerSchema("MICROBIOLOGYEVENTS.csv","microbiologyevents",microbiologyeventsSchema,uri,sqlContext)

val outputeventsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ICUSTAY_ID", IntegerType, true),
    StructField("CHARTTIME", TimestampType, true),
    StructField("ITEMID", IntegerType, true),
    StructField("VALUE", DoubleType, true),
    StructField("VALUEUOM", StringType, true),
    StructField("STORETIME", TimestampType, true),
    StructField("CGID", IntegerType, true),
    StructField("STOPPED", StringType, true),
    StructField("NEWBOTTLE", StringType, true),
    StructField("ISERROR", IntegerType, true)))

registerSchema("OUTPUTEVENTS.csv","outputevents",outputeventsSchema,uri,sqlContext)

val patientsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("GENDER", StringType, true),
    StructField("DOB", TimestampType, true),
    StructField("DOD", TimestampType, true),
    StructField("DOD_HOSP", TimestampType, true),
    StructField("DOD_SSN", TimestampType, true),
    StructField("EXPIRE_FLAG", IntegerType, true)))

registerSchema("PATIENTS.csv","patients",patientsSchema,uri,sqlContext)

val prescriptionsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ICUSTAY_ID", IntegerType, true),
    StructField("STARTDATE", TimestampType, true),
    StructField("ENDDATE", TimestampType, true),
    StructField("DRUG_TYPE", StringType, true),
    StructField("DRUG", StringType, true),
    StructField("DRUG_NAME_POE", StringType, true),
    StructField("DRUG_NAME_GENERIC", StringType, true),
    StructField("FORMULARY_DRUG_CD", StringType, true),
    StructField("GSN", StringType, true),
    StructField("NDC", StringType, true),
    StructField("PROD_STRENGTH", StringType, true),
    StructField("DOSE_VAL_RX", StringType, true),
    StructField("DOSE_UNIT_RX", StringType, true),
    StructField("FORM_VAL_DISP", StringType, true),
    StructField("FORM_UNIT_DISP", StringType, true),
    StructField("ROUTE", StringType, true)))

registerSchema("PRESCRIPTIONS.csv","prescriptions",prescriptionsSchema,uri,sqlContext)

val procedureevents_mvSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ICUSTAY_ID", IntegerType, true),
    StructField("STARTTIME", TimestampType, true),
    StructField("ENDTIME", TimestampType, true),
    StructField("ITEMID", IntegerType, true),
    StructField("VALUE", DoubleType, true),
    StructField("VALUEUOM", StringType, true),
    StructField("LOCATION", StringType, true),
    StructField("LOCATIONCATEGORY", StringType, true),
    StructField("STORETIME", TimestampType, true),
    StructField("CGID", IntegerType, true),
    StructField("ORDERID", IntegerType, true),
    StructField("LINKORDERID", IntegerType, true),
    StructField("ORDERCATEGORYNAME", StringType, true),
    StructField("SECONDARYORDERCATEGORYNAME", StringType, true),
    StructField("ORDERCATEGORYDESCRIPTION", StringType, true),
    StructField("ISOPENBAG", IntegerType, true),
    StructField("CONTINUEINNEXTDEPT", IntegerType, true),
    StructField("CANCELREASON", IntegerType, true),
    StructField("STATUSDESCRIPTION", StringType, true),
    StructField("COMMENTS_EDITEDBY", StringType, true),
    StructField("COMMENTS_CANCELEDBY", StringType, true),
    StructField("COMMENTS_DATE", TimestampType, true)))

registerSchema("PROCEDUREEVENTS_MV.csv","procedureevents_mv",procedureevents_mvSchema,uri,sqlContext)

val procedures_icdSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("SEQ_NUM", IntegerType, true),
    StructField("ICD9_CODE", StringType, true)))

registerSchema("PROCEDURES_ICD.csv","procedures_icd",procedures_icdSchema,uri,sqlContext)

val servicesSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("TRANSFERTIME", TimestampType, true),
    StructField("PREV_SERVICE", StringType, true),
    StructField("CURR_SERVICE", StringType, true)))

registerSchema("SERVICES.csv","services",servicesSchema,uri,sqlContext)

val transfersSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ICUSTAY_ID", IntegerType, true),
    StructField("DBSOURCE", StringType, true),
    StructField("EVENTTYPE", StringType, true),
    StructField("PREV_CAREUNIT", StringType, true),
    StructField("CURR_CAREUNIT", StringType, true),
    StructField("PREV_WARDID", IntegerType, true),
    StructField("CURR_WARDID", IntegerType, true),
    StructField("INTIME", TimestampType, true),
    StructField("OUTTIME", TimestampType, true),
    StructField("LOS", DoubleType, true)))

registerSchema("TRANSFERS.csv","transfers",transfersSchema,uri,sqlContext)

val labeventsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ITEMID", IntegerType, true),
    StructField("CHARTTIME", TimestampType, true),
    StructField("VALUE", StringType, true),
    StructField("VALUENUM", DoubleType, true),
    StructField("VALUEUOM", StringType, true),
    StructField("FLAG", StringType, true)))

registerSchema("LABEVENTS.csv","labevents",labeventsSchema,uri,sqlContext)

val charteventsSchema = StructType(Array(StructField("ROW_ID", IntegerType, true),
    StructField("SUBJECT_ID", IntegerType, true),
    StructField("HADM_ID", IntegerType, true),
    StructField("ICUSTAY_ID", IntegerType, true),
    StructField("ITEMID", IntegerType, true),
    StructField("CHARTTIME", TimestampType, true),
    StructField("STORETIME", TimestampType, true),
    StructField("CGID", IntegerType, true),
    StructField("VALUE", StringType, true),
    StructField("VALUENUM", StringType, true),
    StructField("VALUEUOM", StringType, true),
    StructField("WARNING", StringType, true),
    StructField("ERROR", StringType, true),
    StructField("RESULTSTATUS", StringType, true),
    StructField("STOPPED", StringType, true)))

registerSchema("CHARTEVENTS.csv","chartevents",charteventsSchema,uri,sqlContext)

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

registerSchema("NOTEEVENTS_PROCESSED_2.csv","noteevents",noteeventsSchema,uri,sqlContext)

def writeOutput(df:DataFrame,uri:String,folder:String,tableName:String){
	df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(uri+folder+tableName+".csv")
}

val uofirstday = sqlContext.sql("""select
  -- patient identifiers
  ie.subject_id, ie.hadm_id, ie.icustay_id

  -- volumes associated with urine output ITEMIDs
  , sum(VALUE) as UrineOutput

from icustays ie
-- Join to the outputevents table to get urine output
left join outputevents oe
-- join on all patient identifiers
on ie.subject_id = oe.subject_id and ie.hadm_id = oe.hadm_id and ie.icustay_id = oe.icustay_id
-- and ensure the data occurs during the first day
and oe.charttime between ie.intime and (ie.intime + interval '1' day) -- first ICU day
where itemid in
(
-- these are the most frequently occurring urine output observations in CareVue
40055, -- "Urine Out Foley"
43175, -- "Urine ."
40069, -- "Urine Out Void"
40094, -- "Urine Out Condom Cath"
40715, -- "Urine Out Suprapubic"
40473, -- "Urine Out IleoConduit"
40085, -- "Urine Out Incontinent"
40057, -- "Urine Out Rt Nephrostomy"
40056, -- "Urine Out Lt Nephrostomy"
40405, -- "Urine Out Other"
40428, -- "Urine Out Straight Cath"
40086,--	Urine Out Incontinent
40096, -- "Urine Out Ureteral Stent #1"
40651, -- "Urine Out Ureteral Stent #2"
-- these are the most frequently occurring urine output observations in CareVue
226559, -- "Foley"
226560, -- "Void"
227510, -- "TF Residual"
226561, -- "Condom Cath"
226584, -- "Ileoconduit"
226563, -- "Suprapubic"
226564, -- "R Nephrostomy"
226565, -- "L Nephrostomy"
226567, --	Straight Cath
226557, -- "R Ureteral Stent"
226558  -- "L Ureteral Stent"
)
group by ie.subject_id, ie.hadm_id, ie.icustay_id
order by ie.subject_id, ie.hadm_id, ie.icustay_id
""")
uofirstday.count()
uofirstday.registerTempTable("uofirstday")
writeOutput(uofirstday,uri,folder,"UOFIRSTDAY")

val ventdurations = sqlContext.sql("""
  with ventsettings as (select icustay_id, charttime
     -- case statement determining whether it is an instance of mech vent
    , max(
      case
        when itemid = '' or value = '' then 0 -- can't have null values
        when itemid = 720 and value != 'Other/Remarks' THEN 1  -- VentTypeRecorded
        when itemid = 467 and value = 'Ventilator' THEN 1 -- O2 delivery device == ventilator
        when itemid = 648 and value = 'Intubated/trach' THEN 1 -- Speech = intubated
        when itemid in
          (
          445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
          , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
          , 218,436,535,444,459,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean/Neg insp force ("RespPressure")
          , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
          , 543 -- PlateauPressure
          , 5865,5866,224707,224709,224705,224706 -- APRV pressure
          , 60,437,505,506,686,220339,224700 -- PEEP
          , 3459 -- high pressure relief
          , 501,502,503,224702 -- PCV
          , 223,667,668,669,670,671,672 -- TCPCV
          , 157,158,1852,3398,3399,3400,3401,3402,3403,3404,8382,227809,227810 -- ETT
          , 224701 -- PSVlevel
          )
          THEN 1
        else 0
      end
      ) as MechVent
      , max(
        case when itemid = '' or value = ''  then 0
          when itemid = 640 and value = 'Extubated' then 1
          when itemid = 640 and value = 'Self Extubation' then 1
        else 0
        end
        )
        as Extubated
      , max(
        case when itemid = '' or value = '' then 0
          when itemid = 640 and value = 'Self Extubation' then 1
        else 0
        end
        )
        as SelfExtubated

  from chartevents ce
  where value <> ''
  and itemid in
  (
      640 -- extubated
      , 648 -- speech
      , 720 -- vent type
      , 467 -- O2 delivery device
      , 445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
      , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
      , 218,436,535,444,459,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean/Neg insp force ("RespPressure")
      , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
      , 543 -- PlateauPressure
      , 5865,5866,224707,224709,224705,224706 -- APRV pressure
      , 60,437,505,506,686,220339,224700 -- PEEP
      , 3459 -- high pressure relief
      , 501,502,503,224702 -- PCV
      , 223,667,668,669,670,671,672 -- TCPCV
      , 157,158,1852,3398,3399,3400,3401,3402,3403,3404,8382,227809,227810 -- ETT
      , 224701 -- PSVlevel
  )
  and icustay_id is not null
  group by icustay_id, charttime
  ), vd1 as (
    select icustay_id
    -- this carries over the previous charttime which had a mechanical ventilation event
    , case
        when MechVent=1 then
          LAG(CHARTTIME, 1) OVER (partition by icustay_id, MechVent order by charttime)
        else null
      end as charttime_lag
    , charttime
    , MechVent
    , Extubated
    , SelfExtubated

    -- if this is a mechanical ventilation event, we calculate the time since the last event
    , case
        -- if the current observation indicates mechanical ventilation is present
        when MechVent=1 then
        -- copy over the previous charttime where mechanical ventilation was present
          unix_timestamp(CHARTTIME) - (LAG(unix_timestamp(CHARTTIME), 1) OVER (partition by icustay_id, MechVent order by charttime))
        else null
      end as ventduration

    -- now we determine if the current mech vent event is a "new", i.e. they've just been intubated
    , case
      -- if there is an extubation flag, we mark any subsequent ventilation as a new ventilation event
        when Extubated = 1 then 0 -- extubation is *not* a new ventilation event, the *subsequent* row is
        when
          LAG(Extubated,1)
          OVER
          (
          partition by icustay_id, case when MechVent=1 or Extubated=1 then 1 else 0 end
          order by charttime
          )
          = 1 then 1
          -- if there is less than 8 hours between vent settings, we do not treat this as a new ventilation event
        when (unix_timestamp(CHARTTIME) - (LAG(unix_timestamp(CHARTTIME), 1) OVER (partition by icustay_id, MechVent order by charttime))) <= 28800
          then 0
      else 1
      end as newvent
  FROM
    ventsettings
  ), vd2 as (
    select vd1.*
-- create a cumulative sum of the instances of new ventilation
-- this results in a monotonic integer assigned to each instance of ventilation
, case when MechVent=1 or Extubated = 1 then
    SUM( newvent )
    OVER ( partition by icustay_id order by charttime )
  else '' end
  as ventnum
from vd1
-- now we can isolate to just rows with ventilation settings/extubation settings
-- (before we had rows with extubation flags)
-- this removes any null values for newvent
where
  MechVent = 1 or Extubated = 1
  )

select icustay_id, ventnum
  , min(charttime) as starttime
  , max(charttime) as endtime
from vd2
group by icustay_id, ventnum
order by icustay_id, ventnum
""")
writeOutput(ventdurations,uri,folder,"VENTDURATIONS")
ventdurations.registerTempTable("ventdurations")

val ventfirstday = sqlContext.sql("""
  with tt_ventfirstday as (
select
  ie.subject_id, ie.icustay_id, ie.hadm_id, ce.charttime
  , ce.itemid, di.label, ce.value
  , case
      when ce.itemid = 720 and value != 'Other/Remarks' then 1
    else 0 end as VentTypeRecorded -- VentType

  , case
      when ce.itemid in (445, 448, 449, 450, 1340, 1486, 1600, 224687) then 1 -- minute volume
    else 0 end as MinuteVolume -- MinuteVolume

  , case
      when ce.itemid in (639, 654, 681, 682, 683, 684,224685,224684,224686) then 1 -- tidal volume
    else 0 end as TV -- TidalVolume

  , case
      when ce.itemid in (218,436,535,444,459,224697,224695,224696,224746,224747) then 1 -- High/Low/Peak/Mean/Neg insp force
    else 0 end as RespPressure
  , case
      when ce.itemid in (221,1,1211,1655,2000,226873,224738,224419,224750,227187) then 1 -- Insp pressure
    else 0 end as InspPressure

  , case
      when ce.itemid in (543) then 1 -- PlateauPressure
    else 0 end as PlateauPressure

  , case
      when ce.itemid in (5865,5866,224707,224709,224705,224706) then 1 -- APRV pressure
    else 0 end as APRVPressure

  , case
      when ce.itemid in (60,437,505,506,686,220339,224700) then 1 -- peep
    else 0 end as PEEP

  , case
      when ce.itemid in (141,224417,224418) then 1 -- cuff volume/pressure
    else 0 end as CuffPressure

  , case
      when ce.itemid in (3459) then 1 -- high pressure relief
    else 0 end as HighPressureRelease

  , case
      when ce.itemid in (501,502,503,224702) then 1 -- PCV
    else 0 end as PCV

  , case
      when ce.itemid in (223,667,668,669,670,671,672) then 1 -- TCPCV
    else 0 end as TCPCV -- TCPCV

  , case
      when ce.itemid = 467 and ce.value = 'Ventilator' then 1 -- O2 delivery device == ventilator
    else 0 end as O2FromVentilator

  , case
      when ce.itemid in (578,3605) then 1 -- Pressure/Respiratory
    else 0 end as PressureSupport -- Pressure/Respiratory

  , case
      when ce.itemid in (3688,3689) then 1 -- Vt [Ventilator] and Vt [Spontaneous] - measured in inches??? :S
    else 0 end as VtInches

  , case
      when ce.itemid in (63,64,65,66,67,68
        ,227579,227580,227582,227581) then 1 -- BIPAP
    else 0 end as BIPAP --

  , case
      when ce.itemid in (157,158,1852,3398,3399,3400,3401,3402,3403,3404,8382
          ,227809,227810) then 1 -- ETT
    else 0 end as ETT --

  , case
      when ce.itemid in (224701) then 1 else 0 end PSVlevel

  , case
      when ce.itemid in (223835) then 1 -- FiO2
    else 0 end as FiO2

  , case
      when ce.itemid in (614,615,619,653,224688,224690,224689) then 1
    else 0 end as RespRate -- RespRate
  , case
      when ce.itemid in (194, 195, 224691) then 1 -- flow by
    else 0 end as FlowBy

  , case
      when ce.itemid in (470,471,223834,227287) then 1 -- O2 FLOW
    else 0 end as O2Flow

  , case
      when ce.itemid = 648 and value = 'Intubated/trach' THEN 1 -- Speech = intubated
    else 0 end as SpeechIntubated
from icustays ie
left join chartevents ce
  on ie.icustay_id = ce.icustay_id and ce.value <> ''
  -- only first day of their ICU stay
  and ce.charttime between ie.intime and ie.intime + interval '1' day
left join d_items di
  on ce.itemid = di.itemid
)

select
  subject_id, hadm_id, icustay_id
  , max(case
      -- if no chart data matched, they are not ventilated
      when value = ''
      then 0
      when (VentTypeRecorded + MinuteVolume + TV + InspPressure
        + PlateauPressure + APRVPressure + PEEP + HighPressureRelease
        + PCV + TCPCV + RespPressure + psvlevel + ett + O2FromVentilator) > 0
      then 1
      else 0
      end) as MechVent
from tt_ventfirstday
group by subject_id, hadm_id, icustay_id
order by subject_id, hadm_id, icustay_id

""")
writeOutput(ventfirstday,uri,folder,"VENTFIRSTDAY")
ventfirstday.registerTempTable("ventfirstday")

val vitalsfirstday = sqlContext.sql("""
with vital_pvt as (
 select ie.subject_id, ie.hadm_id, ie.icustay_id , case
    when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 -- HeartRate
    when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2 -- SysBP
    when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 -- DiasBP
    when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4 -- MeanBP
    when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5 -- RespRate
    when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 -- TempF, converted to degC in valuenum call
    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 -- TempC
    when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7 -- SpO2
    when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8 -- Glucose
    else null end as VitalID
      -- convert F to C
  , case when itemid in (223761,678) then (valuenum-32)/1.8 else valuenum end as valuenum
  from icustays ie
  left join chartevents ce
  on ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and ie.icustay_id = ce.icustay_id
  and ce.charttime between ie.intime and ie.intime + interval '1' day
  where ce.itemid in
  ( -- HEART RATE
  211, --"Heart Rate"
  220045, --"Heart Rate"
  -- Systolic/diastolic
  51, --  Arterial BP [Systolic]
  442, -- Manual BP [Systolic]
  455, -- NBP [Systolic]
  6701, --  Arterial BP #2 [Systolic]
  220179, --  Non Invasive Blood Pressure systolic
  220050, --  Arterial Blood Pressure systolic
  8368, --  Arterial BP [Diastolic]
  8440, --  Manual BP [Diastolic]
  8441, --  NBP [Diastolic]
  8555, --  Arterial BP #2 [Diastolic]
  220180, --  Non Invasive Blood Pressure diastolic
  220051, --  Arterial Blood Pressure diastolic
  -- MEAN ARTERIAL PRESSURE
  456, --"NBP Mean"
  52, --"Arterial BP Mean"
  6702, --  Arterial BP Mean #2
  443, -- Manual BP Mean(calc)
  220052, --"Arterial Blood Pressure mean"
  220181, --"Non Invasive Blood Pressure mean"
  225312, --"ART BP mean"
  -- RESPIRATORY RATE
  618,--  Respiratory Rate
  615,--  Resp Rate (Total)
  220210,-- Respiratory Rate
  224690, --  Respiratory Rate (Total)
  -- SPO2, peripheral
  646, 220277,
  -- GLUCOSE, both lab and fingerstick
  807,--  Fingerstick Glucose
  811,--  Glucose (70-105)
  1529,-- Glucose
  3745,-- BloodGlucose
  3744,-- Blood Glucose
  225664,-- Glucose finger stick
  220621,-- Glucose (serum)
  226537,-- Glucose (whole blood)
  -- TEMPERATURE
  223762, -- "Temperature Celsius"
  676,  -- "Temperature C"
  223761, -- "Temperature Fahrenheit"
  678 --  "Temperature F"
  )
) 

SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id
-- Easier names
, min(case when VitalID = 1 then valuenum else null end) as HeartRate_Min
, max(case when VitalID = 1 then valuenum else null end) as HeartRate_Max
, min(case when VitalID = 2 then valuenum else null end) as SysBP_Min
, max(case when VitalID = 2 then valuenum else null end) as SysBP_Max
, min(case when VitalID = 3 then valuenum else null end) as DiasBP_Min
, max(case when VitalID = 3 then valuenum else null end) as DiasBP_Max
, min(case when VitalID = 4 then valuenum else null end) as MeanBP_Min
, max(case when VitalID = 4 then valuenum else null end) as MeanBP_Max
, min(case when VitalID = 5 then valuenum else null end) as RespRate_Min
, max(case when VitalID = 5 then valuenum else null end) as RespRate_Max
, min(case when VitalID = 6 then valuenum else null end) as TempC_Min
, max(case when VitalID = 6 then valuenum else null end) as TempC_Max
, min(case when VitalID = 7 then valuenum else null end) as SpO2_Min
, max(case when VitalID = 7 then valuenum else null end) as SpO2_Max
, min(case when VitalID = 8 then valuenum else null end) as Glucose_Min
, max(case when VitalID = 8 then valuenum else null end) as Glucose_Max
FROM vital_pvt as pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
""")
vitalsfirstday.registerTempTable("vitalsfirstday")
writeOutput(vitalsfirstday,uri,folder,"VITALSFIRSTDAY")

val rrtfirstday = sqlContext.sql("""
with rrt_cv as (
  select ie.icustay_id
    , max(
        case
          when ce.itemid in (152,148,149,146,147,151,150) and value is not null then 1
          when ce.itemid in (229,235,241,247,253,259,265,271) and value = 'Dialysis Line' then 1
          when ce.itemid = 582 and value in ('CAVH Start','CAVH D/C','CVVHD Start','CVVHD D/C','Hemodialysis st','Hemodialysis end') then 1
        else 0 end
        ) as RRT
  from icustays ie
  inner join chartevents ce
    on ie.icustay_id = ce.icustay_id
    and ce.itemid in
    (
       152 -- "Dialysis Type";61449
      ,148 -- "Dialysis Access Site";60335
      ,149 -- "Dialysis Access Type";60030
      ,146 -- "Dialysate Flow ml/hr";57445
      ,147 -- "Dialysate Infusing";56605
      ,151 -- "Dialysis Site Appear";37345
      ,150 -- "Dialysis Machine";27472
      ,229 -- INV Line#1 [Type]
      ,235 -- INV Line#2 [Type]
      ,241 -- INV Line#3 [Type]
      ,247 -- INV Line#4 [Type]
      ,253 -- INV Line#5 [Type]
      ,259 -- INV Line#6 [Type]
      ,265 -- INV Line#7 [Type]
      ,271 -- INV Line#8 [Type]
      ,582 -- Procedures
    )
    and ce.value <> ''
    and ce.charttime between ie.intime and ie.intime + interval '1' day
  where ie.dbsource = 'carevue'
  group by ie.icustay_id
), rrt_mv_ce as (
select ie.icustay_id
    , 1 as RRT
  from icustays ie
  inner join chartevents ce
    on ie.icustay_id = ce.icustay_id
    and ce.charttime between ie.intime and ie.intime + interval '1' day
    and itemid in
    (
      -- Checkboxes
        225126 -- | Dialysis patient                                  | Adm History/FHPA        | chartevents        | Checkbox
      , 226118 -- | Dialysis Catheter placed in outside facility      | Access Lines - Invasive | chartevents        | Checkbox
      , 227357 -- | Dialysis Catheter Dressing Occlusive              | Access Lines - Invasive | chartevents        | Checkbox
      , 225725 -- | Dialysis Catheter Tip Cultured                    | Access Lines - Invasive | chartevents        | Checkbox
      -- Numeric values
      , 226499 -- | Hemodialysis Output                               | Dialysis                | chartevents        | Numeric
      , 224154 -- | Dialysate Rate                                    | Dialysis                | chartevents        | Numeric
      , 225810 -- | Dwell Time (Peritoneal Dialysis)                  | Dialysis                | chartevents        | Numeric
      , 227639 -- | Medication Added Amount  #2 (Peritoneal Dialysis) | Dialysis                | chartevents        | Numeric
      , 225183 -- | Current Goal                     | Dialysis | chartevents        | Numeric
      , 227438 -- | Volume not removed               | Dialysis | chartevents        | Numeric
      , 224191 -- | Hourly Patient Fluid Removal     | Dialysis | chartevents        | Numeric
      , 225806 -- | Volume In (PD)                   | Dialysis | chartevents        | Numeric
      , 225807 -- | Volume Out (PD)                  | Dialysis | chartevents        | Numeric
      , 228004 -- | Citrate (ACD-A)                  | Dialysis | chartevents        | Numeric
      , 228005 -- | PBP (Prefilter) Replacement Rate | Dialysis | chartevents        | Numeric
      , 228006 -- | Post Filter Replacement Rate     | Dialysis | chartevents        | Numeric
      , 224144 -- | Blood Flow (ml/min)              | Dialysis | chartevents        | Numeric
      , 224145 -- | Heparin Dose (per hour)          | Dialysis | chartevents        | Numeric
      , 224149 -- | Access Pressure                  | Dialysis | chartevents        | Numeric
      , 224150 -- | Filter Pressure                  | Dialysis | chartevents        | Numeric
      , 224151 -- | Effluent Pressure                | Dialysis | chartevents        | Numeric
      , 224152 -- | Return Pressure                  | Dialysis | chartevents        | Numeric
      , 224153 -- | Replacement Rate                 | Dialysis | chartevents        | Numeric
      , 224404 -- | ART Lumen Volume                 | Dialysis | chartevents        | Numeric
      , 224406 -- | VEN Lumen Volume                 | Dialysis | chartevents        | Numeric
      , 226457 -- | Ultrafiltrate Output             | Dialysis | chartevents        | Numeric
    )
    and valuenum > 0 -- also ensures it's not null
  group by ie.icustay_id
), rrt_mv_ie as (
  select ie.icustay_id
    , 1 as RRT
  from icustays ie
  inner join inputevents_mv tt
    on ie.icustay_id = tt.icustay_id
    and tt.starttime between ie.intime and ie.intime + interval '1' day
    and itemid in
    (
        227536 -- KCl (CRRT)  Medications inputevents_mv  Solution
      , 227525 -- Calcium Gluconate (CRRT)  Medications inputevents_mv  Solution
    )
    and amount > 0 -- also ensures it's not null
  group by ie.icustay_id
  ), rrt_mv_de as (
  select ie.icustay_id
    , 1 as RRT
  from icustays ie
  inner join datetimeevents tt
    on ie.icustay_id = tt.icustay_id
    and tt.charttime between ie.intime and ie.intime + interval '1' day
    and itemid in
    (
      -- TODO: unsure how to handle "Last dialysis"
      --  225128 -- | Last dialysis                                     | Adm History/FHPA        | datetimeevents     | Date time
        225318 -- | Dialysis Catheter Cap Change                      | Access Lines - Invasive | datetimeevents     | Date time
      , 225319 -- | Dialysis Catheter Change over Wire Date           | Access Lines - Invasive | datetimeevents     | Date time
      , 225321 -- | Dialysis Catheter Dressing Change                 | Access Lines - Invasive | datetimeevents     | Date time
      , 225322 -- | Dialysis Catheter Insertion Date                  | Access Lines - Invasive | datetimeevents     | Date time
      , 225324 -- | Dialysis CatheterTubing Change                    | Access Lines - Invasive | datetimeevents     | Date time
    )
  group by ie.icustay_id
  ), rrt_mv_pe as (
    select ie.icustay_id
      , 1 as RRT
    from icustays ie
    inner join procedureevents_mv tt
      on ie.icustay_id = tt.icustay_id
      and tt.starttime between ie.intime and ie.intime + interval '1' day
      and itemid in
      (
          225441 -- | Hemodialysis                                      | 4-Procedures            | procedureevents_mv | Process
        , 225802 -- | Dialysis - CRRT                                   | Dialysis                | procedureevents_mv | Process
        , 225803 -- | Dialysis - CVVHD                                  | Dialysis                | procedureevents_mv | Process
        , 225805 -- | Peritoneal Dialysis                               | Dialysis                | procedureevents_mv | Process
        , 224270 -- | Dialysis Catheter                                 | Access Lines - Invasive | procedureevents_mv | Process
        , 225809 -- | Dialysis - CVVHDF                                 | Dialysis                | procedureevents_mv | Process
        , 225955 -- | Dialysis - SCUF                                   | Dialysis                | procedureevents_mv | Process
        , 225436 -- | CRRT Filter Change               | Dialysis | procedureevents_mv | Process
      )
    group by ie.icustay_id
  )

select ie.subject_id, ie.hadm_id, ie.icustay_id
  , case
      when rrt_cv.RRT = 1 then 1
      when rrt_mv_ce.RRT = 1 then 1
      when rrt_mv_ie.RRT = 1 then 1
      when rrt_mv_de.RRT = 1 then 1
      when rrt_mv_pe.RRT = 1 then 1
      else 0
    end as RRT
from icustays ie
left join rrt_cv
  on ie.icustay_id = rrt_cv.icustay_id
left join rrt_mv_ce
  on ie.icustay_id = rrt_mv_ce.icustay_id
left join rrt_mv_ie
  on ie.icustay_id = rrt_mv_ie.icustay_id
left join rrt_mv_de
  on ie.icustay_id = rrt_mv_de.icustay_id
left join rrt_mv_pe
  on ie.icustay_id = rrt_mv_pe.icustay_id
order by ie.icustay_id
""")
rrtfirstday.registerTempTable("rrtfirstday")
writeOutput(rrtfirstday,uri,folder,"RRTFIRSTDAY")

val rrt = sqlContext.sql("""
with cv as
(
  select ie.icustay_id
    , max(
        case
          when ce.itemid in (152,148,149,146,147,151,150) and value is not null then 1
          when ce.itemid in (229,235,241,247,253,259,265,271) and value = 'Dialysis Line' then 1
          when ce.itemid = 582 and value in ('CAVH Start','CAVH D/C','CVVHD Start','CVVHD D/C','Hemodialysis st','Hemodialysis end') then 1
        else 0 end
        ) as RRT
  from icustays ie
  inner join chartevents ce
    on ie.icustay_id = ce.icustay_id
    and ce.itemid in
    (
       152 -- "Dialysis Type";61449
      ,148 -- "Dialysis Access Site";60335
      ,149 -- "Dialysis Access Type";60030
      ,146 -- "Dialysate Flow ml/hr";57445
      ,147 -- "Dialysate Infusing";56605
      ,151 -- "Dialysis Site Appear";37345
      ,150 -- "Dialysis Machine";27472
      ,229 -- INV Line#1 [Type]
      ,235 -- INV Line#2 [Type]
      ,241 -- INV Line#3 [Type]
      ,247 -- INV Line#4 [Type]
      ,253 -- INV Line#5 [Type]
      ,259 -- INV Line#6 [Type]
      ,265 -- INV Line#7 [Type]
      ,271 -- INV Line#8 [Type]
      ,582 -- Procedures
    )
    and ce.value is not null
  where ie.dbsource = 'carevue'
  group by ie.icustay_id
)
, mv_ce as
(
  select icustay_id
    , 1 as RRT
  from chartevents ce
  where itemid in
  (
    -- Checkboxes
      225126 -- | Dialysis patient                                  | Adm History/FHPA        | chartevents        | Checkbox
    , 226118 -- | Dialysis Catheter placed in outside facility      | Access Lines - Invasive | chartevents        | Checkbox
    , 227357 -- | Dialysis Catheter Dressing Occlusive              | Access Lines - Invasive | chartevents        | Checkbox
    , 225725 -- | Dialysis Catheter Tip Cultured                    | Access Lines - Invasive | chartevents        | Checkbox
    -- Numeric values
    , 226499 -- | Hemodialysis Output                               | Dialysis                | chartevents        | Numeric
    , 224154 -- | Dialysate Rate                                    | Dialysis                | chartevents        | Numeric
    , 225810 -- | Dwell Time (Peritoneal Dialysis)                  | Dialysis                | chartevents        | Numeric
    , 227639 -- | Medication Added Amount  #2 (Peritoneal Dialysis) | Dialysis                | chartevents        | Numeric
    , 225183 -- | Current Goal                     | Dialysis | chartevents        | Numeric
    , 227438 -- | Volume not removed               | Dialysis | chartevents        | Numeric
    , 224191 -- | Hourly Patient Fluid Removal     | Dialysis | chartevents        | Numeric
    , 225806 -- | Volume In (PD)                   | Dialysis | chartevents        | Numeric
    , 225807 -- | Volume Out (PD)                  | Dialysis | chartevents        | Numeric
    , 228004 -- | Citrate (ACD-A)                  | Dialysis | chartevents        | Numeric
    , 228005 -- | PBP (Prefilter) Replacement Rate | Dialysis | chartevents        | Numeric
    , 228006 -- | Post Filter Replacement Rate     | Dialysis | chartevents        | Numeric
    , 224144 -- | Blood Flow (ml/min)              | Dialysis | chartevents        | Numeric
    , 224145 -- | Heparin Dose (per hour)          | Dialysis | chartevents        | Numeric
    , 224149 -- | Access Pressure                  | Dialysis | chartevents        | Numeric
    , 224150 -- | Filter Pressure                  | Dialysis | chartevents        | Numeric
    , 224151 -- | Effluent Pressure                | Dialysis | chartevents        | Numeric
    , 224152 -- | Return Pressure                  | Dialysis | chartevents        | Numeric
    , 224153 -- | Replacement Rate                 | Dialysis | chartevents        | Numeric
    , 224404 -- | ART Lumen Volume                 | Dialysis | chartevents        | Numeric
    , 224406 -- | VEN Lumen Volume                 | Dialysis | chartevents        | Numeric
    , 226457 -- | Ultrafiltrate Output             | Dialysis | chartevents        | Numeric
  )
  and valuenum > 0 -- also ensures it's not null
  group by icustay_id
)
, mv_ie as
(
  select icustay_id
    , 1 as RRT
  from inputevents_mv
  where itemid in
  (
      227536 -- KCl (CRRT)  Medications inputevents_mv  Solution
    , 227525 -- Calcium Gluconate (CRRT)  Medications inputevents_mv  Solution
  )
  and amount > 0 -- also ensures it's not null
  group by icustay_id
)
, mv_de as
(
  select icustay_id
    , 1 as RRT
  from datetimeevents
  where itemid in
  (
    -- TODO: unsure how to handle "Last dialysis"
    --  225128 -- | Last dialysis                                     | Adm History/FHPA        | datetimeevents     | Date time
      225318 -- | Dialysis Catheter Cap Change                      | Access Lines - Invasive | datetimeevents     | Date time
    , 225319 -- | Dialysis Catheter Change over Wire Date           | Access Lines - Invasive | datetimeevents     | Date time
    , 225321 -- | Dialysis Catheter Dressing Change                 | Access Lines - Invasive | datetimeevents     | Date time
    , 225322 -- | Dialysis Catheter Insertion Date                  | Access Lines - Invasive | datetimeevents     | Date time
    , 225324 -- | Dialysis CatheterTubing Change                    | Access Lines - Invasive | datetimeevents     | Date time
  )
  group by icustay_id
)
, mv_pe as
(
    select icustay_id
      , 1 as RRT
    from procedureevents_mv
    where itemid in
    (
        225441 -- | Hemodialysis                                      | 4-Procedures            | procedureevents_mv | Process
      , 225802 -- | Dialysis - CRRT                                   | Dialysis                | procedureevents_mv | Process
      , 225803 -- | Dialysis - CVVHD                                  | Dialysis                | procedureevents_mv | Process
      , 225805 -- | Peritoneal Dialysis                               | Dialysis                | procedureevents_mv | Process
      , 224270 -- | Dialysis Catheter                                 | Access Lines - Invasive | procedureevents_mv | Process
      , 225809 -- | Dialysis - CVVHDF                                 | Dialysis                | procedureevents_mv | Process
      , 225955 -- | Dialysis - SCUF                                   | Dialysis                | procedureevents_mv | Process
      , 225436 -- | CRRT Filter Change               | Dialysis | procedureevents_mv | Process
    )
    group by icustay_id
)
select ie.subject_id, ie.hadm_id, ie.icustay_id
  , case
      when cv.RRT = 1 then 1
      when mv_ce.RRT = 1 then 1
      when mv_ie.RRT = 1 then 1
      when mv_de.RRT = 1 then 1
      when mv_pe.RRT = 1 then 1
      else 0
    end as RRT
from icustays ie
left join cv
  on ie.icustay_id = cv.icustay_id
left join mv_ce
  on ie.icustay_id = mv_ce.icustay_id
left join mv_ie
  on ie.icustay_id = mv_ie.icustay_id
left join mv_de
  on ie.icustay_id = mv_de.icustay_id
left join mv_pe
  on ie.icustay_id = mv_pe.icustay_id
order by ie.icustay_id
""")
rrt.registerTempTable("rrt")
writeOutput(rrt,uri,folder,"RTT")


val labsfirstday = sqlContext.sql("""
with lab_pvt as (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  -- here we assign labels to ITEMIDs
  -- this also fuses together multiple ITEMIDs containing the same data
  , case
        when itemid = 50868 then 'ANION GAP'
        when itemid = 50862 then 'ALBUMIN'
        when itemid = 50882 then 'BICARBONATE'
        when itemid = 50885 then 'BILIRUBIN'
        when itemid = 50912 then 'CREATININE'
        when itemid = 50806 then 'CHLORIDE'
        when itemid = 50902 then 'CHLORIDE'
        when itemid = 50809 then 'GLUCOSE'
        when itemid = 50931 then 'GLUCOSE'
        when itemid = 50810 then 'HEMATOCRIT'
        when itemid = 51221 then 'HEMATOCRIT'
        when itemid = 50811 then 'HEMOGLOBIN'
        when itemid = 51222 then 'HEMOGLOBIN'
        when itemid = 50813 then 'LACTATE'
        when itemid = 51265 then 'PLATELET'
        when itemid = 50822 then 'POTASSIUM'
        when itemid = 50971 then 'POTASSIUM'
        when itemid = 51275 then 'PTT'
        when itemid = 51237 then 'INR'
        when itemid = 51274 then 'PT'
        when itemid = 50824 then 'SODIUM'
        when itemid = 50983 then 'SODIUM'
        when itemid = 51006 then 'BUN'
        when itemid = 51300 then 'WBC'
        when itemid = 51301 then 'WBC'
      else null
    end as label
  , -- add in some sanity checks on the values
  -- the where clause below requires all valuenum to be > 0, so these are only upper limit checks
    case
      when itemid = 50862 and valuenum >    10 then null -- g/dL 'ALBUMIN'
      when itemid = 50868 and valuenum > 10000 then null -- mEq/L 'ANION GAP'
      when itemid = 50882 and valuenum > 10000 then null -- mEq/L 'BICARBONATE'
      when itemid = 50885 and valuenum >   150 then null -- mg/dL 'BILIRUBIN'
      when itemid = 50806 and valuenum > 10000 then null -- mEq/L 'CHLORIDE'
      when itemid = 50902 and valuenum > 10000 then null -- mEq/L 'CHLORIDE'
      when itemid = 50912 and valuenum >   150 then null -- mg/dL 'CREATININE'
      when itemid = 50809 and valuenum > 10000 then null -- mg/dL 'GLUCOSE'
      when itemid = 50931 and valuenum > 10000 then null -- mg/dL 'GLUCOSE'
      when itemid = 50810 and valuenum >   100 then null -- % 'HEMATOCRIT'
      when itemid = 51221 and valuenum >   100 then null -- % 'HEMATOCRIT'
      when itemid = 50811 and valuenum >    50 then null -- g/dL 'HEMOGLOBIN'
      when itemid = 51222 and valuenum >    50 then null -- g/dL 'HEMOGLOBIN'
      when itemid = 50813 and valuenum >    50 then null -- mmol/L 'LACTATE'
      when itemid = 51265 and valuenum > 10000 then null -- K/uL 'PLATELET'
      when itemid = 50822 and valuenum >    30 then null -- mEq/L 'POTASSIUM'
      when itemid = 50971 and valuenum >    30 then null -- mEq/L 'POTASSIUM'
      when itemid = 51275 and valuenum >   150 then null -- sec 'PTT'
      when itemid = 51237 and valuenum >    50 then null -- 'INR'
      when itemid = 51274 and valuenum >   150 then null -- sec 'PT'
      when itemid = 50824 and valuenum >   200 then null -- mEq/L == mmol/L 'SODIUM'
      when itemid = 50983 and valuenum >   200 then null -- mEq/L == mmol/L 'SODIUM'
      when itemid = 51006 and valuenum >   300 then null -- 'BUN'
      when itemid = 51300 and valuenum >  1000 then null -- 'WBC'
      when itemid = 51301 and valuenum >  1000 then null -- 'WBC'
    else le.valuenum
    end as valuenum

  from icustays ie

  left join labevents le
    on le.subject_id = ie.subject_id and le.hadm_id = ie.hadm_id
    and le.charttime between (ie.intime - interval '6' hour) and (ie.intime + interval '1' day)
    and le.ITEMID in
    (
      -- comment is: LABEL | CATEGORY | FLUID | NUMBER OF ROWS IN LABEVENTS
      50868, -- ANION GAP | CHEMISTRY | BLOOD | 769895
      50862, -- ALBUMIN | CHEMISTRY | BLOOD | 146697
      50882, -- BICARBONATE | CHEMISTRY | BLOOD | 780733
      50885, -- BILIRUBIN, TOTAL | CHEMISTRY | BLOOD | 238277
      50912, -- CREATININE | CHEMISTRY | BLOOD | 797476
      50902, -- CHLORIDE | CHEMISTRY | BLOOD | 795568
      50806, -- CHLORIDE, WHOLE BLOOD | BLOOD GAS | BLOOD | 48187
      50931, -- GLUCOSE | CHEMISTRY | BLOOD | 748981
      50809, -- GLUCOSE | BLOOD GAS | BLOOD | 196734
      51221, -- HEMATOCRIT | HEMATOLOGY | BLOOD | 881846
      50810, -- HEMATOCRIT, CALCULATED | BLOOD GAS | BLOOD | 89715
      51222, -- HEMOGLOBIN | HEMATOLOGY | BLOOD | 752523
      50811, -- HEMOGLOBIN | BLOOD GAS | BLOOD | 89712
      50813, -- LACTATE | BLOOD GAS | BLOOD | 187124
      51265, -- PLATELET COUNT | HEMATOLOGY | BLOOD | 778444
      50971, -- POTASSIUM | CHEMISTRY | BLOOD | 845825
      50822, -- POTASSIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 192946
      51275, -- PTT | HEMATOLOGY | BLOOD | 474937
      51237, -- INR(PT) | HEMATOLOGY | BLOOD | 471183
      51274, -- PT | HEMATOLOGY | BLOOD | 469090
      50983, -- SODIUM | CHEMISTRY | BLOOD | 808489
      50824, -- SODIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 71503
      51006, -- UREA NITROGEN | CHEMISTRY | BLOOD | 791925
      51301, -- WHITE BLOOD CELLS | HEMATOLOGY | BLOOD | 753301
      51300  -- WBC COUNT | HEMATOLOGY | BLOOD | 2371
    )
    and valuenum is not null and valuenum > 0 -- lab values cannot be 0 and cannot be negative
)

select
  pvt.subject_id, pvt.hadm_id, pvt.icustay_id
  , min(case when label = 'ANION GAP' then valuenum else null end) as ANIONGAP_min
  , max(case when label = 'ANION GAP' then valuenum else null end) as ANIONGAP_max
  , min(case when label = 'ALBUMIN' then valuenum else null end) as ALBUMIN_min
  , max(case when label = 'ALBUMIN' then valuenum else null end) as ALBUMIN_max
  , min(case when label = 'BICARBONATE' then valuenum else null end) as BICARBONATE_min
  , max(case when label = 'BICARBONATE' then valuenum else null end) as BICARBONATE_max
  , min(case when label = 'BILIRUBIN' then valuenum else null end) as BILIRUBIN_min
  , max(case when label = 'BILIRUBIN' then valuenum else null end) as BILIRUBIN_max
  , min(case when label = 'CREATININE' then valuenum else null end) as CREATININE_min
  , max(case when label = 'CREATININE' then valuenum else null end) as CREATININE_max
  , min(case when label = 'CHLORIDE' then valuenum else null end) as CHLORIDE_min
  , max(case when label = 'CHLORIDE' then valuenum else null end) as CHLORIDE_max
  , min(case when label = 'GLUCOSE' then valuenum else null end) as GLUCOSE_min
  , max(case when label = 'GLUCOSE' then valuenum else null end) as GLUCOSE_max
  , min(case when label = 'HEMATOCRIT' then valuenum else null end) as HEMATOCRIT_min
  , max(case when label = 'HEMATOCRIT' then valuenum else null end) as HEMATOCRIT_max
  , min(case when label = 'HEMOGLOBIN' then valuenum else null end) as HEMOGLOBIN_min
  , max(case when label = 'HEMOGLOBIN' then valuenum else null end) as HEMOGLOBIN_max
  , min(case when label = 'LACTATE' then valuenum else null end) as LACTATE_min
  , max(case when label = 'LACTATE' then valuenum else null end) as LACTATE_max
  , min(case when label = 'PLATELET' then valuenum else null end) as PLATELET_min
  , max(case when label = 'PLATELET' then valuenum else null end) as PLATELET_max
  , min(case when label = 'POTASSIUM' then valuenum else null end) as POTASSIUM_min
  , max(case when label = 'POTASSIUM' then valuenum else null end) as POTASSIUM_max
  , min(case when label = 'PTT' then valuenum else null end) as PTT_min
  , max(case when label = 'PTT' then valuenum else null end) as PTT_max
  , min(case when label = 'INR' then valuenum else null end) as INR_min
  , max(case when label = 'INR' then valuenum else null end) as INR_max
  , min(case when label = 'PT' then valuenum else null end) as PT_min
  , max(case when label = 'PT' then valuenum else null end) as PT_max
  , min(case when label = 'SODIUM' then valuenum else null end) as SODIUM_min
  , max(case when label = 'SODIUM' then valuenum else null end) as SODIUM_max
  , min(case when label = 'BUN' then valuenum else null end) as BUN_min
  , max(case when label = 'BUN' then valuenum else null end) as BUN_max
  , min(case when label = 'WBC' then valuenum else null end) as WBC_min
  , max(case when label = 'WBC' then valuenum else null end) as WBC_max
from lab_pvt as pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
""")
labsfirstday.registerTempTable("labsfirstday")
writeOutput(labsfirstday,uri,folder,"LABSFIRSTDAY")


val echodata = sqlContext.sql("""
SELECT ROW_ID
  , subject_id, hadm_id
  , chartdate
, CAST(CONCAT(regexp_extract(ne.text, 'Date/Time: \\[\\*\\*([0-9*-]+)\\*\\*\\] at [0-9:]+ NEWLINE'),' ',regexp_extract(ne.text, 'Date/Time: [\\[\\]0-9*-]+ at ([0-9:]+) NEWLINE')) AS timestamp) as charttime,
regexp_extract(ne.text, 'Indication: (.*?) NEWLINE') as Indication,
case
    when regexp_extract(ne.text, 'Height: \\(in\\) (.*?) NEWLINE') like '%*%'
        then null
    else cast(regexp_extract(ne.text, 'Height: \\(in\\) (.*?) NEWLINE') as FLOAT)
end as Height,
case
    when regexp_extract(ne.text, 'Weight \\(lb\\): (.*?) NEWLINE') like '%*%'
        then null
    else cast(regexp_extract(ne.text, 'Weight \\(lb\\): (.*?) NEWLINE') as FLOAT)
end as Weight,
case
    when regexp_extract(ne.text, 'BSA \\(m2\\): (.*?) m2 NEWLINE') like '%*%'
        then null
    else cast(regexp_extract(ne.text, 'BSA \\(m2\\): (.*?) m2 NEWLINE') as FLOAT)
end as BSA,
regexp_extract(ne.text, 'BP \\(mm Hg\\): (.*?) NEWLINE') as BP,
case
    when regexp_extract(ne.text, 'BP \\(mm Hg\\): ([0-9]+)/[0-9]+? NEWLINE') like '%*%'
        then null
    else cast(regexp_extract(ne.text, 'BP \\(mm Hg\\): ([0-9]+)/[0-9]+? NEWLINE') as FLOAT)
end as BPSys,
case
    when regexp_extract(ne.text, 'BP \\(mm Hg\\): [0-9]+/([0-9]+?) NEWLINE') like '%*%'
        then null
    else cast(regexp_extract(ne.text, 'BP \\(mm Hg\\): [0-9]+/([0-9]+?) NEWLINE') as FLOAT)
end as BPDias,
case
    when regexp_extract(ne.text, 'HR \\(bpm\\): ([0-9]+?) NEWLINE') like '%*%'
        then null
    else cast(regexp_extract(ne.text, 'HR \\(bpm\\): ([0-9]+?) NEWLINE') as FLOAT)
end as HR,
regexp_extract(ne.text, 'Status: (.*?) NEWLINE') as Status, 
regexp_extract(ne.text, 'Test: (.*?) NEWLINE') as Test, 
regexp_extract(ne.text, 'Doppler: (.*?) NEWLINE') as Doppler, 
regexp_extract(ne.text,'Contrast: (.*?) NEWLINE') as Contrast, 
regexp_extract(ne.text, 'Technical Quality: (.*?) NEWLINE') as TechnicalQuality
FROM noteevents ne where category = 'Echo'
""")
echodata.registerTempTable("echodata")
writeOutput(echodata,uri,folder,"ECHODATA")

val gcsfirstday = sqlContext.sql("""
with gcs_base_pvt as (
    select l.ICUSTAY_ID
  -- merge the ITEMIDs so that the pivot applies to both metavision/carevue data
  , case
      when l.ITEMID in (723,223900) then 723
      when l.ITEMID in (454,223901) then 454
      when l.ITEMID in (184,220739) then 184
      else l.ITEMID end
    as ITEMID

  -- convert the data into a number, reserving a value of 0 for ET/Trach
  , case
      -- endotrach/vent is assigned a value of 0, later parsed specially
      when l.ITEMID = 723 and l.VALUE = '1.0 ET/Trach' then 0 -- carevue
      when l.ITEMID = 223900 and l.VALUE = 'No Response-ETT' then 0 -- metavision

      else VALUENUM
      end
    as VALUENUM
  , l.CHARTTIME
  from CHARTEVENTS l

  -- get intime for charttime subselection
  inner join icustays b
    on l.icustay_id = b.icustay_id

  -- Isolate the desired GCS variables
  where l.ITEMID in
  (
    -- 198 -- GCS
    -- GCS components, CareVue
    184, 454, 723
    -- GCS components, Metavision
    , 223900, 223901, 220739
  )
  -- Only get data for the first 24 hours
  and l.charttime between b.intime and b.intime + interval '1' day
),  gcs_base as (
  SELECT pvt.ICUSTAY_ID, pvt.charttime
  -- Easier names - note we coalesced Metavision and CareVue IDs below
  , max(case when pvt.itemid = 454 then pvt.valuenum else null end) as GCSMotor
  , max(case when pvt.itemid = 723 then pvt.valuenum else null end) as GCSVerbal
  , max(case when pvt.itemid = 184 then pvt.valuenum else null end) as GCSEyes
  -- If verbal was set to 0 in the below select, then this is an intubated patient
  , case
      when max(case when pvt.itemid = 723 then pvt.valuenum else null end) = 0
    then 1
    else 0
    end as EndoTrachFlag
  , ROW_NUMBER ()
          OVER (PARTITION BY pvt.ICUSTAY_ID ORDER BY pvt.charttime ASC) as rn

  FROM gcs_base_pvt as pvt
  group by pvt.ICUSTAY_ID, pvt.charttime
  ), gcs as (
    select b.*
  , b2.GCSVerbal as GCSVerbalPrev
  , b2.GCSMotor as GCSMotorPrev
  , b2.GCSEyes as GCSEyesPrev
  -- Calculate GCS, factoring in special case when they are intubated and prev vals
  -- note that the coalesce are used to implement the following if:
  --  if current value exists, use it
  --  if previous value exists, use it
  --  otherwise, default to normal
  , case
      -- replace GCS during sedation with 15
      when b.GCSVerbal = 0
        then 15
      when b.GCSVerbal is null and b2.GCSVerbal = 0
        then 15
      -- if previously they were intub, but they aren't now, do not use previous GCS values
      when b2.GCSVerbal = 0
        then
            coalesce(b.GCSMotor,6)
          + coalesce(b.GCSVerbal,5)
          + coalesce(b.GCSEyes,4)
      -- otherwise, add up score normally, imputing previous value if none available at current time
      else
            coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
          + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
          + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
      end as GCS
  from gcs_base b
  -- join to itself within 6 hours to get previous value
  left join gcs_base b2
    on b.ICUSTAY_ID = b2.ICUSTAY_ID and b.rn = b2.rn+1 and b2.charttime > b.charttime - interval '6' hour
), gcsfinal as (
  select gcs.*
  -- This sorts the data by GCS, so rn=1 is the the lowest GCS values to keep
  , ROW_NUMBER ()
          OVER (PARTITION BY gcs.ICUSTAY_ID
                ORDER BY gcs.GCS
               ) as IsMinGCS
  from gcs)

select ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
-- The minimum GCS is determined by the above row partition, we only join if IsMinGCS=1
, GCS as MinGCS
, coalesce(GCSMotor,GCSMotorPrev) as GCSMotor
, coalesce(GCSVerbal,GCSVerbalPrev) as GCSVerbal
, coalesce(GCSEyes,GCSEyesPrev) as GCSEyes
, EndoTrachFlag as EndoTrachFlag
-- subselect down to the cohort of eligible patients
from icustays ie
left join gcsfinal gs
  on ie.ICUSTAY_ID = gs.ICUSTAY_ID and gs.IsMinGCS = 1
ORDER BY ie.ICUSTAY_ID
""")
gcsfirstday.registerTempTable("gcsfirstday")
writeOutput(gcsfirstday,uri,folder,"GCSFIRSTDAY")

val bloodgasfirstday = sqlContext.sql("""
with bloodgas_pvt as (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  -- here we assign labels to ITEMIDs
  -- this also fuses together multiple ITEMIDs containing the same data
      , case
        when itemid = 50800 then 'SPECIMEN'
        when itemid = 50801 then 'AADO2'
        when itemid = 50802 then 'BASEEXCESS'
        when itemid = 50803 then 'BICARBONATE'
        when itemid = 50804 then 'TOTALCO2'
        when itemid = 50805 then 'CARBOXYHEMOGLOBIN'
        when itemid = 50806 then 'CHLORIDE'
        when itemid = 50808 then 'CALCIUM'
        when itemid = 50809 then 'GLUCOSE'
        when itemid = 50810 then 'HEMATOCRIT'
        when itemid = 50811 then 'HEMOGLOBIN'
        when itemid = 50812 then 'INTUBATED'
        when itemid = 50813 then 'LACTATE'
        when itemid = 50814 then 'METHEMOGLOBIN'
        when itemid = 50815 then 'O2FLOW'
        when itemid = 50816 then 'FIO2'
        when itemid = 50817 then 'SO2' -- OXYGENSATURATION
        when itemid = 50818 then 'PCO2'
        when itemid = 50819 then 'PEEP'
        when itemid = 50820 then 'PH'
        when itemid = 50821 then 'PO2'
        when itemid = 50822 then 'POTASSIUM'
        when itemid = 50823 then 'REQUIREDO2'
        when itemid = 50824 then 'SODIUM'
        when itemid = 50825 then 'TEMPERATURE'
        when itemid = 50826 then 'TIDALVOLUME'
        when itemid = 50827 then 'VENTILATIONRATE'
        when itemid = 50828 then 'VENTILATOR'
        else null
        end as label
        , charttime
        , value
        -- add in some sanity checks on the values
        , case
          when valuenum <= 0 then null
          when itemid = 50810 and valuenum > 100 then null -- hematocrit
          when itemid = 50816 and valuenum > 100 then null -- FiO2
          when itemid = 50817 and valuenum > 100 then null -- O2 sat
          when itemid = 50815 and valuenum >  70 then null -- O2 flow
          when itemid = 50821 and valuenum > 800 then null -- PO2
           -- conservative upper limit
        else valuenum
        end as valuenum

    from icustays ie
    left join labevents le
      on le.subject_id = ie.subject_id and le.hadm_id = ie.hadm_id
      and le.charttime between (ie.intime - interval '6' hour) and (ie.intime + interval '1' day)
      and le.ITEMID in
      -- blood gases
      (
        50800, 50801, 50802, 50803, 50804, 50805, 50806, 50807, 50808, 50809
        , 50810, 50811, 50812, 50813, 50814, 50815, 50816, 50817, 50818, 50819
        , 50820, 50821, 50822, 50823, 50824, 50825, 50826, 50827, 50828
        , 51545
      )
)

select pvt.SUBJECT_ID, pvt.HADM_ID, pvt.ICUSTAY_ID, pvt.CHARTTIME

, max(case when label = 'SPECIMEN' then value else null end) as SPECIMEN
, max(case when label = 'AADO2' then valuenum else null end) as AADO2
, max(case when label = 'BASEEXCESS' then valuenum else null end) as BASEEXCESS
, max(case when label = 'BICARBONATE' then valuenum else null end) as BICARBONATE
, max(case when label = 'TOTALCO2' then valuenum else null end) as TOTALCO2
, max(case when label = 'CARBOXYHEMOGLOBIN' then valuenum else null end) as CARBOXYHEMOGLOBIN
, max(case when label = 'CHLORIDE' then valuenum else null end) as CHLORIDE
, max(case when label = 'CALCIUM' then valuenum else null end) as CALCIUM
, max(case when label = 'GLUCOSE' then valuenum else null end) as GLUCOSE
, max(case when label = 'HEMATOCRIT' then valuenum else null end) as HEMATOCRIT
, max(case when label = 'HEMOGLOBIN' then valuenum else null end) as HEMOGLOBIN
, max(case when label = 'INTUBATED' then valuenum else null end) as INTUBATED
, max(case when label = 'LACTATE' then valuenum else null end) as LACTATE
, max(case when label = 'METHEMOGLOBIN' then valuenum else null end) as METHEMOGLOBIN
, max(case when label = 'O2FLOW' then valuenum else null end) as O2FLOW
, max(case when label = 'FIO2' then valuenum else null end) as FIO2
, max(case when label = 'SO2' then valuenum else null end) as SO2 -- OXYGENSATURATION
, max(case when label = 'PCO2' then valuenum else null end) as PCO2
, max(case when label = 'PEEP' then valuenum else null end) as PEEP
, max(case when label = 'PH' then valuenum else null end) as PH
, max(case when label = 'PO2' then valuenum else null end) as PO2
, max(case when label = 'POTASSIUM' then valuenum else null end) as POTASSIUM
, max(case when label = 'REQUIREDO2' then valuenum else null end) as REQUIREDO2
, max(case when label = 'SODIUM' then valuenum else null end) as SODIUM
, max(case when label = 'TEMPERATURE' then valuenum else null end) as TEMPERATURE
, max(case when label = 'TIDALVOLUME' then valuenum else null end) as TIDALVOLUME
, max(case when label = 'VENTILATIONRATE' then valuenum else null end) as VENTILATIONRATE
, max(case when label = 'VENTILATOR' then valuenum else null end) as VENTILATOR
from
bloodgas_pvt as pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME
""")
bloodgasfirstday.registerTempTable("bloodgasfirstday")
writeOutput(bloodgasfirstday,uri,folder,"BLOODGASFIRSTDAY")

val bloodgasfirstdayarterial = sqlContext.sql("""
 with bloodgasfirstdayarterial_stg_spo2 as (
  select SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
    -- max here is just used to group SpO2 by charttime
    , max(case when valuenum <= 0 or valuenum > 100 then null else valuenum end) as SpO2
  from CHARTEVENTS
  -- o2 sat
  where ITEMID in
  (
    646 -- SpO2
  , 220277 -- O2 saturation pulseoxymetry
  )
  group by SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
), bloodgasfirstdayarterial_stg_fio2 as (
  select SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
    -- pre-process the FiO2s to ensure they are between 21-100%
    , max(
        case
          when itemid = 223835
            then case
              when valuenum > 0 and valuenum <= 1
                then valuenum * 100
              -- improperly input data - looks like O2 flow in litres
              when valuenum > 1 and valuenum < 21
                then null
              when valuenum >= 21 and valuenum <= 100
                then valuenum
              else null end -- unphysiological
        when itemid in (3420, 3422)
        -- all these values are well formatted
            then valuenum
        when itemid = 190 and valuenum > 0.20 and valuenum < 1
        -- well formatted but not in %
            then valuenum * 100
      else null end
    ) as fio2_chartevents
  from CHARTEVENTS
  where ITEMID in
  (
    3420 -- FiO2
  , 190 -- FiO2 set
  , 223835 -- Inspired O2 Fraction (FiO2)
  , 3422 -- FiO2 [measured]
  )
  group by SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
), bloodgasfirstdayarterial_stg2 as (
  select bg.*
    , ROW_NUMBER() OVER (partition by bg.icustay_id, bg.charttime order by s1.charttime DESC) as lastRowSpO2
    , s1.spo2
  from bloodgasfirstday bg
  left join bloodgasfirstdayarterial_stg_spo2 s1
    -- same patient
    on  bg.icustay_id = s1.icustay_id
    -- spo2 occurred at most 2 hours before this blood gas
    and s1.charttime between bg.charttime - interval '2' hour and bg.charttime
  where bg.po2 is not null
), bloodgasfirstdayarterial_stg3 as (
  select bg.*
  , ROW_NUMBER() OVER (partition by bg.icustay_id, bg.charttime order by s2.charttime DESC) as lastRowFiO2
  , s2.fio2_chartevents

  -- create our specimen prediction
  ,  1/(1+exp(-(-0.02544
  +    0.04598 * po2
  + coalesce(-0.15356 * spo2             , -0.15356 *   97.49420 +    0.13429)
  + coalesce( 0.00621 * fio2_chartevents ,  0.00621 *   51.49550 +   -0.24958)
  + coalesce( 0.10559 * hemoglobin       ,  0.10559 *   10.32307 +    0.05954)
  + coalesce( 0.13251 * so2              ,  0.13251 *   93.66539 +   -0.23172)
  + coalesce(-0.01511 * pco2             , -0.01511 *   42.08866 +   -0.01630)
  + coalesce( 0.01480 * fio2             ,  0.01480 *   63.97836 +   -0.31142)
  + coalesce(-0.00200 * aado2            , -0.00200 *  442.21186 +   -0.01328)
  + coalesce(-0.03220 * bicarbonate      , -0.03220 *   22.96894 +   -0.06535)
  + coalesce( 0.05384 * totalco2         ,  0.05384 *   24.72632 +   -0.01405)
  + coalesce( 0.08202 * lactate          ,  0.08202 *    3.06436 +    0.06038)
  + coalesce( 0.10956 * ph               ,  0.10956 *    7.36233 +   -0.00617)
  + coalesce( 0.00848 * o2flow           ,  0.00848 *    7.59362 +   -0.35803)
  ))) as SPECIMEN_PROB
from bloodgasfirstdayarterial_stg2 bg
left join bloodgasfirstdayarterial_stg_fio2 s2
  -- same patient
  on  bg.icustay_id = s2.icustay_id
  -- fio2 occurred at most 4 hours before this blood gas
  and s2.charttime between bg.charttime - interval '4' hour and bg.charttime
  where bg.lastRowSpO2 = 1 -- only the row with the most recent SpO2 (if no SpO2 found lastRowSpO2 = 1)
)

select subject_id, hadm_id,
icustay_id, charttime
, SPECIMEN -- raw data indicating sample type, only present 80% of the time

-- prediction of specimen for missing data
, case
      when SPECIMEN is not null then SPECIMEN
      when SPECIMEN_PROB > 0.75 then 'ART'
    else null end as SPECIMEN_PRED
, SPECIMEN_PROB

-- oxygen related parameters
, SO2, spo2 -- note spo2 is from chartevents
, PO2, PCO2
, fio2_chartevents, FIO2
, AADO2
-- also calculate AADO2
, case
    when  PO2 is not null
      and pco2 is not null
      and coalesce(FIO2, fio2_chartevents) is not null
     -- multiple by 100 because FiO2 is in a % but should be a fraction
      then (coalesce(FIO2, fio2_chartevents)/100) * (760 - 47) - (pco2/0.8) - po2
    else null
  end as AADO2_calc
, case
    when PO2 is not null and coalesce(FIO2, fio2_chartevents) is not null
     -- multiply by 100 because FiO2 is in a % but should be a fraction
      then 100*PO2/(coalesce(FIO2, fio2_chartevents))
    else null
  end as PaO2FiO2
-- acid-base parameters
, PH, BASEEXCESS
, BICARBONATE, TOTALCO2

-- blood count parameters
, HEMATOCRIT
, HEMOGLOBIN
, CARBOXYHEMOGLOBIN
, METHEMOGLOBIN

-- chemistry
, CHLORIDE, CALCIUM
, TEMPERATURE
, POTASSIUM, SODIUM
, LACTATE
, GLUCOSE

-- ventilation stuff that's sometimes input
, INTUBATED, TIDALVOLUME, VENTILATIONRATE, VENTILATOR
, PEEP, O2Flow
, REQUIREDO2

from bloodgasfirstdayarterial_stg3 as stg3
where lastRowFiO2 = 1 -- only the most recent FiO2
-- restrict it to *only* arterial samples
and (SPECIMEN = 'ART' or SPECIMEN_PROB > 0.75)
order by icustay_id, charttime
""")
bloodgasfirstdayarterial.registerTempTable("bloodgasfirstdayarterial")
writeOutput(bloodgasfirstdayarterial,uri,folder,"BLOODGASFIRSTDAYARTERIAL")


val oasis = sqlContext.sql("""
with surgflag as (
select ie.icustay_id
    , max(case
        when lower(curr_service) like '%surg%' then 1
        when curr_service = 'ORTHO' then 1
    else 0 end) as surgical
  from icustays ie
  left join services se
    on ie.hadm_id = se.hadm_id
    and se.transfertime < ie.intime + interval '1' day
  group by ie.icustay_id
), cohort as (
  SELECT ie.subject_id, ie.hadm_id, ie.icustay_id
      , ie.intime
      , ie.outtime
      , adm.deathtime,
      floor(unix_timestamp(ie.intime)-unix_timestamp(adm.admittime)) as PreICULOS,
      floor((unix_timestamp(ie.intime) - unix_timestamp(pat.dob)) / (365.242*24*3600) ) as age,
      case
          when adm.ADMISSION_TYPE = 'ELECTIVE' and sf.surgical = 1
            then 1
          when adm.ADMISSION_TYPE is null or sf.surgical is null
            then null
          else 0
        end as ElectiveSurgery,
      case
        when ( ( unix_timestamp(ie.intime) - unix_timestamp(pat.dob) ) ) <= (60*60*24*12) then 'neonate'
        when ( ( unix_timestamp(ie.intime) - unix_timestamp(pat.dob) ) ) <= (60*60*24*12*15) then 'middle'
        else 'adult' end as ICUSTAY_AGE_GROUP,
       case
          when adm.deathtime between ie.intime and ie.outtime
            then 1
          when adm.deathtime <= ie.intime -- sometimes there are typographical errors in the death date
            then 1
          when adm.dischtime <= ie.outtime and adm.discharge_location = 'DEAD/EXPIRED'
            then 1
          else 0 end
        as ICUSTAY_EXPIRE_FLAG
      , adm.hospital_expire_flag
      , vital.heartrate_max
      , vital.heartrate_min
      , vital.meanbp_max
      , vital.meanbp_min
      , vital.resprate_max
      , vital.resprate_min
      , vital.tempc_max
      , vital.tempc_min
      , vent.mechvent
      , uo.urineoutput
      , gcs.mingcs
from icustays ie
inner join admissions adm
on ie.hadm_id = adm.hadm_id 
inner join patients pat
on ie.subject_id = pat.subject_id
left join surgflag sf
  on ie.icustay_id = sf.icustay_id
left join vitalsfirstday vital
  on ie.icustay_id = vital.icustay_id
left join ventfirstday vent
  on ie.icustay_id = vent.icustay_id
left join uofirstday uo
  on ie.icustay_id = uo.icustay_id
left join gcsfirstday gcs
  on ie.icustay_id = gcs.icustay_id
  ), scorecomp as (
select co.subject_id, co.hadm_id, co.icustay_id
, co.ICUSTAY_AGE_GROUP
, co.icustay_expire_flag
, co.hospital_expire_flag

-- Below code calculates the component scores needed for OASIS
, case when preiculos is null then null
     when preiculos < 12+10*60 then 5
     when preiculos < 57*60+4*3600 then 3
     when preiculos < 86400 then 0
     when preiculos < 12*86400+23*3600+48*60 then 1
     else 2 end as preiculos_score
, case when age is null then null
      when age < 24 then 0
      when age <= 53 then 3
      when age <= 77 then 6
      when age <= 89 then 9
      when age >= 90 then 7
      else 0 end as age_score
,  case when mingcs is null then null
      when mingcs <= 7 then 10
      when mingcs < 14 then 4
      when mingcs = 14 then 3
      else 0 end as gcs_score
,  case when heartrate_max is null then null
      when heartrate_max > 125 then 6
      when heartrate_min < 33 then 4
      when heartrate_max >= 107 and heartrate_max <= 125 then 3
      when heartrate_max >= 89 and heartrate_max <= 106 then 1
      else 0 end as heartrate_score
,  case when meanbp_min is null then null
      when meanbp_min < 20.65 then 4
      when meanbp_min < 51 then 3
      when meanbp_max > 143.44 then 3
      when meanbp_min >= 51 and meanbp_min < 61.33 then 2
      else 0 end as meanbp_score
,  case when resprate_min is null then null
      when resprate_min <   6 then 10
      when resprate_max >  44 then  9
      when resprate_max >  30 then  6
      when resprate_max >  22 then  1
      when resprate_min <  13 then 1 else 0
      end as resprate_score
,  case when tempc_max is null then null
      when tempc_max > 39.88 then 6
      when tempc_min >= 33.22 and tempc_min <= 35.93 then 4
      when tempc_max >= 33.22 and tempc_max <= 35.93 then 4
      when tempc_min < 33.22 then 3
      when tempc_min > 35.93 and tempc_min <= 36.39 then 2
      when tempc_max >= 36.89 and tempc_max <= 39.88 then 2
      else 0 end as temp_score
,  case when UrineOutput is null then null
      when UrineOutput < 671.09 then 10
      when UrineOutput > 6896.80 then 8
      when UrineOutput >= 671.09
       and UrineOutput <= 1426.99 then 5
      when UrineOutput >= 1427.00
       and UrineOutput <= 2544.14 then 1
      else 0 end as UrineOutput_score
,  case when mechvent is null then null
      when mechvent = 1 then 9
      else 0 end as mechvent_score
,  case when ElectiveSurgery is null then null
      when ElectiveSurgery = 1 then 0
      else 6 end as electivesurgery_score
, preiculos
, age
, mingcs as gcs
,  case when heartrate_max is null then null
      when heartrate_max > 125 then heartrate_max
      when heartrate_min < 33 then heartrate_min
      when heartrate_max >= 107 and heartrate_max <= 125 then heartrate_max
      when heartrate_max >= 89 and heartrate_max <= 106 then heartrate_max
      else (heartrate_min+heartrate_max)/2 end as heartrate
,  case when meanbp_min is null then null
      when meanbp_min < 20.65 then meanbp_min
      when meanbp_min < 51 then meanbp_min
      when meanbp_max > 143.44 then meanbp_max
      when meanbp_min >= 51 and meanbp_min < 61.33 then meanbp_min
      else (meanbp_min+meanbp_max)/2 end as meanbp
,  case when resprate_min is null then null
      when resprate_min <   6 then resprate_min
      when resprate_max >  44 then resprate_max
      when resprate_max >  30 then resprate_max
      when resprate_max >  22 then resprate_max
      when resprate_min <  13 then resprate_min
      else (resprate_min+resprate_max)/2 end as resprate
,  case when tempc_max is null then null
      when tempc_max > 39.88 then tempc_max
      when tempc_min >= 33.22 and tempc_min <= 35.93 then tempc_min
      when tempc_max >= 33.22 and tempc_max <= 35.93 then tempc_max
      when tempc_min < 33.22 then tempc_min
      when tempc_min > 35.93 and tempc_min <= 36.39 then tempc_min
      when tempc_max >= 36.89 and tempc_max <= 39.88 then tempc_max
      else (tempc_min+tempc_max)/2 end as temp
,  UrineOutput
,  mechvent
,  ElectiveSurgery
from cohort co
  ), score as (
select s.*
    ,coalesce(age_score,0)
    + coalesce(preiculos_score,0)
    + coalesce(gcs_score,0)
    + coalesce(heartrate_score,0)
    + coalesce(meanbp_score,0)
    + coalesce(resprate_score,0)
    + coalesce(temp_score,0)
    + coalesce(urineoutput_score,0)
    + coalesce(mechvent_score,0)
    + coalesce(electivesurgery_score,0)
    as OASIS
from scorecomp s
  )

select
  subject_id, hadm_id, icustay_id
  , ICUSTAY_AGE_GROUP
  , hospital_expire_flag
  , icustay_expire_flag
  , OASIS
  -- Calculate the probability of in-hospital mortality
  , 1 / (1 + exp(- (-6.1746 + 0.1275*(OASIS) ))) as OASIS_PROB
  , age, age_score
  , preiculos, preiculos_score
  , gcs, gcs_score
  , heartrate, heartrate_score
  , meanbp, meanbp_score
  , resprate, resprate_score
  , temp, temp_score
  , urineoutput, UrineOutput_score
  , mechvent, mechvent_score
  , electivesurgery, electivesurgery_score
from score
order by icustay_id
""")
oasis.registerTempTable("oasis")
writeOutput(oasis,uri,folder,"OASIS")

val sapsii = sqlContext.sql("""
with cpap as (
select ie.icustay_id
    , min(charttime - interval '1' hour) as starttime
    , max(charttime + interval '4' hour) as endtime
    , max(case when value in ('CPAP Mask','Bipap Mask') then 1 else 0 end) as cpap
  from icustays ie
  inner join chartevents ce
    on ie.icustay_id = ce.icustay_id
    and ce.charttime between ie.intime and ie.intime + interval '1' day
  where itemid in
  (
    -- TODO: when metavision data import fixed, check the values in 226732 match the value clause below
    467, 469, 226732
  )
  and value in ('CPAP Mask','Bipap Mask')
  group by ie.icustay_id
), surgflag as (
  select adm.hadm_id
    , case when lower(curr_service) like '%surg%' then 1 else 0 end as surgical
    , ROW_NUMBER() over
    (
      PARTITION BY adm.HADM_ID
      ORDER BY TRANSFERTIME
    ) as serviceOrder
  from admissions adm
  left join services se
    on adm.hadm_id = se.hadm_id
), comorb as (
  select hadm_id
-- these are slightly different than elixhauser comorbidities, but based on them
-- they include some non-comorbid ICD-9 codes (e.g. 20302, relapse of multiple myeloma)
  , max(CASE
    when icd9_code between '042  ' and '0449 ' then 1
      end) as AIDS     
  , max(CASE
    when icd9_code between '20000' and '20238' then 1 -- lymphoma
    when icd9_code between '20240' and '20248' then 1 -- leukemia
    when icd9_code between '20250' and '20302' then 1 -- lymphoma
    when icd9_code between '20310' and '20312' then 1 -- leukemia
    when icd9_code between '20302' and '20382' then 1 -- lymphoma
    when icd9_code between '20400' and '20522' then 1 -- chronic leukemia
    when icd9_code between '20580' and '20702' then 1 -- other myeloid leukemia
    when icd9_code between '20720' and '20892' then 1 -- other myeloid leukemia
    when icd9_code = '2386 ' then 1 -- lymphoma
    when icd9_code = '2733 ' then 1 -- lymphoma
      end) as HEM
  , max(CASE
    when icd9_code between '1960 ' and '1991 ' then 1
    when icd9_code between '20970' and '20975' then 1
    when icd9_code = '20979' then 1
    when icd9_code = '78951' then 1
      end) as METS    
  from
  (
    select hadm_id, seq_num
    , cast(icd9_code as char(5)) as icd9_code
    from diagnoses_icd
  ) icd
  group by hadm_id
), pafi1 as (
  select bg.icustay_id, bg.charttime
  , PaO2FiO2
  , case when vd.icustay_id is not null then 1 else 0 end as vent
  , case when cp.icustay_id is not null then 1 else 0 end as cpap
  from bloodgasfirstdayarterial bg
  left join ventdurations vd
    on bg.icustay_id = vd.icustay_id
    and bg.charttime >= vd.starttime
    and bg.charttime <= vd.endtime
  left join cpap cp
    on bg.icustay_id = cp.icustay_id
    and bg.charttime >= cp.starttime
    and bg.charttime <= cp.endtime
), pafi2 as (
  select icustay_id
  , min(PaO2FiO2) as PaO2FiO2_vent_min
  from pafi1
  where vent = 1 or cpap = 1
  group by icustay_id
), cohort as (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
      , ie.intime
      , ie.outtime
      , vital.heartrate_max
      , vital.heartrate_min
      , vital.sysbp_max
      , vital.sysbp_min
      , vital.tempc_max
      , vital.tempc_min
      , pf.PaO2FiO2_vent_min
      , uo.urineoutput
      , labs.bun_max
      , labs.wbc_min
      , labs.wbc_max
      , labs.potassium_min
      , labs.potassium_max
      , labs.sodium_min
      , labs.sodium_max
      , labs.bicarbonate_min
      , labs.bicarbonate_max
      , labs.bilirubin_min
      , labs.bilirubin_max
      , gcs.mingcs
      , comorb.AIDS
      , comorb.HEM
      , comorb.METS
      -- the casts ensure the result is numeric.. we could equally extract EPOCH from the interval
      -- however this code works in Oracle and Postgres
      , round( ( unix_timestamp(ie.intime) - unix_timestamp(pat.dob) ) / (365.242*24*3600) , 2 ) as age
      , case
          when adm.ADMISSION_TYPE = 'ELECTIVE' and sf.surgical = 1
            then 'ScheduledSurgical'
          when adm.ADMISSION_TYPE != 'ELECTIVE' and sf.surgical = 1
            then 'UnscheduledSurgical'
          else 'Medical'
        end as AdmissionType
from icustays ie
inner join admissions adm
  on ie.hadm_id = adm.hadm_id
inner join patients pat
  on ie.subject_id = pat.subject_id
left join surgflag sf
  on adm.hadm_id = sf.hadm_id and sf.serviceOrder = 1
left join comorb
  on ie.hadm_id = comorb.hadm_id
left join vitalsfirstday vital
  on ie.icustay_id = vital.icustay_id
left join pafi2 pf
  on ie.icustay_id = pf.icustay_id
left join uofirstday uo
  on ie.icustay_id = uo.icustay_id
left join labsfirstday labs
  on ie.icustay_id = labs.icustay_id
left join gcsfirstday gcs
  on ie.icustay_id = gcs.icustay_id
), scorecomp as (
  select
  cohort.*
  -- Below code calculates the component scores needed for SAPS
  , case
      when age is null then null
      when age <  40 then 0
      when age <  60 then 7
      when age <  70 then 12
      when age <  75 then 15
      when age <  80 then 16
      when age >= 80 then 18
    end as age_score

  , case
      when heartrate_max is null then null
      when heartrate_min <   40 then 11
      when heartrate_max >= 160 then 7
      when heartrate_max >= 120 then 4
      when heartrate_min  <  70 then 2
      when  heartrate_max >= 70 and heartrate_max < 120
        and heartrate_min >= 70 and heartrate_min < 120
      then 0
    end as hr_score

  , case
      when  sysbp_min is null then null
      when  sysbp_min <   70 then 13
      when  sysbp_min <  100 then 5
      when  sysbp_max >= 200 then 2
      when  sysbp_max >= 100 and sysbp_max < 200
        and sysbp_min >= 100 and sysbp_min < 200
        then 0
    end as sysbp_score

  , case
      when tempc_max is null then null
      when tempc_min <  39.0 then 0
      when tempc_max >= 39.0 then 3
    end as temp_score

  , case
      when PaO2FiO2_vent_min is null then null
      when PaO2FiO2_vent_min <  100 then 11
      when PaO2FiO2_vent_min <  200 then 9
      when PaO2FiO2_vent_min >= 200 then 6
    end as PaO2FiO2_score

  , case
      when UrineOutput is null then null
      when UrineOutput <   500.0 then 11
      when UrineOutput <  1000.0 then 4
      when UrineOutput >= 1000.0 then 0
    end as uo_score

  , case
      when bun_max is null then null
      when bun_max <  28.0 then 0
      when bun_max <  83.0 then 6
      when bun_max >= 84.0 then 10
    end as bun_score

  , case
      when wbc_max is null then null
      when wbc_min <   1.0 then 12
      when wbc_max >= 20.0 then 3
      when wbc_max >=  1.0 and wbc_max < 20.0
       and wbc_min >=  1.0 and wbc_min < 20.0
        then 0
    end as wbc_score

  , case
      when potassium_max is null then null
      when potassium_min <  3.0 then 3
      when potassium_max >= 5.0 then 3
      when potassium_max >= 3.0 and potassium_max < 5.0
       and potassium_min >= 3.0 and potassium_min < 5.0
        then 0
      end as potassium_score

  , case
      when sodium_max is null then null
      when sodium_min  < 125 then 5
      when sodium_max >= 145 then 1
      when sodium_max >= 125 and sodium_max < 145
       and sodium_min >= 125 and sodium_min < 145
        then 0
      end as sodium_score

  , case
      when bicarbonate_max is null then null
      when bicarbonate_min <  15.0 then 5
      when bicarbonate_min <  20.0 then 3
      when bicarbonate_max >= 20.0
       and bicarbonate_min >= 20.0
          then 0
      end as bicarbonate_score

  , case
      when bilirubin_max is null then null
      when bilirubin_max  < 4.0 then 0
      when bilirubin_max  < 6.0 then 4
      when bilirubin_max >= 6.0 then 9
      end as bilirubin_score

   , case
      when mingcs is null then null
        when mingcs <  3 then null -- erroneous value/on trach
        when mingcs <  6 then 26
        when mingcs <  9 then 13
        when mingcs < 11 then 7
        when mingcs < 14 then 5
        when mingcs >= 14
         and mingcs <= 15
          then 0
        end as gcs_score

    , case
        when AIDS = 1 then 17
        when HEM  = 1 then 10
        when METS = 1 then 9
        else 0
      end as comorbidity_score

    , case
        when AdmissionType = 'ScheduledSurgical' then 0
        when AdmissionType = 'Medical' then 6
        when AdmissionType = 'UnscheduledSurgical' then 8
        else null
      end as admissiontype_score

from cohort
), score as (
   select s.*
  -- coalesce statements impute normal score of zero if data element is missing
  , coalesce(age_score,0)
  + coalesce(hr_score,0)
  + coalesce(sysbp_score,0)
  + coalesce(temp_score,0)
  + coalesce(PaO2FiO2_score,0)
  + coalesce(uo_score,0)
  + coalesce(bun_score,0)
  + coalesce(wbc_score,0)
  + coalesce(potassium_score,0)
  + coalesce(sodium_score,0)
  + coalesce(bicarbonate_score,0)
  + coalesce(bilirubin_score,0)
  + coalesce(gcs_score,0)
  + coalesce(comorbidity_score,0)
  + coalesce(admissiontype_score,0)
    as SAPSII
  from scorecomp s
)

select ie.subject_id, ie.hadm_id, ie.icustay_id
, SAPSII
, 1 / (1 + exp(- (-7.7631 + 0.0737*(SAPSII) + 0.9971*(ln(SAPSII + 1))) )) as SAPSII_PROB
, age_score
, hr_score
, sysbp_score
, temp_score
, PaO2FiO2_score
, uo_score
, bun_score
, wbc_score
, potassium_score
, sodium_score
, bicarbonate_score
, bilirubin_score
, gcs_score
, comorbidity_score
, admissiontype_score
from icustays ie
left join score s
  on ie.icustay_id = s.icustay_id
order by ie.icustay_id
""")
sapsii.registerTempTable("sapsii")
writeOutput(sapsii,uri,folder,"SAPSII")


val sofa = sqlContext.sql("""
with wt as (
  SELECT ie.icustay_id
    -- ensure weight is measured in kg
    , avg(CASE
        WHEN itemid IN (762, 763, 3723, 3580, 226512)
          THEN valuenum
        -- convert lbs to kgs
        WHEN itemid IN (3581)
          THEN valuenum * 0.45359237
        WHEN itemid IN (3582)
          THEN valuenum * 0.0283495231
        ELSE null
      END) AS weight

  from icustays ie
  left join chartevents c
    on ie.icustay_id = c.icustay_id
  WHERE valuenum IS NOT NULL
  AND itemid IN
  (
    762, 763, 3723, 3580,                     -- Weight Kg
    3581,                                     -- Weight lb
    3582,                                     -- Weight oz
    226512 -- Metavision: Admission Weight (Kg)
  )
  AND valuenum != 0
  and charttime between ie.intime - interval '1' day and ie.intime + interval '1' day
  group by ie.icustay_id
), echo2 as (
  select ie.icustay_id, avg(weight * 0.45359237) as weight
  from icustays ie
  left join echodata echo
    on ie.hadm_id = echo.hadm_id
    and echo.chartdate > ie.intime - interval '7' day
    and echo.chartdate < ie.intime + interval '1' day
  group by ie.icustay_id
), vaso_cv as (
  select ie.icustay_id
    -- case statement determining whether the ITEMID is an instance of vasopressor usage
    , max(case
            when itemid = 30047 then rate / coalesce(wt.weight,ec.weight) -- measured in mcgmin
            when itemid = 30120 then rate -- measured in mcgkgmin ** there are clear errors, perhaps actually mcgmin
            else null
          end) as rate_norepinephrine

    , max(case
            when itemid =  30044 then rate / coalesce(wt.weight,ec.weight) -- measured in mcgmin
            when itemid in (30119,30309) then rate -- measured in mcgkgmin
            else null
          end) as rate_epinephrine

    , max(case when itemid in (30043,30307) then rate end) as rate_dopamine
    , max(case when itemid in (30042,30306) then rate end) as rate_dobutamine

  from icustays ie
  inner join inputevents_cv cv
    on ie.icustay_id = cv.icustay_id and cv.charttime between ie.intime and ie.intime + interval '1' day
  left join wt
    on ie.icustay_id = wt.icustay_id
  left join echo2 ec
    on ie.icustay_id = ec.icustay_id
  where itemid in (30047,30120,30044,30119,30309,30043,30307,30042,30306)
  and rate is not null
  group by ie.icustay_id
), vaso_mv as (
    select ie.icustay_id
    -- case statement determining whether the ITEMID is an instance of vasopressor usage
    , max(case when itemid = 221906 then rate end) as rate_norepinephrine
    , max(case when itemid = 221289 then rate end) as rate_epinephrine
    , max(case when itemid = 221662 then rate end) as rate_dopamine
    , max(case when itemid = 221653 then rate end) as rate_dobutamine
  from icustays ie
  inner join inputevents_mv mv
    on ie.icustay_id = mv.icustay_id and mv.starttime between ie.intime and ie.intime + interval '1' day
  where itemid in (221906,221289,221662,221653)
  -- 'Rewritten' orders are not delivered to the patient
  and statusdescription != 'Rewritten'
  group by ie.icustay_id
), pafi1 as
(
  -- join blood gas to ventilation durations to determine if patient was vent
  select bg.icustay_id, bg.charttime
  , PaO2FiO2
  , case when vd.icustay_id is not null then 1 else 0 end as IsVent
  from bloodgasfirstdayarterial bg
  left join ventdurations vd
    on bg.icustay_id = vd.icustay_id
    and bg.charttime >= vd.starttime
    and bg.charttime <= vd.endtime
  order by bg.icustay_id, bg.charttime
)
, pafi2 as
(
  -- because pafi has an interaction between vent/PaO2:FiO2, we need two columns for the score
  -- it can happen that the lowest unventilated PaO2/FiO2 is 68, but the lowest ventilated PaO2/FiO2 is 120
  -- in this case, the SOFA score is 3, *not* 4.
  select icustay_id
  , min(case when IsVent = 0 then PaO2FiO2 else null end) as PaO2FiO2_novent_min
  , min(case when IsVent = 1 then PaO2FiO2 else null end) as PaO2FiO2_vent_min
  from pafi1
  group by icustay_id
)
-- Aggregate the components for the score
, scorecomp as
(
select ie.icustay_id
  , v.MeanBP_Min
  , coalesce(cv.rate_norepinephrine, mv.rate_norepinephrine) as rate_norepinephrine
  , coalesce(cv.rate_epinephrine, mv.rate_epinephrine) as rate_epinephrine
  , coalesce(cv.rate_dopamine, mv.rate_dopamine) as rate_dopamine
  , coalesce(cv.rate_dobutamine, mv.rate_dobutamine) as rate_dobutamine

  , l.Creatinine_Max
  , l.Bilirubin_Max
  , l.Platelet_Min

  , pf.PaO2FiO2_novent_min
  , pf.PaO2FiO2_vent_min

  , uo.UrineOutput

  , gcs.MinGCS
from icustays ie
left join vaso_cv cv
  on ie.icustay_id = cv.icustay_id
left join vaso_mv mv
  on ie.icustay_id = mv.icustay_id
left join pafi2 pf
 on ie.icustay_id = pf.icustay_id
left join vitalsfirstday v
  on ie.icustay_id = v.icustay_id
left join labsfirstday l
  on ie.icustay_id = l.icustay_id
left join uofirstday uo
  on ie.icustay_id = uo.icustay_id
left join gcsfirstday gcs
  on ie.icustay_id = gcs.icustay_id
)
, scorecalc as
(
  -- Calculate the final score
  -- note that if the underlying data is missing, the component is null
  -- eventually these are treated as 0 (normal), but knowing when data is missing is useful for debugging
  select icustay_id
  -- Respiration
  , case
      when PaO2FiO2_vent_min   < 100 then 4
      when PaO2FiO2_vent_min   < 200 then 3
      when PaO2FiO2_novent_min < 300 then 2
      when PaO2FiO2_novent_min < 400 then 1
      when coalesce(PaO2FiO2_vent_min, PaO2FiO2_novent_min) is null then null
      else 0
    end as respiration

  -- Coagulation
  , case
      when platelet_min < 20  then 4
      when platelet_min < 50  then 3
      when platelet_min < 100 then 2
      when platelet_min < 150 then 1
      when platelet_min is null then null
      else 0
    end as coagulation

  -- Liver
  , case
      -- Bilirubin checks in mg/dL
        when Bilirubin_Max >= 12.0 then 4
        when Bilirubin_Max >= 6.0  then 3
        when Bilirubin_Max >= 2.0  then 2
        when Bilirubin_Max >= 1.2  then 1
        when Bilirubin_Max is null then null
        else 0
      end as liver

  -- Cardiovascular
  , case
      when rate_dopamine > 15 or rate_epinephrine >  0.1 or rate_norepinephrine >  0.1 then 4
      when rate_dopamine >  5 or rate_epinephrine <= 0.1 or rate_norepinephrine <= 0.1 then 3
      when rate_dopamine >  0 or rate_dobutamine > 0 then 2
      when MeanBP_Min < 70 then 1
      when coalesce(MeanBP_Min, rate_dopamine, rate_dobutamine, rate_epinephrine, rate_norepinephrine) is null then null
      else 0
    end as cardiovascular

  -- Neurological failure (GCS)
  , case
      when (MinGCS >= 13 and MinGCS <= 14) then 1
      when (MinGCS >= 10 and MinGCS <= 12) then 2
      when (MinGCS >=  6 and MinGCS <=  9) then 3
      when  MinGCS <   6 then 4
      when  MinGCS is null then null
  else 0 end
    as cns

  -- Renal failure - high creatinine or low urine output
  , case
    when (Creatinine_Max >= 5.0) then 4
    when  UrineOutput < 200 then 4
    when (Creatinine_Max >= 3.5 and Creatinine_Max < 5.0) then 3
    when  UrineOutput < 500 then 3
    when (Creatinine_Max >= 2.0 and Creatinine_Max < 3.5) then 2
    when (Creatinine_Max >= 1.2 and Creatinine_Max < 2.0) then 1
    when coalesce(UrineOutput, Creatinine_Max) is null then null
  else 0 end
    as renal
  from scorecomp
)
select ie.subject_id, ie.hadm_id, ie.icustay_id
  -- Combine all the scores to get SOFA
  -- Impute 0 if the score is missing
  , coalesce(respiration,0)
  + coalesce(coagulation,0)
  + coalesce(liver,0)
  + coalesce(cardiovascular,0)
  + coalesce(cns,0)
  + coalesce(renal,0)
  as SOFA
, respiration
, coagulation
, liver
, cardiovascular
, cns
, renal
from icustays ie
left join scorecalc s
  on ie.icustay_id = s.icustay_id
order by ie.icustay_id
""")
sofa.registerTempTable("sofa")
writeOutput(sofa,uri,folder,"SOFA")
