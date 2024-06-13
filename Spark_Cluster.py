
import pyspark
import warnings
warnings.filterwarnings("ignore")
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkSession.builder.appName('PySparkShell').getOrCreate()
# print(sc)

df = sc.read.option('header','true').csv('gs://dsci-551/Data/patients_data.csv')

# df.printSchema()

df = df.withColumn('dob', 
                   to_date(unix_timestamp(col('dob'), 'MM-dd-yyyy').cast("timestamp")))

df2 = sc.read.option('header','true').csv('gs://dsci-551/Data/icustays_data.csv')

df2 = df2.withColumn('intime', 
                   to_date(unix_timestamp(col('intime'), 'MM-dd-yyyy').cast("timestamp")))
df2 = df2.withColumn('outtime', 
                   to_date(unix_timestamp(col('outtime'), 'MM-dd-yyyy').cast("timestamp")))
# print(df2.show())
# print(df2.printSchema())

df3 = sc.read.option('header','true').csv('gs://dsci-551/Data/services_data.csv')

df4 = sc.read.option('header','true').csv('gs://dsci-551/Data/admissions_data.csv')

df5 = sc.read.option('header','true').csv('gs://dsci-551/Data/prescriptions_data.csv')

df.createOrReplaceTempView("patients_db")
search_1_count = sc.sql("SELECT count(*) as tot_count from patients_db where gender = 'M' and year(dob)<=2000")
# print(search_1_count.show())

search_1 = sc.sql("SELECT * from patients_db where gender = 'M' and year(dob)<=2000")
# print(search_1.show(1228))

df4.createOrReplaceTempView("admissions_db")
search_2_count = sc.sql("""SELECT count(*) as tot_count from admissions_db where religion = 'CATHOLIC' 
and admission_location = 'EMERGENCY ROOM ADMIT' and discharge_location = 'HOME'""")
# print(search_2_count.show())

search_2 = sc.sql("""SELECT * from admissions_db where religion = 'CATHOLIC' and 
admission_location = 'EMERGENCY ROOM ADMIT' and discharge_location = 'HOME'""")
# print(search_2.show(101))

df2.createOrReplaceTempView("icustays_db")
search_3_count = sc.sql("SELECT count(*) from icustays_db where year(outtime)-year(intime)>=2 ")
# print(search_3_count.show())
search_3 = sc.sql("SELECT * from icustays_db where year(outtime)-year(intime)>=2 ")
# print(search_3.show(1590))

df3.createOrReplaceTempView("services_db")
search_4_count = sc.sql("SELECT count(*) as tot_count from services_db where curr_serv='SURG' ")
# print(search_4_count.show())
search_4 = sc.sql("SELECT * from services_db where curr_serv='SURG' ")
# print(search_4.show(251))

df5.createOrReplaceTempView("prescriptions_db")
search_5_count = sc.sql("SELECT count(*) as tot_count from prescriptions_db where drug = 'Bisacodyl' ")
# print(search_5_count.show())

search_5 = sc.sql("SELECT * from prescriptions_db where drug = 'Bisacodyl' ")
# print(search_5.show(137))

search_1.write.mode('overwrite').parquet("gs://dsci-551/Output")
search_2.write.mode('overwrite').parquet("gs://dsci-551/Output")
search_3.write.mode('overwrite').parquet("gs://dsci-551/Output")
search_4.write.mode('overwrite').parquet("gs://dsci-551/Output")
search_5.write.mode('overwrite').parquet("gs://dsci-551/Output")

analytics_1 = sc.sql("""SELECT count(*) as Count from admissions_db a join patients_db p 
                     on a.subject_id = p.subject_id where p.gender = 'M' and a.marital_status = 'SINGLE' 
                     and a.insurance = 'Medicare'""")
# print(analytics_1.show())

analytics_2 = sc.sql("""SELECT SUM(CASE WHEN year(intime)<2000 then 1 else 0 end) as Admits_before_Year_2000, 
                     SUM(CASE WHEN year(intime)>=2000 then 1 else 0 end) as Admits_after_Year_2000 from icustays_db""")
# print(analytics_2.show())

analytics_3 = sc.sql("""(SELECT b.gender,a.drug,count(*) as prescription_count from prescriptions_db a 
join patients_db b on a.subject_id = b.subject_id where b.gender = 'M' group by a.drug,b.gender 
order by prescription_count desc limit 1) UNION 
(SELECT b.gender,a.drug,count(*) as prescription_count from prescriptions_db a join patients_db b 
on a.subject_id = b.subject_id where b.gender = 'F' group by a.drug,b.gender order by prescription_count desc limit 1)""") 
# print(analytics_3.show())

analytics_1.write.mode('overwrite').parquet("gs://dsci-551/Output")
analytics_2.write.mode('overwrite').parquet("gs://dsci-551/Output")
analytics_3.write.mode('overwrite').parquet("gs://dsci-551/Output")