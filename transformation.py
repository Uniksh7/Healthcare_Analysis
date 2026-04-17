from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

class Transformation:
    def clean(self, df):
        return df.filter(col('pid').isNotNull())
    
    def remove_duplicates(self, df, key):
        return df.dropDuplicates([key])
    
    def fill_missing(self, df):
        return df.fillna({"paid_amount": 0,"billed_amount": 0,"allowed_amount": 0})
    
    def add_payment_ratio(self,df):
        return df.withColumn('payment_ratio',col('paid_amount')/col('billed_amount'))
    
    def claim_loss(self,df):
        return df.withColumn('claim_loss',col('billed_amount')-col('paid_amount'))
    
    def add_high_cost_flag(self,df):
        return df.withColumn('high_cost_flag',when(col('billed_amount')>5000,1).otherwise(0))
    
    def add_age(self,df):
        return df.withColumn('age',(datediff(current_date(),col('dob'))/365).cast(IntegerType()))
    
    def add_plan_flag(self, df):
        return df.withColumn("plan_flag",when(col("plan_id").startswith("17956"), 1).otherwise(0))
    
    def member_spend(self,df):
        return df.groupBy('pid').agg(sum('paid_amount').alias('Total_spend'))