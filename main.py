from session import sparkSession
from load import DataLoader
from analysis import Analysis
from config import config
from transformation import Transformation

def main():
    #Initlialize Spark Session
    spark=sparkSession().get_session()

    #load data
    loader=DataLoader(spark)
    claims=loader.load_data(config.claims_path)
    elig=loader.load_data(config.elig_path)
    rx=loader.load_data(config.pharmacy_path)

    transformer=Transformation()
    claims=transformer.clean(claims)
    claims.show(5,vertical=True)

    claims=transformer.add_age(claims)
    claims.show(5,vertical=True)

    rx=transformer.add_high_cost_flag(rx)
    rx.show(5,vertical=True)

    elig=transformer.add_plan_flag(elig)
    elig.show(5)

    # claims.printSchema()
    elig.printSchema()
    # rx.printSchema()

#Joining different datasets
    joiner=Analysis()
    joined_claims_elig=joiner.join_claims_eligibility(claims,elig)
    joined_claims_elig.select('claim_id','provider_name','health_plan_name','coverage_type').show(5)

    joined_rx_elig=joiner.join_rx_eligibility(rx,elig)
    joined_rx_elig.select('pid','plan_name','coverage_type','drug_type_code','billed_amount').show(5)

if __name__ == "__main__":
    main()