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

    elig=transformer.add_chronic_flag(claims)
    elig.show(5)


if __name__ == "__main__":
    main()