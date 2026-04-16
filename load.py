class DataLoader:
    def __init__(self,spark):
        self.spark=spark

    def load_data(self,path):
        return self.spark.read.csv(path,header=True,inferSchema=True)