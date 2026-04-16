from pyspark.sql import SparkSession

class sparkSession:
    def __init__(self,app_name='healthcare_analysis'):
        self.app_name=app_name
        self._session=None

    def get_session(self):
        if self._session is None:
            self._session=SparkSession.builder.appName(self.app_name).getOrCreate()
        print('SparkSession initialized.')
        return self._session
    
    def stop_session(self):
        if self._session is not None:
            self._session.stop()
            self._session=None
            print('SparkSession stopped.')