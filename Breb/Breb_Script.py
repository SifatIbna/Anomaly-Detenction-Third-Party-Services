from msilib.schema import CustomAction, File
from os import stat
import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql
import numpy as np
from datetime import date
from datetime import datetime, timedelta

# Importing Class
import Exceptions
from  Anomaly_Detection import Anomaly_Detection
from Enums.AnomalyNames import ErrorStatus
from Enums.Status import Status
from PreProcess_Datasets import PreprocessData
from Db.Db_query import Db_query
from Enums.ThirdPartyNames import ThirdPatyServiceNames

class BrebReport:
    def __init__(self,day_span,file_name,from_date,to_date):
        self.day_span = day_span
        self.from_date = from_date
        self.to_date = to_date
        self.file_name = file_name
        self.breb_file_to_df = self.read_file()
        self.pre_process_data = self.pre_process()

    def read_file(self):
        try:
            breb_end = pd.read_excel(self.file_name)
            return breb_end
        except FileNotFoundError:
            raise Exceptions.FileNotFoundException("File Not Found in the current Directory!")

    def pre_process_data(self):
        status = Anomaly_Detection.check_preprocess_status(self.breb_file_to_df)

        if status == ErrorStatus.UNABLE_TO_PREPROCESS_DATA:
            raise Exceptions.EmptyDataFrame("Dataframe Empty")
        if status == Status.DATASET_READY_TO_PREPROCESS:
            preprocess_obj  = PreprocessData.PreProcessDatasets(self.breb_file_to_df,ThirdPatyServiceNames.BREB)
            return preprocess_obj.preprocess_datasets()

    def reconcile_datasets(self):

        # Check Anomaly For BREB Dataset
        status = Anomaly_Detection.check_processed_datasets_status(self.pre_process_data,ThirdPatyServiceNames.BREB)

        if status == ErrorStatus.DUPLICATE_IN_DATASETS:
            raise Exceptions.DuplicateInDataFrame("Duplicate Records In Datasets")

        if status == Status.DATASET_READY_TO_PROCESS:
            self.upay_end_data = Db_query().breb_query(self.from_date,self.to_date)

            merge_upay_breb = pd.merge(self.pre_process_data,self.upay_end_data,on='transaction_id',how='outer',indicator=True)
            status =  Anomaly_Detection.check_for_missing_date(merge_upay_breb,self.to_date,self.from_date)





