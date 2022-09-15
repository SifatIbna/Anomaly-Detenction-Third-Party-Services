from datetime import datetime
from enum import Enum
import Enums
from Enums import AnomalyNames,Status

from Enums.ThirdPartyNames import ThirdPatyServiceNames

class Anomaly_Detection:
    def __init__(self,anomaly_type,data_frame,reconcilation_report,finalize_report,refund_report):
        self.anomaly_type = anomaly_type
        self.data_frame = data_frame
        self.reconcilation_report = reconcilation_report
        self.finalize_report = finalize_report
        self.refund_report = refund_report

    @staticmethod
    def check_preprocess_status(dataframe,service_name):
        if dataframe.empty:
            return AnomalyNames.ErrorStatus.UNABLE_TO_PREPROCESS_DATA
        if service_name == ThirdPatyServiceNames.BREB:
            if len(dataframe) < 20 or len(dataframe) > 20:
                return AnomalyNames.ErrorStatus.UNABLE_TO_PREPROCESS_DATA
        return Status.DATASET_READY_TO_PROCESS

    @staticmethod
    def check_processed_datasets_status(processed_dataset,serivce_name):
        if serivce_name == ThirdPatyServiceNames.BREB:
            duplicate = processed_dataset['transaction_id'].value_counts(ascending=False)
            for val in duplicate:
                if(val > 1):
                    return Enums.ErrorStatus.DUPLICATE_IN_DATASETS

            return Enums.Status.DATASET_READY_TO_PROCESS

    @staticmethod
    def check_for_missing_date(check_dataframe,to_date,from_date):
        unique_initiate_value = []

        for value in check_dataframe['initiated_at']:

            date_to_string = datetime.strftime(value,'%Y-%m-%d')

            if date_to_string not in unique_initiate_value:
                unique_initiate_value.append(date_to_string)

        return Enums.ErrorStatus.MISSING_DATE_FROM_DATASETS if to_date not in unique_initiate_value else Enums.ErrorStatus.MISSING_DATE_FROM_DATASETS if from_date in unique_initiate_value else Status.DATASET_READY_TO_PROCESS


    @staticmethod
    def pre_processed_data_and_breb_data_mismatch(preprorcessed_data,breb_data):
        pre_processed_data_total_rows = preprorcessed_data.index
        breb_data = breb_data.index

