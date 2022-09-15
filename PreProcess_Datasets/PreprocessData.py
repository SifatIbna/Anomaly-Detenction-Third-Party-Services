class PreProcessDatasets:
    def __init__(self,dataframe,service_name):
        self.dataframe = dataframe
        self.third_party_serice = service_name

    def preprocess_datasets(self):
        self.dataframe = self.rename_drop_columns(self.dataframe,self.third_party_serice)
        self.dataframe = self.drop_index_column(self.dataframe)
        self.dataframe = self.drop_duplicates(self.dataframe)
        return self.dataframe

    @staticmethod
    def rename_drop_columns(dataframe,third_party_serice):
        if third_party_serice == 'breb':
            breb_end = dataframe
            breb_end.dropna(subset = ["Unnamed: 1"], inplace=True) #removing excess null values
            breb_end.dropna(subset = ["Unnamed: 9"], inplace=True)
            breb_end.rename(columns={'Unnamed: 0': 'breb_serial','Unnamed: 1': 'sms_ac_no',
                            'Unnamed: 4': 'bill_no',
                            'Unnamed: 7': 'due_date',
                            'Unnamed: 19': 'transaction_id',
                            'Unnamed: 17': 'initiated_at',
                            'Unnamed: 15': 'amount',
                            },
                    inplace=True, errors='raise') #remane as needed
            breb_end = breb_end.drop(breb_end.columns[[2, 3, 5,6,8,9,10,11,12,13,14,16,18]], axis=1)
            return breb_end

    @staticmethod
    def drop_duplicates(dataframe):
        dataframe=dataframe.drop_duplicates()
        return dataframe

    @staticmethod
    def drop_index_column(dataframe):
        dataframe=dataframe.drop(dataframe.columns[0], axis=1)
        return dataframe