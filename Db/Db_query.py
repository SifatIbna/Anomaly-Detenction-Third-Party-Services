from dotenv import load_dotenv
from pathlib import Path
import os
import psycopg2 as pg
import pandas as pd

class Db_query:

    @staticmethod
    def load_connection():
        path_env = Path('../.env')
        load_dotenv(dotenv_path=path_env)

        host = os.getenv('HOST')
        port = os.getenv('PORT')
        dbname = os.getenv('DBNAME')
        user = os.getenv('USER')
        password = os.getenv('PASSWORD')

        '''
        "host='192.168.10.231' port=5432 dbname=warehouse user=rana password='fhghuityurUtyt6377Gggi'"
        '''

        connection_string = f"host='{host}' port={port} dbname={dbname} user={user} password='{password}'"

        connection = pg.connect(connection_string)
        return connection

    def breb_query(self,date_from,date_to):
        breb_query = '''
        select * from(select  transaction_id, trx_id, amount, date(initiated_at) initiated_at, date(completed_at) completed_at, sms_ac_no,
        concat('',bill_no) bill_no, bill_month,bill_year,
        jsonb_extract_path_text(breb_post_bill_info,'pbs_name_e') pbs_name, status from fundtransferuser.payment_brebpostpaidpayment

        where (date(initiated_at at time zone 'Asia/Dhaka') between '{0}' and '{1}')
        )
        as d
        where  status in ('success','to_finalize','success_not_finalized','finalize_failed','vendor_failed_manual','vendor_failed','processing')
        '''.format(date_from,date_to)
        breb_query_result = pd.read_sql_query(breb_query,self.load_connection())
        return breb_query_result



if __name__ == '__main__':
    query_result = Db_query().breb_query('2022-09-01','2022-09-02')
    print(type(query_result))