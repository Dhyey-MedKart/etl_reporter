import logging
from celery import shared_task
from .query import report3_data,report2_data,report1_data,data_to_sql,report4_data,report5_data,users,products,stores

def nan_handler(df):
    for col in df.columns:
        df.loc[df[col].isna(), col] = -999
    return df  

@shared_task(bind=True)
def test_func(self, data):
    logging.basicConfig(filename="etl_logs.log",
                    filemode='a',
                    level=logging.ERROR,
                    format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("ETL Task Started")
    print("Starting ETL Task...")

    
    rp1 = report1_data()
    rp2 = report2_data()
    rp3 = report3_data()
    rp4 = report4_data()
    rp5 = report5_data()
    u = users()
    p = products()
    s = stores()

    data_to_sql(rp1,'report1')
    data_to_sql(rp2,'report2')
    data_to_sql(rp3,'report3')
    data_to_sql(rp4,'report4')
    data_to_sql(rp5,'report5')
    data_to_sql(u,'users')
    data_to_sql(p,'products')
    data_to_sql(s,'stores')