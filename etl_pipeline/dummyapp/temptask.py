import logging
import pandas as pd
import numpy as np
from celery import shared_task
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
from .local_session import create_local_session,SalesInvoiceDetail,SalesReturn,SalesReturnDetail,AdvanceSalesInvoice,Product,stores,users,sales_invoices
from .remote_session import create_remote_session
from sqlalchemy import text



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

    queries = {
        "products": "SELECT ws_code, product_name, mis_reporting_category FROM products;",
        "stores": "SELECT id, name, is_active FROM stores;",
        "users": "SELECT id, name FROM users;",
        "sales_invoices": "SELECT id, store_id, billing_user_id, customer_id, prepaid_amount, total_bill_amount, created_at FROM sales_invoices WHERE created_at > '2024-12-01 00:00:00.000';",
        "sales_invoice_details": "SELECT sales_invoice_id, product_id, quantity, bill_amount FROM sales_invoice_details WHERE created_at > '2024-12-01 00:00:00.000';",
        "sales_return": "SELECT id, store_id, billing_user_id, customer_id, sales_invoice_id, bill_amount, created_at FROM sales_return WHERE created_at > '2024-12-01 00:00:00.000';",
        "sales_return_details": "SELECT sales_invoice_detail_id, sales_invoice_id, product_id, quantity, bill_amount FROM sales_return_details WHERE created_at > '2024-12-01 00:00:00.000';",
        "advance_sales_invoices": "SELECT sales_invoice_draft_id, store_id, status, is_urgent_order, total_amount FROM advance_sales_invoices WHERE created_at > '2024-12-01 00:00:00.000';"
    }

    model_mapping = {
        "products": Product,
        "stores": stores,
        "users": users,
        "sales_invoices": sales_invoices,
        "sales_invoice_details": SalesInvoiceDetail,
        "sales_return": SalesReturn,
        "sales_return_details": SalesReturnDetail,
        "advance_sales_invoices": AdvanceSalesInvoice,
    }

    try:
        local_session = create_local_session()
        dataframes = {}

        for table, query in queries.items():
            try:
                with create_remote_session() as remote_session:
                    result = remote_session.execute(text(query))
                    columns = result.keys() 
                    rows = result.fetchall() 
                    df = pd.DataFrame(rows, columns=columns)
                    dataframes[table] = df
                    logging.info(f"Extracted data from {table} successfully.")
            except Exception as e:
                logging.error(f"Error fetching {table}: {e}")

        # Transform

        df = dataframes["products"]
        df.rename(columns={"mis_reporting_category": "category"}, inplace=True)
        df["is_active"] = df["ws_code"].apply(lambda x: True if pd.notna(x) else False)
        df["ws_code"] = df["ws_code"].apply(lambda x: x if pd.notna(x) else -1)
        dataframes["stores"].rename(columns={"name": "store_name"}, inplace=True)
        dataframes['sales_invoices']["total_bill_amount"] = dataframes['sales_invoices']["total_bill_amount"].apply(lambda x: None if pd.isna(x) else x)

        for table, df in dataframes.items():
            try:
                model = model_mapping[table]
                df = nan_handler(df)

                records = df.to_dict(orient="records")
                with local_session.begin(): 
                    if isinstance(records, pd.DataFrame):
                        records = records.replace("nan", np.nan)
                        records = records.where(pd.notna(records), None)
                    for record in records:
                        if table == 'products':
                            stmt = insert(model).values(**record).on_conflict_do_update(
                                index_elements=['ws_code'],
                                set_={"product_name": record["product_name"], "category": record["category"]}
                            )
                        elif table == 'stores':
                            stmt = insert(model).values(**record).on_conflict_do_update(
                                index_elements=['id'],
                                set_={"store_name": record["store_name"], "is_active": record["is_active"]}
                            )
                        else:
                            stmt = insert(model).values(**record)
                        local_session.execute(stmt)

                logging.info(f"Loaded {table} successfully.")

            except SQLAlchemyError as e:
                local_session.rollback()
                logging.error(f"Error loading {table}: {e}")
                print(f"Error loading {table}: {e}")
                continue

        logging.info("ETL Process Completed Successfully")
        print("ETL Process Completed.")
        return "ETL Completed."

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return f"ETL Failed: {str(e)}"

    finally:
        remote_session.close()
        local_session.close()
        print("Closed Connection")
