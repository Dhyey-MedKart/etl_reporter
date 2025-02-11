import pandas as pd
from .connections import conn_string_remote,conn_string_local
import sqlalchemy as db


engine = db.create_engine(conn_string_remote())

remote_engine = db.create_engine(conn_string_local())

def execute_query(query):
    with engine.connect() as connection:
        df = pd.read_sql(query, engine)
    
    return df

def data_to_sql(df,table_name): 
    for col in df.select_dtypes(include=['number']).columns:
        df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) and x == int(x) else float(x))

    # Convert object columns (if needed)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)

    # Replace NaN with None for SQL compatibility
    df = df.where(pd.notna(df), None)

    with remote_engine.connect() as c:
        df.to_sql(name=table_name,if_exists='replace', con=remote_engine)
    return True


def report3_data():
    q1= '''SELECT 
            st.id as store_id,
            st.name as store_name
        FROM 
            stores st'''
    q2 = '''SELECT 
            store_id, 
            billing_user_id, 
            SUM(total_bill_amount) AS total_amount 
        FROM sales_invoices 
        GROUP BY store_id, billing_user_id'''
    q3 ='''SELECT 
            si.store_id, 
            si.billing_user_id, 
            SUM(sid.total_amount) AS gen_total_amount 
        FROM 
            sales_invoices si
        left JOIN 
            sales_invoice_details sid 
            ON si.id = sid.sales_invoice_id
        left JOIN 
            products p 
            ON sid.product_id = p.id
        WHERE
            p.mis_reporting_category in ('Generic medicine','Generic','Generic Medicine', 'Generic Multivitamin', 'Generic direct medicine')
        GROUP BY 
        si.store_id, si.billing_user_id'''
    q4 = '''SELECT 
            store_id, 
            billing_user_id, 
            SUM(bill_amount) AS total_amount 
        FROM sales_return
        GROUP BY store_id, billing_user_id'''
    q5 ='''Select 
            id as billing_user_id,
            name
        from
            users'''
    q6 = '''select
            si.store_id,
            si.billing_user_id,
            count(*) as msp_count
        from
            sales_invoice_details
        left join
            sales_invoices si
            on si.id = sales_invoice_id
        where
            product_id = 9
        group by 
            si.store_id, si.billing_user_id'''
    
    temp1 = execute_query(q1)
    temp2 = execute_query(q2)
    temp3 = execute_query(q3)
    temp4 = execute_query(q4)
    temp5 = execute_query(q5)
    temp6 = execute_query(q6)

    temp1.rename(columns={'total_amount':'total_billed_amount'}, inplace=True)
    temp3.rename(columns={'gen_total_amount':'GenericSales'}, inplace=True)
    temp4.rename(columns={'total_amount':'Returnsales'}, inplace=True)
    temp4['Returnsales'] = temp4['Returnsales'] * -1
    temp1_2 = temp1.merge(temp2, how='left', on=['store_id'])
    temp1_2_3 = pd.merge(temp1_2, temp3, how='left', on=['store_id', 'billing_user_id'])
    temp1_2_3_4 = pd.merge(temp1_2_3, temp4, how='left', on=['store_id', 'billing_user_id'])
    temp1_2_3_4_5 = pd.merge(temp1_2_3_4, temp5, how='left', on=['billing_user_id'])
    temp1_2_3_4_5_6 = pd.merge(temp1_2_3_4_5, temp6, how='left', on=['billing_user_id', 'store_id'])

    return temp1_2_3_4_5_6

def report1_data():
    q = '''select 
        sid.store_id,
        si.billing_user_id,
        sid.product_id as product_code,
        extract(year from sid.created_at) as year,
            extract(month from sid.created_at) as month,
            extract(day from sid.created_at) as day,
        count(*) as qty
    from sales_invoice_details sid
    left join sales_invoices si
        on si.id = sid.sales_invoice_id
    where sid.created_at > '01-09-2024'
    group by sid.store_id, si.billing_user_id, sid.product_id,extract(year from sid.created_at),extract(month from sid.created_at),extract(day from sid.created_at)'''
    t = execute_query(q)
    t['date'] = pd.to_datetime(t[['year', 'month', 'day']])
    t.drop(columns=['year', 'month', 'day'], inplace=True)
    return t


def report2_data():
    rp2_df1 = pd.DataFrame(columns=['store_id','billing_user_id','sales','Brand_cat'])

    q = f'''select 
        sid.store_id,
        si.billing_user_id,
        sid.product_id,
        extract(year from sid.created_at) as year, 
        extract(month from sid.created_at) as month, 
        sum(sid.total_amount) as sales
    from sales_invoice_details sid
    left join sales_invoices si
        on si.id = sid.sales_invoice_id
    where sid.created_at >= '01-09-2024'
    group by sid.store_id, si.billing_user_id, sid.product_id,extract(year from sid.created_at), extract(month from sid.created_at)'''
    rp2_df = execute_query(q)
    rp2_df1 = pd.concat([rp2_df1, rp2_df], ignore_index=True)
    #rp2_dict[col] = rp2_df
    rp2_df1['date'] = pd.to_datetime(rp2_df1[['year', 'month']].assign(day=1))
    rp2_df1.drop(columns=['year','month'], inplace=True)
    return rp2_df1  


def report3_data():
    q1= '''SELECT 
            st.id as store_id,
            st.name as store_name
        FROM 
            stores st'''
    q2 = '''SELECT 
            store_id, 
            billing_user_id, 
            extract(month from created_at) as month, 
            extract(year from created_at) as year,
            SUM(total_bill_amount) AS total_amount 
        FROM sales_invoices 
        where created_at > '01-09-2024'
        GROUP BY store_id, billing_user_id,extract(month from created_at), extract(year from created_at)'''
    q3 ='''SELECT 
            si.store_id, 
            si.billing_user_id, 
            extract(month from si.created_at) as month, 
            extract(year from si.created_at) as year,
            SUM(sid.total_amount) AS gen_total_amount 
        FROM 
            sales_invoices si
        left JOIN 
            sales_invoice_details sid 
            ON si.id = sid.sales_invoice_id
        left JOIN 
            products p 
            ON sid.product_id = p.id
        WHERE
            p.mis_reporting_category in ('Generic medicine','Generic','Generic Medicine', 'Generic Multivitamin', 'Generic direct medicine')
            and si.created_at > '01-09-2024'
        GROUP BY 
        si.store_id, si.billing_user_id,extract(month from si.created_at), extract(year from si.created_at)'''
    q4 = '''SELECT 
            store_id, 
            billing_user_id, 
            extract(month from created_at) as month, 
            extract(year from created_at) as year,
            SUM(bill_amount) AS total_amount 
        FROM sales_return
        where
            created_at > '01-09-2024'
        GROUP BY store_id, billing_user_id,extract(month from created_at), extract(year from created_at)'''
    q5 ='''Select 
            id as billing_user_id,
            name
        from
            users'''
    q6 = '''select
            si.store_id,
            si.billing_user_id,
            extract(month from si.created_at) as month, 
            extract(year from si.created_at) as year,
            count(*) as msp_count
        from
            sales_invoice_details
        left join
            sales_invoices si
            on si.id = sales_invoice_id
        where
            product_id = 9
            and si.created_at > '01-09-2024'
        group by 
            si.store_id, si.billing_user_id,extract(month from si.created_at), extract(year from si.created_at)'''

    temp1 = execute_query(q1)
    temp2 = execute_query(q2)
    temp3 = execute_query(q3)
    temp4 = execute_query(q4)
    temp5 = execute_query(q5)
    temp6 = execute_query(q6)

    temp1.rename(columns={'total_amount':'total_billed_amount'}, inplace=True)
    temp3.rename(columns={'gen_total_amount':'GenericSales'}, inplace=True)
    temp4.rename(columns={'total_amount':'Returnsales'}, inplace=True)
    temp4['Returnsales'] = temp4['Returnsales'] * -1
    temp1_2 = temp1.merge(temp2, how='left', on=['store_id'])
    temp1_2_3 = pd.merge(temp1_2, temp3, how='left', on=['store_id', 'billing_user_id','month', 'year'])
    temp1_2_3_4 = pd.merge(temp1_2_3, temp4, how='left', on=['store_id', 'billing_user_id','month', 'year'])

    temp1_2_3_4_5 = pd.merge(temp1_2_3_4, temp5, how='left', on=['billing_user_id'])
    temp1_2_3_4_5_6 = pd.merge(temp1_2_3_4_5, temp6, how='left', on=['billing_user_id', 'store_id','month', 'year'])   
    temp1_2_3_4_5_6['date'] = pd.to_datetime(temp1_2_3_4_5_6[['year', 'month']].assign(day=1))
    temp1_2_3_4_5_6.drop(columns=['year', 'month'], inplace=True)
    
    return temp1_2_3_4_5_6

def report4_data():
    sd = '01-09-2024'
    ed = '11-02-2025'
    q = f'''SELECT 
            si.store_id,
            SUM(sid.total_amount) AS spotsale,
            extract(year from sid.created_at) as year,
            extract(month from sid.created_at) as month,
            extract(day from sid.created_at) as day
        FROM 
            sales_invoices si
        left JOIN 
            sales_invoice_details sid 
            ON si.id = sid.sales_invoice_id
        left JOIN 
            products p 
            ON sid.product_id = p.id
        WHERE
            sid.created_at between '{sd}' and '{ed}'
        GROUP BY 
        si.store_id,extract(year from sid.created_at),extract(month from sid.created_at),extract(day from sid.created_at)'''

    df1 = execute_query(q)
    df1['date'] = pd.to_datetime(df1[['year', 'month', 'day']])
    df1.drop(columns=['year', 'month', 'day'], inplace=True)
    # genspotsale = df1['gen_total_amount'].values
    q = f'''SELECT 
            si.store_id,
            SUM(sid.total_amount) AS genspotsale,
            extract(year from sid.created_at) as year,
            extract(month from sid.created_at) as month,
            extract(day from sid.created_at) as day
        FROM 
            sales_invoices si
        left JOIN 
            sales_invoice_details sid 
            ON si.id = sid.sales_invoice_id
        left JOIN 
            products p 
            ON sid.product_id = p.id
        WHERE
            p.mis_reporting_category not in ('Branded medicine','Branded OTC','Branded Medicine', 'Branded Multivitamin','Branded Direct Medicine', 'Branded Direct medicine')
            and sid.created_at between '{sd}' and '{ed}'
        GROUP BY 
        si.store_id,extract(year from sid.created_at),extract(month from sid.created_at),extract(day from sid.created_at)'''
    df = execute_query(q)
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df.drop(columns=['year', 'month', 'day'], inplace=True)
    df1 = pd.merge(df1,df,how='left',on = ['store_id', 'date'])
    # spotsale = df['sales'].values
    q = f'''SELECT 
            si.store_id,
            SUM(sid.bill_amount) AS genspotsalereturn,
            extract(year from sid.created_at) as year,
            extract(month from sid.created_at) as month,
            extract(day from sid.created_at) as day
        FROM 
            sales_return si
        left JOIN 
            sales_return_details sid 
            ON si.sales_invoice_id = sid.sales_invoice_id
        left JOIN 
            products p 
            ON sid.product_id = p.id
        WHERE
            p.mis_reporting_category not in ('Branded medicine','Branded OTC','Branded Medicine', 'Branded Multivitamin','Branded Direct Medicine', 'Branded Direct medicine')
            and sid.created_at between '{sd}' and '{ed}'
        GROUP BY 
        si.store_id,extract(year from sid.created_at),extract(month from sid.created_at),extract(day from sid.created_at)'''
    df = execute_query(q)
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df.drop(columns=['year', 'month', 'day'], inplace=True)
    df1 = pd.merge(df1,df,how='left',on = ['store_id', 'date'])
    # genspotsalereturn = df['gen_total_amount'].values
    q = f'''SELECT 
            si.store_id,
            SUM(sid.bill_amount) AS spotsalereturn,
            extract(year from sid.created_at) as year,
            extract(month from sid.created_at) as month,
            extract(day from sid.created_at) as day
        FROM 
            sales_return si
        left JOIN 
            sales_return_details sid 
            ON si.sales_invoice_id = sid.sales_invoice_id
        left JOIN 
            products p 
            ON sid.product_id = p.id
        WHERE
            sid.created_at between '{sd}' and '{ed}'
        GROUP BY 
        si.store_id,extract(year from sid.created_at),extract(month from sid.created_at),extract(day from sid.created_at)'''
    df = execute_query(q)
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df.drop(columns=['year', 'month', 'day'], inplace=True)
    df1 = pd.merge(df1,df,how='left',on = ['store_id', 'date'])


    return df1


def report5_data():
    q = '''SELECT 
        store_id,
        billing_user_id,
        status,
        is_urgent_order,
        EXTRACT(day FROM created_at) AS day,
        EXTRACT(MONTH FROM created_at) AS month,
        EXTRACT(year FROM created_at) AS year,
        SUM(total_amount) AS total_amount
    FROM 
        advance_sales_invoices
    WHERE
        status IN ('INVOICED', 'PENDING')
        and created_at > '01-09-2024'
    GROUP BY 
        store_id, billing_user_id, status, is_urgent_order, EXTRACT(year FROM created_at), EXTRACT(MONTH FROM created_at), EXTRACT(day FROM created_at)'''
    df = execute_query(q)


    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df.drop(columns=['year', 'month', 'day'], inplace=True)
    df['Urgent_Order_Punched_Amt'] = 0.0
    df['Advance_Order_Punched_Amt'] = 0.0
    df['Urgent_Order_Invoiced_Amt'] = 0.0
    df['Advance_Order_Invoiced_Amt'] = 0.0

    df.loc[(df['status'] == 'PENDING') & (df['is_urgent_order']), 'Urgent_Order_Punched_Amt'] = df['total_amount']
    df.loc[(df['status'] == 'PENDING') & (~df['is_urgent_order']), 'Advance_Order_Punched_Amt'] = df['total_amount']
    df.loc[(df['status'] == 'INVOICED') & (df['is_urgent_order']), 'Urgent_Order_Invoiced_Amt'] = df['total_amount']
    df.loc[(df['status'] == 'INVOICED') & (~df['is_urgent_order']), 'Advance_Order_Invoiced_Amt'] = df['total_amount']
    result = df.groupby(['store_id', 'billing_user_id','date'], as_index=False).sum()
    # result['Urgent_Order_Sales%'] = (result['Urgent_Order_Invoiced_Amt'] / result['Urgent_Order_Punched_Amt'].replace(0, 1)) * 100
    # result['Advance_Order_Sales%'] = (result['Advance_Order_Invoiced_Amt'] / result['Advance_Order_Punched_Amt'].replace(0, 1)) * 100

    result.fillna(0, inplace=True)

    columns_to_format = [
        'Urgent_Order_Punched_Amt', 'Advance_Order_Punched_Amt',
        'Urgent_Order_Invoiced_Amt', 'Advance_Order_Invoiced_Amt',
        # 'Urgent_Order_Sales%', 'Advance_Order_Sales%'
    ]
    result[columns_to_format] = result[columns_to_format].round(2)

    result.drop(columns=['status','total_amount','is_urgent_order'], inplace=True)


    return result


def users():
    return execute_query('select id,name from users')

def products():
    return execute_query('select ws_code, product_name from products')

def stores():
    return execute_query('select id, name from stores where is_active is True')