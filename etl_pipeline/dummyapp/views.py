from django.shortcuts import render
from django.http import HttpResponse
from .tasks import test_func
from .connections import conn_string_local
import sqlalchemy as db
import pandas as pd
import numpy as np
from time import sleep

# Create your views here.

remote_engine = db.create_engine(conn_string_local())
def execute_query(query):
    with remote_engine.connect() as connection:
        df = pd.read_sql(query, remote_engine)
    
    return df
def test(request):
    #return HttpResponse("Done") task in the background
    return render(request, 'home.html')


def report2(request):
    users = execute_query('SELECT * FROM users')
    stores = execute_query('SELECT * FROM stores')
    # return HttpResponse("hello from Home_Page")
    # q2 = '''select * from brand_cat_product'''
    # brand_cat_product = execute_query(q2)
    # q2 = '''select * from Brand_tieup_2'''
    # Brand_tieup_2 = execute_query(q2)
    brand_cat_product = pd.DataFrame({'Kapiva': [11377,18437],
                                    'Himalaya': [30196,18739]})
    Brand_tieup_2 = pd.DataFrame({'month': ['01-02-2025','01-02-2025','01-02-2025','01-02-2025','01-02-2025','01-02-2025'],
                                'Brand_cat':['Kapiva','Kapiva','Kapiva','Himalaya','Himalaya','Himalaya'],
                                'Brand_sales_rate': [4000,8000,12000,5000,9000,14000],
                                "%applied": [4,10,15,5,12,20]})
    rp2_df1 = pd.DataFrame(columns=['store_id','billing_user_id','sales','Brand_cat'])
    for col in brand_cat_product.columns:
        pd_id = str(tuple(set(brand_cat_product[col])))
        q = f'''select 
            store_id,
            billing_user_id,
            sum(sales) as sales
        from report2
        where product_id in {pd_id}
        and date >= DATE_TRUNC('month', CURRENT_DATE)
        group by store_id, billing_user_id, product_id'''
        rp2_df = execute_query(q)
        rp2_df['Brand_cat'] = col
        rp2_df1 = pd.concat([rp2_df1, rp2_df], ignore_index=True)
    #rp2_dict[col] = rp2_df

    percent_applied = []
    rp2_df1 = rp2_df1.sort_values(by=['Brand_cat'])  # Correct sorting

    for col in sorted(set(Brand_tieup_2['Brand_cat'].values)):  # Ensure sorted order
        brand_percent1 = Brand_tieup_2[Brand_tieup_2['Brand_cat'] == col]
        brand_percent = sorted(brand_percent1['Brand_sales_rate'].values)  # Ensure sorting
        df = rp2_df1[rp2_df1['Brand_cat'] == col]
        
        applied_percent_list = []  # Temporary list for each Brand_cat

        for _, row in df.iterrows():  
            applied_percent = None  # Default value
            
            for i in range(len(brand_percent) - 1):  
                if brand_percent[i] < row['sales'] < brand_percent[i + 1]:
                    applied_percent = brand_percent1.iloc[i]['%applied']
                    break  

            if applied_percent is None and row['sales'] >= brand_percent[-1]:
                applied_percent = brand_percent1.iloc[-1]['%applied']
            
            applied_percent_list.append(applied_percent)

        rp2_df1.loc[rp2_df1['Brand_cat'] == col, '%applied'] = applied_percent_list  
    rp2_df1['TotalIncentive'] = rp2_df1['%applied']*rp2_df1['sales']/100
    rp2_df1['billing_user_id'] = rp2_df1['billing_user_id'].astype(int)
    rp2_df1['store_id'] = rp2_df1['store_id'].astype(int)
    rp2_df1 = rp2_df1.merge(users,how='left', left_on= ['billing_user_id'], right_on = ['id'])
    rp2_df1 = rp2_df1.merge(stores,how='left', left_on = ['store_id'], right_on = ['id'])
    rp2_df1 = rp2_df1.drop(columns=['billing_user_id','store_id','id_x','id_y'])
    rp2_df1 = rp2_df1.to_dict('records')
    return render(request, 'report1.html',{'leads': rp2_df1, 'report_id': 2})


def report1(request):
    sleep(3)  # Simulate delay
    q = '''SELECT * FROM report1 WHERE date >= DATE_TRUNC('month', CURRENT_DATE);'''
    
    users = execute_query('SELECT * FROM users')
    stores = execute_query('SELECT * FROM stores')
    products = execute_query('SELECT ws_code, product_name FROM products')
    
    df_in = pd.DataFrame({
        'product_code': [11377, 30196, 18437],
        'Incentives': [350, 30, 100]
    })

    df = execute_query(q)

    df['billing_user_id'] = pd.to_numeric(df['billing_user_id'], errors='coerce').fillna(0).astype(int)

    df = pd.merge(df_in, df, how='left', on='product_code')
    df.rename(columns={'Incentives': 'Incentive per qty'}, inplace=True)
    
    df['TotalIncentive'] = df['Incentive per qty'] * df['qty']
    df = df.drop(columns=['index', 'date'], errors='ignore')

    df = df.merge(users, how='left', left_on='billing_user_id', right_on='id')
    df = df.merge(stores, how='left', left_on='store_id', right_on='id')
    df = df.merge(products, how='left', left_on='product_code', right_on='ws_code')

    df = df.drop(columns=['billing_user_id', 'store_id', 'product_code', 'index_x', 'index_y', 'id_x', 'id_y', 'ws_code'], errors='ignore')

    df = df.fillna('None')
    df = df.to_dict('records')

    return render(request, 'report1.html', {'leads': df, 'report_id': 1})


def report3(request):
    q = '''select * from report3 where date = '01-09-2024' '''
    df = execute_query(q)
    target_df = pd.DataFrame({'StoreName':['MEDKART B AKOTA'],
                            'Sales': [4000],
                            'Generic':[1000],
                            'Offer': ['No'],
                            'OTC':[2000],
                            'MSP':[1],
                            'WOW': [5000]})
    result = target_df.merge(df, how = 'left', left_on=['StoreName'], right_on = ['store_name'])
    result = result.drop(columns=['Offer','OTC','WOW','index','billing_user_id','date'])
    result['Generic%'] = result['GenericSales']/result['Generic']*100
    result['Target_achieved'] = (result['total_amount'] + result['Returnsales'])/result['Sales']*100
    result['msp_count'] = pd.to_numeric(result['msp_count'], errors='coerce')
    result['Eligible'] = np.where(
        (result['Generic%'] >= 100) & 
        (result['Target_achieved'] >= 1) &
        (result['msp_count'] >= result['MSP']),
        True, 
        False
    )
    result = result.to_dict('records')
    return render(request, 'report1.html', {'leads': result, 'report_id': 3})



def report4(request):
    spot = pd.DataFrame({
        'store_id': [36, 23],
        'date': ['2024-09-15-2024-09-17', '2024-09-15-2024-09-17'],
        'SpotTarget': [7000, 5000],
        'GenSpotTarget': [300, 100]
    })

    spot['date_split'] = spot['date'].str.split('-')

    def extract_dates(date_list):
        if len(date_list) >= 6:  
            return '-'.join(date_list[:3]), '-'.join(date_list[3:6])
        return '-'.join(date_list[:3]), None

    spot[['Start_date', 'End_date']] = spot['date_split'].apply(lambda x: pd.Series(extract_dates(x)))

    spot.drop(columns=['date_split'], inplace=True)

    report4 = pd.DataFrame()

    for _, row in spot.iterrows():
        date_range = pd.date_range(start=row['Start_date'], end=row['End_date']).strftime('%Y-%m-%d')

        query = f"""
            SELECT * FROM report4 
            WHERE store_id = {row['store_id']} 
            AND date BETWEEN '{row['Start_date']}' AND '{row['End_date']}'
        """
        df = execute_query(query)

        df['date'] = df['date'].astype(str)

        missing_dates = set(date_range) - set(df['date'])

        for missing_date in missing_dates:
            new_row = {
                'store_id': row['store_id'],
                'date': missing_date,
                'spotsale': 0,  # Default values
                'spotsalereturn': 0,
                'genspotsale': 0,
                'genspotsalereturn': 0
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        df['SpotTarget'] = row['SpotTarget'] / 3
        df['GenSpotTarget'] = row['GenSpotTarget'] / 3

        df['achieved'] = np.where(
            (df['spotsale'] - df['spotsalereturn'] >= df['SpotTarget']) & 
            (df['genspotsale'] - df['genspotsalereturn'] >= df['GenSpotTarget']),
            True, 
            False
        )

        df.drop(columns=['index'], errors='ignore', inplace=True)

        report4 = pd.concat([report4, df], ignore_index=True)
    
    report4 = report4.to_dict('records')
    return render(request, 'report1.html', {'leads': report4, 'report_id': 3})  

def report5(request):
    q = '''SELECT * from report5 where extract(month from date) = 9 and extract(year from date) = 2024 '''
    df = execute_query(q)
    df.drop(columns=['date','index'], errors='ignore', inplace=True)
    df = df.groupby(['store_id', 'billing_user_id']).sum().reset_index()
    df = df.to_dict('records')
    return render(request, 'report1.html', {'leads': df, 'report_id': 5})