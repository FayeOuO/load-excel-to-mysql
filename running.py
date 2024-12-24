import pandas as pd
import sycm
from datetime import datetime
import mappings,int_list

host = 'localhost'
user = 'root'
port = 3306
password = '513311'
database = 'sycm'
# table_name = 'sycm_new_traffic_sources_store_channel_temp'
engine = sycm.create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
template_shop_df = pd.read_excel(r"D:\桌面\12.12 生意参谋\店铺渠道1.xlsx")
template_advantage_df = pd.read_csv(r"D:\桌面\12.12 生意参谋\经营优势来源渠道.csv")
template_all_product_df = pd.read_csv(r"D:\桌面\12.12 生意参谋\全部商品.csv")
dir = (r"D:\桌面\12.12 生意参谋\test")
date_format = "%Y-%m-%d"
present_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for file in sycm.list_all_file(dir):
    if '无线店铺流量来源' in file:
        sycm.process_file_to_mysql(
            file=file,
            sheetname='店铺渠道',
            template_df=template_shop_df,
            column_mapping=mappings.shop_channel_mapping,
            date_format=date_format,
            column_list = int_list.shop_int_list,
            platform_name='淘系生意参谋(新版)',
            store_name='馥绿德雅旗舰店',
            present_time=present_time,
            job_work_uuid='1',
            table_name='sycm_new_traffic_sources_store_channel_temp',
            engine=engine
        )
        sycm.process_file_to_mysql(
            file=file,
            sheetname='经营优势来源渠道',
            template_df=template_advantage_df,
            column_mapping=mappings.business_advantage_mapping,
            date_format=date_format,
            column_list=int_list.advantage_int_list,
            platform_name='淘系生意参谋(新版)',
            store_name='馥绿德雅旗舰店',
            present_time=present_time,
            job_work_uuid="2",
            table_name='sycm_new_traffic_sources_business_advantages_channels',
            engine=engine
        )
    else:
        sycm.process_file_to_mysql(
            file=file,
            sheetname='【生意参谋平台】1',
            column_mapping=mappings.allproduct_mapping,
            template_df=template_all_product_df,
            date_format=date_format,
            column_list=int_list.all_product_int_list,
            platform_name='淘系生意参谋(新版)',
            store_name='馥绿德雅旗舰店',
            present_time=present_time,
            job_work_uuid="3",
            table_name='sycm_new_product_ranking_allproducts',
            engine=engine
        )