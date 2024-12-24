import pandas as pd
import os
import mappings # 导入同目录下的 mappings.py 文件
from datetime import datetime
import pymysql
from sqlalchemy import create_engine
'''
1. 从文件夹里面读取文件

传入文件夹地址读取文件夹里面的文件
'''
def list_all_file(dir):
    '''
    root 是当前遍历的目录路径。_ 是目录中的子目录列表（这里用下划线忽略了子目录，
    因为子目录不需要使用）。files 是当前目录中的文件列表。
    '''
    result = []
    for root, _, files in os.walk(dir):
        for file in files:
            result.append(os.path.join(root, file))
    return result
'''
2. 读取文件、处理数据

删除空行设置列索引
'''
def percent_to_float(df):
    """
    将 DataFrame 中包含百分数的列转换为浮点数格式。12% --> 0.12
    在字符串格式下去掉%，并除以100，最后转换为浮点型。
    """

    for col in df.columns:
        if df[col].dtypes == 'object':  # 检查列是否为字符串类型;这段代码是否可以去掉
            if df[col].str.contains('%', na=False).any():  # 检查列中是否包含百分号
                # 去掉百分号，无法转换的值替换为 NaN
                df[col] = df[col].str.replace('%', '', regex=True).apply(
                    lambda x: pd.to_numeric(x, errors='coerce')) / 100
                df[col] = df[col].astype(float)
    return df

def data_processing(path,sheetname, int_column_list, column_mapping):
    '''
    读取文件，去除空值，设置去除空值后的df的第一行作为列索引
    '''
    df = pd.read_excel(path, sheet_name=sheetname, header=None)
    df.dropna(inplace=True)
    df = df.set_axis(df.iloc[0], axis=1)  # 将第一行作为列索引
    df.drop(df.index[0], inplace=True)
    """
    按照映射把列索引改为英文，并把df中所有的_转换为空值；
    把df列中的,删除，目的为方便后续将‘123,45’（字符串类型），转换为12345的整数型；
    """
    df = df.rename(columns=column_mapping)  # 按照映射把表头改为英文
    df = df.replace('-', '') # 把所有_替换成空值
    df = df.replace(",", "", regex=True) # 数据处理前所有的数据类型均为df中的object格式，即字符串类型。将所有数字中的,去掉方便后续进行数据类型的转换。不加regex=True会导致类似123,45.67这类数字无法导入数据库。
    df = percent_to_float(df)
    df[int_column_list] = df[int_column_list].apply(lambda x:pd.to_numeric(x,errors="coerce").fillna(0).astype(int))
    return df

'''
3. 调整表结构

根据三个表数据库与df的映射关系,更改原始数据表头；按照数据库中的表格是调整列的顺序
'''
def mapping(template_df,target_df):
    template_columns = template_df.columns.tolist()
    aligned_data_df = target_df.reindex(columns=template_columns, fill_value='')
    return aligned_data_df
'''
4. 写入excel
'''
def append_to_excel(file_path, data_frame, sheet_name='Sheet1'):
    """将 DataFrame 追加到指定 Excel 文件的工作表中"""
    try:
        # 尝试以追加模式打开文件
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            # 追加数据到现有工作表
            data_frame.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=writer.sheets[sheet_name].max_row)
            print(f"数据已追加到 {file_path} 的工作表 {sheet_name}")
    except FileNotFoundError:
        # 文件不存在时，创建新文件
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='w') as writer:
            data_frame.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"文件不存在，已创建新文件 {file_path} 并写入数据。")
'''
5. 整合

增加特定列到df中
'''
def process_file_to_excel(file, sheetname, column_mapping, column_list,date_format, template_df, saved_to, platform_name, store_name,present_time):
    """处理单个文件并写入 Excel"""
    print(f'正在处理{file} {sheetname}')
    data = data_processing(file, sheetname=sheetname, int_column_list=column_list, column_mapping=column_mapping)
    data = data.rename(columns=column_mapping)
    data = data.replace('-', '')
    data['business_date'] = datetime.strptime(file.split('_')[-1].split('.')[0], date_format)
    # 按照文件名中的时间增加business_date列
    data['platform_name'] = platform_name
    data['store_name'] = store_name
    data['gather_time'] = present_time
    append_to_excel(saved_to, mapping(template_df, data), sheet_name=sheetname)
    # return data
'''
写入本地的数据库
'''
def process_file_to_mysql(file, sheetname, column_mapping, template_df, column_list,date_format, platform_name, store_name,present_time,job_work_uuid,table_name,engine):
    """处理单个文件并写入 Excel"""
    print(f'正在处理{file} {sheetname}')
    data = data_processing(file, sheetname=sheetname, int_column_list=column_list, column_mapping=column_mapping)
    data = data.rename(columns=column_mapping)
    data = mapping(template_df , data)
    """
    不mapping：“全部”df的列数大于数据库中的列，导入数据库时报错；
    mapping：会导致空值无法导入数据库，如id列和job_work_uuid列，选择增加job_work_uuid列，删除
    id列。
    """
    data = data.drop(columns=['id'])
    data['business_date'] = datetime.strptime(file.split('_')[-1].split('.')[0], date_format) # business_time等于文件上面的日期，转换成时间戳类型
    data['platform_name'] = platform_name # 平台名称
    data['store_name'] = store_name # 店铺名称
    data['gather_time'] = present_time # 收集时间及当前时间
    data['job_work_uuid'] = job_work_uuid
    data = data.replace('', None)
    try:
        data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"数据成功写入表 {table_name}！")
    except Exception as e:
        print(f"数据写入失败：{e}")