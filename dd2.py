import os
import pandas as pd


def merge_monthly_files(year, file_type, encoding='ISO-8859-1'):
    """
    合并指定年份的月度 CSV 文件，仅保留 TOPIC 列不为空的数据，并删除指定列。
    """
    files = [
        f'/home/isss/ravenpackdataset/event/RPNA_DJEdition_{year}_4.0-Equities-Events/{year}-{str(i).zfill(2)}-{file_type}.csv'
        for i in range(1, 13)
    ]

    # 检查文件存在性
    valid_files = [file for file in files if os.path.exists(file)]
    if not valid_files:
        print(f"[警告] 未找到任何有效的 {file_type} 文件，年份: {year}")
        return pd.DataFrame()  # 返回空 DataFrame

    # 指定要删除的列
    columns_to_remove = [
    "ENS_KEY", 
    "ENS_ELAPSED", 
    #"G_ENS_KEY", 
    "G_ENS_ELAPSED", 
    "EVENT_SIMILARITY_KEY", 
    "RP_STORY_ID",
    "SUB_TYPE",
    "PROPERTY",
    "EVALUATION_METHOD",
    "MATURITY",
    ]
    
    # columns_to_remove=[]
    # 读取并合并数据
    data_list = []
    for file in valid_files:
        try:
            df = pd.read_csv(file, encoding=encoding, low_memory=False)
            # 只保留 TOPIC 列不为空的行
            if 'TOPIC' in df.columns:
                df = df[df['TOPIC'].notna()]
                # 删除指定的列（如果存在）
                df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
                # 筛选职位为 Chief Executive Officer 的行
                if 'POSITION_NAME' in df.columns:
                    # df = df[df['POSITION_NAME'].str.contains('Chief Executive Officer', na=False)]
                    pass
                data_list.append(df)
            else:
                print(f"[警告] 文件缺少 TOPIC 列: {file}")
        except Exception as e:
            print(f"[错误] 无法读取文件: {file}, 错误信息: {e}")

    if not data_list:
        print(f"[警告] 所有文件读取失败或无有效数据，年份: {year}, 类型: {file_type}")
        return pd.DataFrame()

    merged_data = pd.concat(data_list, ignore_index=True)
    print(f"[信息] 成功合并 {len(valid_files)} 个 {file_type} 文件，年份: {year}")
    return merged_data


def filter_and_save_data(data, match_file, output_file):
    """
    根据匹配文件筛选数据，并进一步筛选职位为 Chief Executive Officer 的行。
    """
    if data.empty:
        print(f"[警告] 输入数据为空，跳过处理: {output_file}")
        return

    try:
        # 读取匹配结果文件
        match_df = pd.read_csv(match_file)
    except Exception as e:
        print(f"[错误] 无法读取匹配文件: {match_file}, 错误信息: {e}")
        return

    # 筛选数据
    if 'RP_ENTITY_ID' not in data.columns or 'RP_ENTITY_ID' not in match_df.columns:
        print(f"[错误] 缺少必要的 'RP_ENTITY_ID' 列，无法筛选: {output_file}")
        return

    # 通过 RP_ENTITY_ID 进行初步筛选
    filtered_data = data[data['RP_ENTITY_ID'].isin(match_df['RP_ENTITY_ID'])]

    # 保存筛选后的数据
    try:
        filtered_data.to_csv(output_file, index=False)
        print(f"[信息] 筛选数据已保存: {output_file}")
    except Exception as e:
        print(f"[错误] 无法保存文件: {output_file}, 错误信息: {e}")


def process_yearly_data(start_year, end_year, match_file):
    """
    处理指定年份范围内的所有数据。
    """
    for year in range(start_year, end_year + 1):
        print(f"[信息] 开始处理年份: {year}")

        # equities-events 文件
        d_data = merge_monthly_files(year, file_type='equities-events')
        filter_and_save_data(
            data=d_data,
            match_file=match_file,
            output_file=f'/home/isss/ravenpackdataset/{year}-events2.csv'
        )


# 主程序入口
if __name__ == "__main__":
    MATCH_FILE = "/home/isss/ravenpackdataset/newdata_mapping.csv"
    START_YEAR = 2000
    END_YEAR = 2022

    process_yearly_data(start_year=START_YEAR, end_year=END_YEAR, match_file=MATCH_FILE)
