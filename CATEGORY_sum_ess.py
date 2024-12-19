import polars as pl
from tqdm import tqdm
from joblib import Parallel, delayed
from concurrent.futures import ThreadPoolExecutor

# 读取数据
ceo = pl.read_csv('/home/isss/ravenpackdataset/ceo.csv')
ticker_entity_id = pl.read_csv('/home/isss/ravenpackdataset/ticker_entity_id.csv')

# 过滤 ceo 数据
filtered_ceo = ceo.filter(pl.col('ticker').is_in(ticker_entity_id['Ticker']))

# 合并 filtered_ceo 和 ticker_entity_id 数据，匹配规则为 ticker 和 Ticker
ceodata = filtered_ceo.join(ticker_entity_id, left_on='ticker', right_on='Ticker', how='left')

# 读取事件数据
years = range(2000, 2022)
dic = {}

for year in tqdm(years, desc="Loading event data"):
    dic[year] = pl.read_csv(f'/home/isss/ravenpackdataset/{year}_events.csv')

# 获取所有唯一的 GROUP 类别
li = dic[2008]['CATEGORY'].unique().to_list()
category_counts = {str(category) + str(x): 0 for category in li for x in ['_ess_positive', '_ess_neutral', '_ess_negative']}
category_counts_list = [str(category) + str(x) for category in li for x in ['_ess_positive', '_ess_neutral', '_ess_negative']]

# 初始化类别列
type_columns = pl.DataFrame({category: [0] * ceodata.shape[0] for category in li})

# 拼接 ceodata 和 type_columns
ceodata = ceodata.hstack(type_columns)

def process_row(index, row, dic, li):
    local_category_counts = {key: 0 for key in category_counts_list}
    year = row['year']
    rp_entity_id = str(row['RP_ENTITY_ID']).strip()

    event_data = dic.get(year)
    if event_data is not None:
        event_data = event_data.with_columns([
            pl.col('RP_ENTITY_ID').cast(pl.Utf8),
            pl.col('CATEGORY').cast(pl.Utf8)
        ])
        for TY in category_counts_list:
            group, _, sentiment = TY.split('_')
            if sentiment == 'positive':
                matching_events = event_data.filter(
                    (pl.col('RP_ENTITY_ID') == rp_entity_id) &  
                    (pl.col('CATEGORY') == group) &  
                    (pl.col('ESS') > 0)
                )
            elif sentiment == 'neutral':
                matching_events = event_data.filter(
                    (pl.col('RP_ENTITY_ID') == rp_entity_id) &  
                    (pl.col('CATEGORY') == group) &  
                    (pl.col('ESS') == 0)
                )
            elif sentiment == 'negative':
                matching_events = event_data.filter(
                    (pl.col('RP_ENTITY_ID') == rp_entity_id) &  
                    (pl.col('CATEGORY') == group) &  
                    (pl.col('ESS') < 0)
                )
            
            ess_sum = matching_events['ESS'].sum()
            print(ess_sum)
            local_category_counts[TY] = ess_sum

    return index, local_category_counts


# 使用 ThreadPoolExecutor 来并行处理任务
with ThreadPoolExecutor(max_workers=16) as executor:
    futures = []
    for index, row in enumerate(ceodata.iter_rows(named=True)):
        futures.append(executor.submit(process_row, index, row, dic, li))
    
    results = []
    for future in tqdm(futures, total=len(futures), desc="Processing rows"):
        results.append(future.result())

# 通过 Polars 直接处理和更新结果
new_columns = {category: [] for category in results[0][1].keys()}
for index, category_counts in results:
    for category, count in category_counts.items():
        new_columns[category].append(count)

# 将结果列添加到 ceodata 中
for category, values in new_columns.items():
    ceodata = ceodata.with_columns([pl.lit(values).alias(category)])

# 保存最终结果直接使用 Polars 写入 CSV
ceodata.write_csv('/home/isss/ravenpackdataset/ceodata_CATEGORY.csv')