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
    dic[year] = pl.read_csv(f'/home/isss/ravenpackdataset/{year}-events.csv')

# 获取所有唯一的 GROUP 类别
li = dic[2008]['GROUP'].unique().to_list()

# 初始化类别列
type_columns = pl.DataFrame({category: [0] * ceodata.shape[0] for category in li})

# 拼接 ceodata 和 type_columns
ceodata = ceodata.hstack(type_columns)

def process_row(index, row, dic, li):
    year = row['year']
    rp_entity_id = str(row['RP_ENTITY_ID']).strip()
    category_counts = {category: 0 for category in li}

    event_data = dic.get(year)
    if event_data is not None:
        event_data = event_data.with_columns([
            pl.col('RP_ENTITY_ID').cast(pl.Utf8),
            pl.col('GROUP').cast(pl.Utf8)
        ])
        for TY in li:
            TY = str(TY)
            matching_events = event_data.filter(
                (pl.col('RP_ENTITY_ID') == rp_entity_id) & 
                (pl.col('GROUP') == TY)
            )
            count = matching_events.shape[0]
            category_counts[TY] = count

    return index, category_counts

# 使用 ThreadPoolExecutor 来并行处理任务
with ThreadPoolExecutor(max_workers=16) as executor:
    futures = []
    for index, row in enumerate(ceodata.iter_rows(named=True)):
        futures.append(executor.submit(process_row, index, row, dic, li))
    
    results = []
    for future in tqdm(futures, total=len(futures), desc="Processing rows"):
        results.append(future.result())

# 将结果写入 DataFrame
# 由于 polars DataFrame 是不可变的，我们将其转换为 pandas DataFrame 进行更新
ceodata_pd = ceodata.to_pandas()
for index, category_counts in results:
    for category, count in category_counts.items():
        ceodata_pd.at[index, category] = count

# 将 pandas DataFrame 转换回 polars DataFrame
ceodata = pl.from_pandas(ceodata_pd)

# 保存最终结果
ceodata.write_csv('/home/isss/ravenpackdataset/ceodata.csv')