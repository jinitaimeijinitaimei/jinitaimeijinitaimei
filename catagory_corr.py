import pandas as pd

# 读取数据
df = pd.read_csv('"/home/isss/ravenpackdataset/ceodata_CATEGORY.csv"')

# 删除包含缺失值的行
df = df.dropna(subset=['ceo_dismissal'])

# 选择非字符串类型的列
non_string_columns = df.select_dtypes(include=['number'])

# 计算相关性矩阵
correlation_matrix = non_string_columns.corr()

# 提取与 'ceo_dismissal' 列的相关性
ceo_dismissal_corr = correlation_matrix['ceo_dismissal'].drop('ceo_dismissal')

# 按相关性绝对值排序，获取前十个相关性最高的列
top_10_corr = ceo_dismissal_corr.abs().nlargest(10)

# 打印相关性最高的前十列及其相关系数
print("与 'ceo_dismissal' 相关性最高的前十列及其相关系数：")
print(top_10_corr)