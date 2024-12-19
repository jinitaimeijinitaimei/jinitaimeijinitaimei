import pandas as pd
import os

# 定义输出目录
output_dir = '/home/isss/ravenpackdataset/'

# 确保输出目录存在
os.makedirs(output_dir, exist_ok=True)

# 处理每年的数据
for year in range(2000, 2022):
    # 读取CSV文件
    file_path = f"/home/isss/ravenpackdataset/{year}-events2.csv"
    df = pd.read_csv(file_path)

    # 手动线性映射ESS列到-1到1的范围，并保留两位小数
    min_ess = df['ESS'].min()
    max_ess = df['ESS'].max()
    df['ESS'] = round(2 * (df['ESS'] - min_ess) / (max_ess - min_ess) - 1, 2)

    # 过滤G_ENS_SIMILARITY_GAP大于1的行
    df = df[df['G_ENS_SIMILARITY_GAP'] > 1]

    # 保存处理后的数据到新的CSV文件
    output_file_path = os.path.join(output_dir, f"{year}_events2.csv")
    df.to_csv(output_file_path, index=False)

    print(f"{year}年的数据已经保存")