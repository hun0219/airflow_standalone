import pandas as pd
import sys

READ = sys.argv[1]
SAVE = sys.argv[2]

df = pd.read_csv(READ, on_bad_lines='skip', names=['dt', 'cmd', 'cnt'])

#df.tail(3)
df['dt'] = df['dt'].str.replace('^', '')

#'coerce'는 변환할 수 없는 데이터를 만나면 그 값을 강제로 NaN으로 바꿈
df['cnt'] = pd.to_numeric(df['cnt'], errors='coerce')
#df['cnt'] = df['cnt'].astype(object)

# NaN 값을 원하는 방식으로 처리 (예: 0으로채우기)
df['cnt'] = df['cnt'].fillna(0).astype(int)

df['cnt'] = df['cnt'].astype(int)

df.to_parquet(SAVE, partition_cols=['dt'])
#df.dtypes
#df.tail(3)

#fdf = df[df['cmd'].str.contains('aws')]

#cnt = fdf['cnt'].sum()

#print(cnt)

#df.to_parquet("~/tmp/history.parquet")
