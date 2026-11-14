import pandas as pd
df=pd.read_csv('c:/Users/shaik/Downloads/project/youtube_trending_data.csv')
print('dtypes:', df.dtypes)
print('first:', df['Date'].iloc[0])
try:
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', format='mixed')
    print('mixed ok', df['Date'].dtype)
except Exception as e:
    print('mixed error', repr(e))
try:
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    print('infer ok', df['Date'].dtype)
except Exception as e:
    print('infer error', repr(e))
