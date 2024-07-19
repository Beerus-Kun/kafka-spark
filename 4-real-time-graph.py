import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.animation import FuncAnimation
import json
import matplotlib.dates as mdates
from kafka.consumer import KafkaConsumer
from pandas.core.frame import DataFrame 

# plt.style.use('fivethirtyeight')

consumer = KafkaConsumer('streaming', auto_commit_interval_ms = 60000, auto_offset_reset='earliest')
df = pd.DataFrame()

def transform(new_df:DataFrame, df:DataFrame):
    new_df = new_df.drop(columns='start_time')
    new_df = new_df.rename(columns={'end_time': 'time'})
    new_df['time'] = pd.to_datetime(new_df['time'])
    new_df = new_df.reset_index(drop=True)
    new_df = new_df.set_index('time')

    if 'total_buy' in df.columns:
        new_df = new_df.rename(columns={'total_buy': 'total_buy_1',
                        'total_click': 'total_click_1',
                        'total_search': 'total_search_1'})
        
        join_df = pd.concat([df, new_df], axis=1)

        trans_df = pd.DataFrame()
        trans_df['total_buy'] = join_df['total_buy_1'].mask(join_df['total_buy_1'].isna(), join_df['total_buy'])
        trans_df['total_click'] = join_df['total_click_1'].mask(join_df['total_click_1'].isna(), join_df['total_click'])
        trans_df['total_search'] = join_df['total_search_1'].mask(join_df['total_search_1'].isna(), join_df['total_search'])

        df = trans_df


    else:
        df = new_df
        
    return df

fig, ax = plt.subplots()
def animate(i):
    plt.cla()
    global df
    
    new_data = json.loads(next(consumer).value.decode("utf-8"))
    new_df = pd.DataFrame([new_data])
    df = transform(new_df, df)
    print(df)
    ax.plot(df.index.to_frame()['time'].dt.tz_localize(None), df['total_buy'], label='Buy Amount')
    ax.plot(df.index.to_frame()['time'].dt.tz_localize(None), df['total_click'], label='Click Amount')
    ax.plot(df.index.to_frame()['time'].dt.tz_localize(None), df['total_search'], label='Search Amount')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.xlabel('time')
    plt.legend(loc='best')


ani = FuncAnimation(plt.gcf(), animate, interval = 500)

plt.tight_layout()
plt.show()