import os
import numpy as np
import pandas as pd
import ujson
from misc import load_config
import sys

np.set_printoptions(threshold=sys.maxsize)

# def get_timestamp_from_filename(filename):
#     """Extract the timestamp from the filename of an order book snapshot or update file."""
#     # 20230425000503.txt
#     # extract timestamp from filename
#     timestamp_str = filename[:-4]
#     timestamp = int(timestamp_str)
#     print(f'timestamp: {timestamp} ({type(timestamp)})')

def read_snapshot_file(pair, date, begin_time, end_time):
    
    begin_time_formatted = begin_time.replace(':', '')
    end_time_formatted = end_time.replace(':', '')
    
    try:
        snapshot_files = os.listdir(os.path.join(data_dir, date, pair, 'orderbook_snapshots'))
        
        # filename is in format: YYYYMMDDHHMMSS.txt
        # use regex to sort in chronological order
        snapshot_files_sorted = sorted(snapshot_files, key=lambda x: x[8:14])
        
        for snapshot_file in snapshot_files_sorted:
            if snapshot_file[8:14] >= begin_time_formatted and snapshot_file[8:14] <= end_time_formatted:
                
                # read as json
                snapshot_file = os.path.join(data_dir, date, pair, 'orderbook_snapshots', snapshot_file)
                with open(snapshot_file, 'r') as f:
                    
                    snapshot_file_json = ujson.load(f)
                    lastUpdateId = snapshot_file_json['lastUpdateId']
                    
                    print(f'Opening snapshot with LastUpdateId: {lastUpdateId} (file: {snapshot_file}))')
                    # bids = np.array(snapshot_file['bids'])
                    # asks = np.array(snapshot_file['asks'])

                    bids = pd.DataFrame(snapshot_file_json['bids'], columns=['price', 'quantity'], dtype=float)
                    bids['side'] = 'bid'
                    asks = pd.DataFrame(snapshot_file_json['asks'], columns=['price', 'quantity'], dtype=float)
                    asks['side'] = 'ask'
                    
                    ob_snapshot = pd.concat([bids, asks], axis=0).sort_values(by='price', ascending=False)
                    
                    ob_snapshot_json = ob_snapshot.to_json(orient='records')
                    print(ob_snapshot_json)
                    
                    with open ('ob_snapshot.json', 'w') as f:
                        f.write(ob_snapshot_json)
                    
                    # implement above as numpy  
                    # bids = np.array(snapshot_file_json['bids'])
                    # bids = np.insert(bids, 0, np.arange(len(bids)), axis=1)
                    # bids = np.insert(bids, 1, 0, axis=1) # 0 = bid
                    # asks = np.array(snapshot_file_json['asks'])
                    # asks = np.insert(asks, 0, np.arange(len(asks)), axis=1)
                    # asks = np.insert(asks, 1, 1, axis=1) # 1 = ask
                    # ob_snapshot = np.concatenate((bids, asks), axis=0)
                    # ob_snapshot = ob_snapshot[ob_snapshot[:, 2].argsort()[::-1]]
                    
                    # # print whole order book
                    # print(ob_snapshot)
                    
    
    except FileNotFoundError:
        print('No snapshot files found. Choose another date!')
        return
    
    

 
if __name__ == '__main__':
    
    # Define the path to the data directory
    config = load_config()
    data_dir = '../test_data_rigged'

    # Define the pair and date for which you want to replay the order book
    pair = 'ADAUSDT'
    date = '20230425'
    begin_time = '03:00:00'
    end_time = '05:00:03'

    # measure time it takes to run read_snapshot_file
    import time
    start = time.time()
    read_snapshot_file(pair, date, begin_time, end_time)
    end = time.time()
    # print time in ms
    print(f'Time elapsed: {(end - start) * 1000} ms')