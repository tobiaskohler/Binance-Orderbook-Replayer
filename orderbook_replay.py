import os
import pandas as pd
import ujson
from misc import load_config
import sys
import time



class OrderbookReplayer():
    
    
    def __init__(self, date: str, symbol: str, begin: str, end: str=None):
        
        '''
        symbol: str
            Symbol of the orderbook to be replayed
        begin: str
            Begin of the period to be replayed in format 'HH:MM:SS'
        end: str
            End of the period to be replayed in format 'HH:MM:SS'
        '''
        
        self.date = date
        self.symbol = symbol
        self.begin = begin.replace(':', '')
        self.end = end.replace(':', '')
        self.config = load_config()
        #self.data_dir = self.config['data_warehouse_path'] #PRODUCTION
        self.data_dir = '../test_data_rigged'
        
        print(f'Initializing OrderbookReplayer for {symbol} from {begin} to {end}')
    


    def get_snapshot_files(self) -> dict:
        
        snapshot_files_dict = {}
        
        try:
            snapshot_files = os.listdir(os.path.join(self.data_dir, self.date, self.symbol, 'orderbook_snapshots'))
            
            snapshot_files_sorted = sorted(snapshot_files, key=lambda x: x[8:14])
        
            for snapshot_file in snapshot_files_sorted:
                
                if snapshot_file[8:14] >= self.begin and snapshot_file[8:14] <= self.end:
                    
                    snapshot_file_path = os.path.join(self.data_dir, self.date, self.symbol, 'orderbook_snapshots', snapshot_file)
                    
                    with open(snapshot_file_path, 'r') as f:
                        
                        snapshot_file_json = ujson.load(f)
                        lastUpdateId = snapshot_file_json['lastUpdateId']
                        
                        snapshot_files_dict[lastUpdateId] = snapshot_file_path
                     
            print(f'Found {len(snapshot_files_dict)} snapshot files for {self.symbol} on {self.date} between {self.begin} and {self.end}')
            
            return snapshot_files_dict
            
        except FileNotFoundError:
            print(f'No snapshot files found for {self.symbol} on {self.date}')
            return None
        
    
    def merge_update_files(self) -> ujson:

        update_files = os.listdir(os.path.join(self.data_dir, self.date, self.symbol, 'orderbook_updates'))
        
        update_files_sorted = sorted(update_files, key=lambda x: x[8:14])

        update_files_list = []
        
        for update_file in update_files_sorted:
            
            update_file_path = os.path.join(self.data_dir, self.date, self.symbol, 'orderbook_updates', update_file)
            
            update_files_list.append(update_file_path)
        
        with open(os.path.join(self.data_dir, self.date, self.symbol, 'orderbook_updates', 'all_updates.txt'), 'w') as outfile:
            
            for filename in update_files_list:

                if filename.endswith('.txt'):

                    with open(filename, 'r') as input_file:

                        file_contents = input_file.read()

                        outfile.write(file_contents)
        
                
        print(f'Found {len(update_files_list)} update files for {self.symbol} on {self.date} between {self.begin} and {self.end}')

        return update_files_list



 
#6 loop over snapshot-list, starting to read first available snapshot file, updating with updates until u of next snapshot file is reached

    # https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream
    # Drop any event where u is <= lastUpdateId in the snapshot.
    # The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
    # While listening to the stream, each new event's U should be equal to the previous event's u+1.
    # The data in each event is the absolute quantity for a price level.
    # If the quantity is 0, remove the price level.
    # Receiving an event that removes a price level that is not in your local order book can happen and is normal.
    
    
if __name__ == '__main__':
    
    
    # measure time
    start_time = time.time()
    ob_replayer = OrderbookReplayer(date='20230425', symbol='ADAUSDT', begin='03:00:00', end='05:00:00')
    
    ob_replayer.get_snapshot_files()
    ob_replayer.merge_update_files()
    
    end_time = time.time()
    execution_time_ms = (end_time - start_time) * 1000
    
    print(f'Execution time: {execution_time_ms} ms')
    
