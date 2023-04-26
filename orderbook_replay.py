import os
import pandas as pd
import ujson
from misc import load_config
import sys
import time
from datetime import datetime
import sqlite3  

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
        self.end_datetime = datetime.strptime(f'{self.date} {end}', '%Y%m%d %H:%M:%S')
        
        self.config = load_config()
        #self.data_dir = self.config['data_warehouse_path'] #PRODUCTION
        self.data_dir = '../test_data_rigged'
        self.data_output_dir = self.config['data_output_path']
        
        print(f'Initializing OrderbookReplayer for {symbol} from {begin} to {end}')
    


    def _get_snapshot_files(self) -> dict:
        
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
        
    
    def _merge_update_files(self) -> str:

        update_files = os.listdir(os.path.join(self.data_dir, self.date, self.symbol, 'orderbook_updates'))
        
        update_files_sorted = sorted(update_files, key=lambda x: x[8:14])

        update_files_list = []
        
        for update_file in update_files_sorted:
            
            update_file_path = os.path.join(self.data_dir, self.date, self.symbol, 'orderbook_updates', update_file)
            
            update_files_list.append(update_file_path)
        
        all_updates_path = os.path.join(self.data_dir, self.date, self.symbol, 'orderbook_updates', 'all_updates.txt')
        
        with open(all_updates_path, 'w') as outfile:
            
            for filename in update_files_list:

                if filename.endswith('.txt'):

                    with open(filename, 'r') as input_file:

                        file_contents = input_file.read()

                        outfile.write(file_contents)
        
                
        print(f'Merged {len(update_files_list)-1} update files for {self.symbol} on {self.date} between {self.begin} and {self.end} to all_updates.txt')

        return all_updates_path

    def replay_orderbook(self, auto: bool=True) -> None:
        '''
        auto: bool
            If True, automatic replay of all snapshots and updates between begin and end every 5 seconds
            If False, manual replay of snapshots and updates by hitting key 'n' for next state
        '''
        
        snapshot_files_dict = self._get_snapshot_files()
            
        # Sanity check to ensure correct order of snapshot files
        for i, (key, value) in enumerate(snapshot_files_dict.items()):
            if i == 0:
                continue
            
            prev_key, prev_value = list(snapshot_files_dict.items())[i-1]
            
            if value < prev_value:
                print(f"Alert: {key} has a smaller value ({value}) than {prev_key} ({prev_value}). Order replay not possible, chronological order of snapshot files is not correct.")
                
            else:
                print("Chronological order of snapshot files is correct.")

        depth_updates_path = self._merge_update_files()
        depth_updates = pd.read_json(depth_updates_path, lines=True).drop(columns=['e', 's'])


    #6 loop over snapshot-list, starting to read first available snapshot file, updating with updates until u of next snapshot file is reached

        # https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream
        # 1. Drop any event where u is <= lastUpdateId in the snapshot.
        # 2. The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
        # 3. While listening to the stream, each new event's U should be equal to the previous event's u+1.
        # 4. The data in each event is the absolute quantity for a price level.
        # 5. If the quantity is 0, remove the price level.
        # 6. Receiving an event that removes a price level that is not in your local order book can happen and is normal.

        if auto:
            
            for key, value in snapshot_files_dict.items():
                    
                print(f'Loading: lastUpdateId: {key} // snapshot file {value}.')
                
                conn = sqlite3.connect(f'./orderbooks/orderbook_{key}.db')
                c = conn.cursor()
                columns = ['lastUpdateId', 'price', 'quantity', 'side']
                
                c.execute('''DROP TABLE IF EXISTS snapshots''')
                c.execute('''CREATE TABLE snapshots
                                    (id INTEGER PRIMARY KEY AUTOINCREMENT, eventTime INT, firstUpdateId INT, price TEXT, quantity TEXT, side TEXT)''')

                conn.commit()
            
                with open(value, 'r') as f:
                    
                    snapshot = ujson.load(f)

                    bids = pd.DataFrame(snapshot['bids'], columns=['price', 'quantity'], dtype=float)
                    bids['side'] = 'bid'
                    asks = pd.DataFrame(snapshot['asks'], columns=['price', 'quantity'], dtype=float)
                    asks['side'] = 'ask'
                    
                    ob_snapshot_t = pd.concat([bids, asks], axis=0).sort_values(by='price', ascending=False)
                    
                    len_depth_updates_before_pruning = len(depth_updates)
                    last_update_id_snapshot = key

                    
                    # 1. drop any event where u is <= lastUpdateId in the snapshot
                    depth_updates = depth_updates[depth_updates['u'] > last_update_id_snapshot] # ENSURES THAT LOOP IS GETTING QUICKER OVER TIME
                    first_final_update_id_depth_update = depth_updates['u'].iloc[0]
                    
                    print(f'lastUpdateId of snapshot: {last_update_id_snapshot}\n"First" final update ID of depth_updates: {first_final_update_id_depth_update}')
                    print(f'Length of depth_updates before pruning: {len_depth_updates_before_pruning}\nLength of depth_updates after pruning: {len(depth_updates)}')

                    if key != list(snapshot_files_dict.keys())[-1]: #check if last key
                        
                        # loop over depth_updates, beginning with first update after lastUpdateId of snapshot
                        for i, row in depth_updates.iterrows():
                            
                            # START RE-CREATING ORDERBOOK AT TIME T
                            
                            updated_bids = pd.DataFrame(row['b'], columns=['price', 'quantity'], dtype=float)
                            updated_bids['side'] = 'bid'
                            updated_asks = pd.DataFrame(row['a'], columns=['price', 'quantity'], dtype=float)
                            updated_asks['side'] = 'ask'
                            
                            ob_update_t = pd.concat([updated_bids, updated_asks], axis=0).sort_values(by='price', ascending=False)
                            
                            # Combine ob_snapshot_t and ob_update_t 
                            # IMPLEMENTATION LOGIC #
                            # If price level exists in both, replace quantity with quantity of ob_updated_t
                            # If price level only exists in ob_update_t, add it to ob_snapshot_t
                            # If price level only exists in ob_snapshot_t, keep it
                            # If price is 0, remove it from ob_snapshot_t
                            
                            def _loop_over_updates(ob_snapshot_t: pd.DataFrame, up_update_t: pd.DataFrame) -> pd.DataFrame:
                                
                                for _, r in ob_update_t.iterrows():
                                    
                                    first_update_id_row = row['U']
                                    event_time = row['E']
                                    
                                    # 1. If price level exists in both, replace quantity with quantity of ob_updated_t
                                    if r['price'] in ob_snapshot_t['price'].values:
                                        ob_snapshot_t.loc[ob_snapshot_t['price'] == r['price'], 'quantity'] = r['quantity']
                                        
                                    # 2. If price level only exists in ob_update_t, add it to ob_snapshot_t
                                    elif r['price'] not in ob_snapshot_t['price'].values:

                                        ob_snapshot_t = pd.concat([ob_snapshot_t, r.to_frame().T], axis=0, ignore_index=True)
                                        ob_snapshot_t = ob_snapshot_t.sort_values(by='price', ascending=False)
                                        print(f'Added price level {r["price"]} to orderbook.')
                                    
                                    # 3. If price is 0, remove it from ob_snapshot_t
                                    if r['quantity'] == 0:
                                        ob_snapshot_t = ob_snapshot_t[ob_snapshot_t['price'] != r['price']]
                                        print(f'Removed price level {r["price"]} from orderbook.')
                                        
                                    
                                return ob_snapshot_t, event_time, first_update_id_row
                            
                            ob_snapshot_t, event_time, first_update_id_row = _loop_over_updates(ob_snapshot_t, ob_update_t) 
                            
                            # Data preparation for SQL
                            prices_list = ob_snapshot_t['price'].tolist()
                            quantity_list = ob_snapshot_t['quantity'].tolist()
                            side_list = ob_snapshot_t['side'].tolist()
                                                        
                            query = "INSERT INTO snapshots (eventTime, firstUpdateId, price, quantity, side) VALUES (?, ?, ?, ?, ?)"
                            c.execute(query, (int(event_time), int(first_update_id_row), str(prices_list), str(quantity_list), str(side_list)))   
                            conn.commit()


                            # break loop if U of current row is greater than u of next snapshot file (key+i)
                            if row['U'] > list(snapshot_files_dict.keys())[list(snapshot_files_dict.keys()).index(key)+1]:
                                print(f'First update ID in event (U={row["U"]}) of current row is greater than u of next snapshot file (key+i={list(snapshot_files_dict.keys())[list(snapshot_files_dict.keys()).index(key)+1]}). Break loop. Going to next snapshot file.')
                                
                                conn.close()
                                break
                            
                    else: # if last key, loop over all remaining depth_updates until specified end, since no more up-to-date snapshot files are available
                        
                        for i, row in depth_updates.iterrows():
                                                        
                            updated_bids = pd.DataFrame(row['b'], columns=['price', 'quantity'], dtype=float)
                            updated_bids['side'] = 'bid'
                            updated_asks = pd.DataFrame(row['a'], columns=['price', 'quantity'], dtype=float)
                            updated_asks['side'] = 'ask'
                            
                            ob_update_t = pd.concat([updated_bids, updated_asks], axis=0).sort_values(by='price', ascending=False)
                            
                            ob_snapshot_t, event_time, first_update_id_row = _loop_over_updates(ob_snapshot_t, ob_update_t) 
                            
                            # Data preparation for SQL
                            prices_list = ob_snapshot_t['price'].tolist()
                            quantity_list = ob_snapshot_t['quantity'].tolist()
                            side_list = ob_snapshot_t['side'].tolist()
                                                        
                            query = "INSERT INTO snapshots (eventTime, firstUpdateId, price, quantity, side) VALUES (?, ?, ?, ?, ?)"
                            c.execute(query, (int(event_time), int(first_update_id_row), str(prices_list), str(quantity_list), str(side_list)))   
                            conn.commit()

                            
                            # End loop if event_time is greater than specified end time
                            event_time = row['E']
                            event_time_formatted = datetime.fromtimestamp(int(event_time)/1000)
                            
                            if event_time_formatted > self.end_datetime:
                                print(f'Event time {event_time_formatted} is greater than specified end time {self.end_datetime}. Break loop.')
                                break

 

    
    
if __name__ == '__main__':
    
    
    # measure time
    start_time = time.time()
    ob_replayer = OrderbookReplayer(date='20230425', symbol='BTCUSDT', begin='12:00:00', end='13:00:00')
    
    ob_replayer.replay_orderbook(auto=True)
    
    end_time = time.time()
    execution_time_ms = (end_time - start_time) * 1000
    
    print(f'Execution time: {execution_time_ms} ms')
    
