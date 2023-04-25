



# Order book replayer - Binance Exchange




1. User defines period. If no period is given, whole history is loaded
2. Take the nearest snapshot and initiliaze as numpy array
3. take all update files beginning from first update file greater than snapshot
4. When hitting a key OR by time interval, read in next line of order book updates and update numpy array


YYYYMMDD
.
..BTCUSDT
..
....orderbook_snapshots
......YYYYMMDDHHMMSS.txt
....orderbook_updates
......YYYYMMDDHHMMSS.txt
....trades
......YYYYMMDDHHMMSS.txt




    First, you need to read the order book snapshot files and create a dictionary to store the order book data. The dictionary should have three keys: 'bids', 'asks', and 'lastUpdateId'. The 'bids' key should map to a numpy array with shape (n, 2), where n is the number of bids in the order book. The first column of the array should contain the bid price and the second column should contain the bid quantity. Similarly, the 'asks' key should map to a numpy array with shape (m, 2), where m is the number of asks in the order book. The first column of the array should contain the ask price and the second column should contain the ask quantity. The 'lastUpdateId' key should map to the last update ID in the snapshot file.

    Next, you need to read the order book update files and update the order book data in the dictionary accordingly. Each line in the update file contains an update event in the format of "{side} {price} {quantity}", where 'side' is either 'bid' or 'ask', 'price' is the price of the update event, and 'quantity' is the quantity of the update event. To update the order book data in the dictionary, you need to first find the correct row in the 'bids' or 'asks' array that corresponds to the price of the update event. If the row does not exist, you need to insert a new row. Then you need to update the quantity in the row accordingly.

    Finally, you can replay the order book by iterating through the order book data in the dictionary and aggregating the bids and asks at each price level. You can then plot the aggregated bids and asks on a chart to visualize the order book.