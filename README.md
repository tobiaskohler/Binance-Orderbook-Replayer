



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



# On Buyer & Seller / Maker & Taker

If isBuyerMaker is true for the trade, it means that the order of whoever was on the buy side, was sitting as a bid in the orderbook for some time (so that it was making the market) and then someone came in and matched it immediately (market taker). So, that specific trade will now qualify as SELL and in UI highlight as redish. On the opposite isBuyerMaker=false trade will qualify as BUY and highlight greenish.

In a rising price scenario, trades flagged with "m=False" would still indicate buying pressure, as buyers are willing to pay the higher prices to acquire the asset. Trades flagged with "m=True" could indicate selling pressure, as makers are offering liquidity to the market by placing limit orders, and these limit orders may act as resistance to further price increases.

In a falling price scenario, trades flagged with "m=False" would indicate selling pressure, as buyers are no longer willing to pay the previous, higher prices for the asset. Trades flagged with "m=True" could indicate buying pressure, as makers may be placing limit orders that act as support, creating buying opportunities at lower prices.

As we mentioned earlier, makers usually pay a lower fee than takers. Therefore, a market maker can try to capture this fee advantage by placing limit orders on the side of the book where they are most likely to be the maker.

For example, if there is a lot of buying pressure and the majority of trades are flagged with "m=False", a market maker might place limit buy orders on the bid side of the book in the hope of being the maker on future trades.

On the other hand, if there is a lot of selling pressure and the majority of trades are flagged with "m=True", the market maker might place limit sell orders on the ask side of the book in the hope of being the maker on future trades.

In summary, the maker-taker flag can be used by market makers to place limit orders in a way that takes advantage of the fee structure and market conditions.