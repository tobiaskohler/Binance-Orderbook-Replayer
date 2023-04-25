



# Order book replayer - Binance Exchange




1. User defines period. If no period is given, whole history is loaded
2. Take the nearest snapshot and initiliaze as numpy array
3. take all update files beginning from first update file greater than snapshot
4. When hitting a key OR by time interval, read in next line of order book updates and update numpy array