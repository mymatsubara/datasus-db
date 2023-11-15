from multiprocessing import Pool
import pandas as pd
import time
from dbfread import DBF

def main():
    with Pool(processes=16) as pool:
        waiting = [pool.apply_async(worker, args=(i, )) for i in range(1)]

        while len(waiting) != 0:
            still_wating = []

            for process in waiting:
                if process.ready():
                    print(process.get())
                else:
                    still_wating.append(process)

            waiting = still_wating
            time.sleep(1)

def worker(input):
    print("executing process")
    filename = r"C:\Users\Murilo\Downloads\dbf\RDSP2301.dbf"
    dbf = DBF(filename)
    df = pd.DataFrame(iter(dbf))

    return {"input": input, "dataframe": df}

if __name__ == "__main__":
    main()