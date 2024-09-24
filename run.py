from process import *
from joblib import Parallel, delayed

if __name__ == "__main__":
    historical_csv_dir = r"E:\BaiduNetdiskDownload\data"
    years = [i for i in range(2000,2024,1)]
    for year in years:
        # create_stock_kbars_1min_db(year)
        one_year_data_to_stock_kbars_1min_db(year, historical_csv_dir)
    # years = [2000]
    # param_list = ["2000"]
    # Parallel(n_jobs=10)(delayed(one_year_data_to_clickhouse)(param) for param in param_list)
    # create_stock_1minute_bars()
    # result = []
    # for year in range(2000, 2024):
    #     result.append(check_max(str(year), r"E:\BaiduNetdiskDownload\data"))
    # pd.DataFrame(result).to_csv("result.csv", index=False)
    