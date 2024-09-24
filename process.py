import config
from preprocess import *
# http://www.postgres.cn/docs/9.4/ddl-partitioning.html

# TODO: 约束排除查询优化 SET constraint_exclusion = on; (postgresql.conf配置参数constraint_exclusion = on)
def create_stock_kbars_1min_db(year):
    # 主表结构
    create_base_table = '''
    CREATE TABLE IF NOT EXISTS stock_kbars_1min (
        trading_day date not null,
        close_time time not null,
        symbol character(9) not null,
        open decimal(8, 2) not null default -1,
        high decimal(8, 2) not null default -1,
        low decimal(8, 2) not null default -1,
        close decimal(8, 2) not null default -1,
        volume decimal(10, 2) not null default -1,
        amount decimal(16, 2) not null default -1,
        insert_time timestamp not null default now()
    ) PARTITION BY RANGE (trading_day)
    '''
    market_data_db.execute(create_base_table)
    # 构造按月分区条件
    month_partition_condition = []
    for month in range(1, 13):
        if month != 12:
            next_month = month + 1
            next_month = f"{next_month:02d}"
            month = f"{month:02d}"
            month_partition_condition.append({"month":month,"start_date":f"\'{year}-{month}-01\'", "end_date":f"\'{year}-{next_month}-01\'"})
        else:
            next_month = "01"
            month = "12"
            month_partition_condition.append({"month":month,"start_date":f"\'{year}-{month}-01\'", "end_date":f"\'{year+1}-{next_month}-01\'"})
    # 按月分区
    for condition in month_partition_condition:
        create_sub_table = f'''
            CREATE TABLE stock_kbars_1min_{year}{condition["month"]} PARTITION OF stock_kbars_1min 
                FOR VALUES FROM (DATE {condition["start_date"]}) TO (DATE {condition["end_date"]});

            CREATE INDEX stock_kbars_1min_{year}{condition["month"]}_key on stock_kbars_1min_{year}{condition["month"]} (trading_day, close_time);
        '''
        market_data_db.execute(create_sub_table)
        # logger.info(create_sub_table)
    # 创建触发器重定向插入数据到分区表
    create_insert_trigger_if = ""
    for i in range(len(month_partition_condition)):
        condition = month_partition_condition[i]
        trigger = \
        f'''( NEW.trading_day >= DATE {condition["start_date"]} AND
                NEW.trading_day < DATE {condition["end_date"]} ) THEN
                INSERT INTO stock_kbars_1min_{year}{condition["month"]} VALUES (NEW.*);
        '''
        if i == 0:
            create_insert_trigger_if += "IF " + trigger
        else:
            create_insert_trigger_if += "\tELSIF " + trigger
    create_insert_trigger = f'''
        CREATE OR REPLACE FUNCTION stock_kbars_1min_insert_trigger_{year}()
        RETURNS TRIGGER AS $$
        BEGIN
            {create_insert_trigger_if}
            ELSE
                RAISE EXCEPTION 'Date out of range.  Fix the stock_kbars_1min_insert_trigger_{year}() function!';
            END IF;
            RETURN NULL;
        END;
        $$
        LANGUAGE plpgsql;
    '''
    market_data_db.execute(create_insert_trigger)
    # logger.info(create_insert_trigger)


# 淘宝原始csv数据整理, 按年份整理为二维列表, 写入clickhouse
def one_year_data_to_stock_kbars_1min_db(year, historical_csv_dir="/mnt/e/BaiduNetdiskDownload/data"):
    year = str(year)
    logger.info(f"start collecting year {year} data")
    data_dir = os.path.join(historical_csv_dir, year + "年")
    data_files = os.listdir(data_dir)
    columns = ["trading_day", "close_time", "symbol", "open", "high", "low", "close", "volume", "amount"]
    for file in tqdm(data_files):
        exchange_code, stock_code, _ = file.split(".")
        symbol = stock_code + "." + exchange_code
        symbol_csv = pd.read_csv(os.path.join(data_dir, file), index_col=0).drop_duplicates()

        symbol_csv["timestamp"] = pd.to_datetime(symbol_csv["日期"])
        symbol_csv["trading_day"] = symbol_csv["timestamp"].dt.strftime("%Y-%m-%d")
        symbol_csv["close_time"] = symbol_csv["timestamp"].dt.strftime("%H:%M:%S")
        symbol_csv["symbol"] = symbol
        symbol_csv["open"] = symbol_csv["开盘价"]
        symbol_csv["high"] = symbol_csv["最高价"]
        symbol_csv["low"] = symbol_csv["最低价"]
        symbol_csv["close"] = symbol_csv["收盘价"]
        if "成交量（手）" in symbol_csv.columns:
            symbol_csv["volume"] = symbol_csv["成交量（手）"]
            symbol_csv["amount"] = symbol_csv["成交额（元）"]
        else:
            symbol_csv["volume"] = symbol_csv["成交量(手)"]
            symbol_csv["amount"] = symbol_csv["成交额(元)"]
        symbol_csv = symbol_csv[columns]
        symbol_csv.to_sql("stock_kbars_1min", market_data_db, if_exists="append", index=False)
    # logger.info(f"finish collecting year {year} data, save to stock_kbars_1min")

def check_max(year, historical_csv_dir="/mnt/e/BaiduNetdiskDownload/data"):
    year = str(year)
    logger.info(f"start checking year {year} data")
    result = {"year":year, "max_open":-1, "max_high":-1, "max_low":-1, "max_close":-1, "max_volume":-1, "max_amount":-1}

    data_dir = os.path.join(historical_csv_dir, year + "年")
    data_files = os.listdir(data_dir)
    columns = ["trading_day", "close_time", "symbol", "open", "high", "low", "close", "volume", "amount"]
    for file in tqdm(data_files):
        exchange_code, stock_code, _ = file.split(".")
        symbol = stock_code + "." + exchange_code
        symbol_csv = pd.read_csv(os.path.join(data_dir, file), index_col=0).drop_duplicates()

        symbol_csv["timestamp"] = pd.to_datetime(symbol_csv["日期"])
        symbol_csv["trading_day"] = symbol_csv["timestamp"].dt.strftime("%Y-%m-%d")
        symbol_csv["close_time"] = symbol_csv["timestamp"].dt.strftime("%H:%M:%S")
        symbol_csv["symbol"] = symbol
        symbol_csv["open"] = symbol_csv["开盘价"]
        symbol_csv["high"] = symbol_csv["最高价"]
        symbol_csv["low"] = symbol_csv["最低价"]
        symbol_csv["close"] = symbol_csv["收盘价"]
        if "成交量（手）" in symbol_csv.columns:
            symbol_csv["volume"] = symbol_csv["成交量（手）"]
            symbol_csv["amount"] = symbol_csv["成交额（元）"]
        else:
            symbol_csv["volume"] = symbol_csv["成交量(手)"]
            symbol_csv["amount"] = symbol_csv["成交额(元)"]
        symbol_csv = symbol_csv[columns]
        result["max_open"] = max(result["max_open"], symbol_csv["open"].max())
        result["max_high"] = max(result["max_high"], symbol_csv["high"].max())
        result["max_low"] = max(result["max_low"], symbol_csv["low"].min())
        result["max_close"] = max(result["max_close"], symbol_csv["close"].max())
        result["max_volume"] = max(result["max_volume"], symbol_csv["volume"].max())
        result["max_amount"] = max(result["max_amount"], symbol_csv["amount"].max())
    logger.info(f"finish checking year {year}")  
    return result  
