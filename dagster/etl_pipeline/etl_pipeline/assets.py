import os
from datetime import datetime
from contextlib import contextmanager
from typing import Union
import pandas as pd
from bs4 import BeautifulSoup
import requests
import yfinance as yf
from dagster import (
    asset,
    AssetIn,
    AssetOut,
    Output,
    multi_asset,
    AssetExecutionContext,
    EventMetadataEntry,
    Definitions,
    ScheduleDefinition,
)
from dagster import MetadataValue
import time
from dagster_airbyte import (
    load_assets_from_airbyte_instance, 
    AirbyteResource, 
    airbyte_sync_op, 
    airbyte_resource
)
# from pathlib import Path
# from dagster.utils import file_relative_path
# from dagster_dbt import DbtCliResource, dbt_assets, load_assets_from_dbt_project

@asset(group_name="python_s3")
def active_snp500_stocks() -> Output[pd.DataFrame]:
    wikiurl = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    response = requests.get(wikiurl)

    soup = BeautifulSoup(response.text, 'html.parser')
    snp500tbl = soup.find('table', {'class': "wikitable"}, {'id': 'constituents'})

    snp500df = pd.read_html(str(snp500tbl))[0]
    snp500df['Symbol'] = snp500df['Symbol'].str.replace('.', '-')

    return Output(
        value=snp500df,
        metadata={
            "columns count": MetadataValue.int(snp500df.shape[1]),
            "records count": MetadataValue.int(snp500df.shape[0])
        }
    )


# Asset to fetch stock price data
@asset(group_name="python_s3")
def stock_price_data(context: AssetExecutionContext, active_snp500_stocks: pd.DataFrame) -> Output[pd.DataFrame]:
    tickers = active_snp500_stocks['Symbol'].tolist()
    context.log.info(f"Starting to download data for {len(tickers)} tickers")

    price_data_list = []
    missing_tickers = []

    for i, ticker in enumerate(tickers, start=1):
        try:
            price_data = yf.download(ticker, start="2023-01-01", end="2023-12-31", interval='1d')
            if not price_data.empty:
                price_data['ticker'] = ticker
                price_data_list.append(price_data)
                context.log.info(f"Successfully downloaded data for {ticker}")
            else:
                context.log.warning(f"No data available for {ticker}")
                missing_tickers.append(ticker)
        except Exception as e:
            context.log.error(f"Error downloading data for {ticker}: {str(e)}")
            missing_tickers.append(ticker)
        
        # Log progress at every 100 tickers
        if i % 100 == 0:
            context.log.info(f"Processed {i} tickers so far...")
        
        time.sleep(1)  # Wait 1 second between requests
    context.log.info(f"Finished downloading data. Successful downloads: {len(price_data_list)}")
    if missing_tickers:
        context.log.warning(f"Missing data for tickers: {', '.join(missing_tickers)}")

    if not price_data_list:
        context.log.warning("No data was downloaded for any ticker")
        return Output(
            value=pd.DataFrame(),
            metadata={
                "tickers processed": MetadataValue.int(0),
                "total records": MetadataValue.int(0),
                "date range": MetadataValue.text("N/A"),
                "missing tickers": MetadataValue.text(', '.join(missing_tickers))
            }
        )

    combined_data = pd.concat(price_data_list, ignore_index=True)

    return Output(
        value=combined_data,
        metadata={
            "tickers processed": MetadataValue.int(len(price_data_list)),
            "total records": MetadataValue.int(len(combined_data)),
            "date range": MetadataValue.text(f"{combined_data.index.min()} to {combined_data.index.max()}"),
            "missing tickers": MetadataValue.text(', '.join(missing_tickers))
        }
    )
# Asset for quotes datas
@asset(group_name="python_s3")
def stock_quote_data(context: AssetExecutionContext, active_snp500_stocks: pd.DataFrame) -> Output[pd.DataFrame]:
    tickers = active_snp500_stocks['Symbol'].tolist()
    context.log.info(f"Starting to download quote data for {len(tickers)} tickers")

    quote_data_list = []

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            quote_data = stock.info
            quote_data['AsOfDataTime'] = datetime.now()
            quote_data_list.append(pd.DataFrame([quote_data], index=[ticker]))
            context.log.info(f"Successfully downloaded data for {ticker}")
        except Exception as e:
            context.log.error(f"Error downloading data for {ticker}: {e}")

    if not quote_data_list:
        context.log.warning("No data downloaded. Exiting function.")
        return Output(
            value=pd.DataFrame(),
            metadata={
                "tickers processed": MetadataValue.int(0),
                "total records": MetadataValue.int(0),
                "date range": MetadataValue.text("N/A")
            }
        )

    quote_data_df = pd.concat(quote_data_list)
    quote_data_df = quote_data_df.rename_axis('ticker')

    # Data Cleaning Steps
    threshold = len(quote_data_df) * 0.5
    quote_data_df = quote_data_df.dropna(axis=1, thresh=threshold)
    quote_data_df = quote_data_df.fillna(method='ffill').fillna(method='bfill')

    numeric_columns = quote_data_df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        quote_data_df[col] = pd.to_numeric(quote_data_df[col], errors='coerce')

    quote_data_df = quote_data_df[~quote_data_df.index.duplicated(keep='first')]

    if (quote_data_df['ask'] < 0).any():
        context.log.warning("Negative values found in 'ask' column")
        quote_data_df = quote_data_df[quote_data_df['ask'] >= 0]

    columns_to_keep = [
        'previousClose', 'open', 'dayLow', 'dayHigh', 'regularMarketPreviousClose', 'regularMarketOpen',
        'regularMarketDayLow', 'regularMarketDayHigh', 'dividendRate', 'dividendYield', 'payoutRatio',
        'beta', 'trailingPE', 'forwardPE', 'volume', 'regularMarketVolume', 'averageVolume', 'averageVolume10days',
        'averageDailyVolume10Day', 'bid', 'ask', 'marketCap', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh',
        'priceToSalesTrailing12Months', 'fiftyDayAverage', 'twoHundredDayAverage', 'trailingAnnualDividendRate',
        'trailingAnnualDividendYield', 'currency', 'enterpriseValue', 'profitMargins', 'floatShares', 'sharesOutstanding',
        'sharesShort', 'sharesShortPriorMonth', 'sharesPercentSharesOut', 'heldPercentInsiders', 'heldPercentInstitutions',
        'shortRatio', 'shortPercentOfFloat', 'bookValue', 'priceToBook', 'earningsQuarterlyGrowth', 'netIncomeToCommon',
        'trailingEps', 'forwardEps', 'pegRatio', 'enterpriseToRevenue', 'enterpriseToEbitda', '52WeekChange',
        'SandP52WeekChange', 'lastDividendValue', 'lastDividendDate', 'currentPrice', 'targetHighPrice',
        'targetLowPrice', 'targetMeanPrice', 'targetMedianPrice', 'recommendationMean', 'totalCash', 'totalCashPerShare',
        'ebitda', 'totalDebt', 'quickRatio', 'currentRatio', 'totalRevenue', 'debtToEquity', 'revenuePerShare',
        'returnOnAssets', 'returnOnEquity', 'freeCashflow', 'operatingCashflow', 'earningsGrowth', 'revenueGrowth',
        'grossMargins', 'ebitdaMargins', 'operatingMargins', 'trailingPegRatio', 'AsOfDataTime'
    ]

    quote_data_df = quote_data_df[columns_to_keep]

    return Output(
        value=quote_data_df,
        metadata={
            "tickers processed": MetadataValue.int(len(quote_data_list)),
            "total records": MetadataValue.int(len(quote_data_df)),
            "columns count": MetadataValue.int(len(columns_to_keep))
        }
    )

# Asset to upload S&P 500 stocks data to MinIO
@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["raw", "stock_data"],
    name="wiki_table",
    compute_kind="MINIO",
    group_name="python_s3"
)
def active_snp500_stocks_to_minio(
    context: AssetExecutionContext,
    active_snp500_stocks: pd.DataFrame
) -> Output[pd.DataFrame]:
    return Output(
        value=active_snp500_stocks,
        metadata={
            "columns count": MetadataValue.int(active_snp500_stocks.shape[1]),
            "records count": MetadataValue.int(active_snp500_stocks.shape[0])
        }
    )

# Asset to upload stock price data to MinIO
@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["raw", "stock_data"],
    name="price_to_minio",
    compute_kind="MINIO",
    group_name="python_s3"
)
def stock_price_data_to_minio(
    context: AssetExecutionContext,
    stock_price_data: pd.DataFrame
) -> Output[pd.DataFrame]:
    return Output(
        value=stock_price_data,
        metadata={
            "tickers processed": stock_price_data['ticker'].nunique() if not stock_price_data.empty else 0,
            "total records": len(stock_price_data),
            "date range": f"{stock_price_data.index.min()} to {stock_price_data.index.max()}" if not stock_price_data.empty else "N/A"
        }
    )
@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["raw", "stock_data"],
    name="quote_to_minio",
    compute_kind="MINIO",
    group_name="python_s3"
)
def stock_quote_data_to_minio(
    context: AssetExecutionContext,
    stock_quote_data: pd.DataFrame
) -> Output[pd.DataFrame]:
    return Output(
        value=stock_quote_data,
        metadata={
            "tickers processed": stock_quote_data.index.nunique() if not stock_quote_data.empty else 0,
            "total records": len(stock_quote_data),
            "columns count": MetadataValue.int(stock_quote_data.shape[1])
        }
    )

#### ----- AIRBYTE -------
#Implementing Dagster & Airbyte plugin
my_airbyte_resource = airbyte_resource.configured(
    {
        "host": "airbyte-server",
        "port": "8001",  # Lưu ý port 8001, không phải 8000
        "username": "airbyte",
        "password": "password",
    }
)
#Setting up connections
sync_minio_psql = airbyte_sync_op.configured({"connection_id": "ac22388f-4f60-4792-8de8-5d8fbdaec1bf"}, name="S3_to_Postgres")

airbyte_assets = load_assets_from_airbyte_instance(my_airbyte_resource)

#Setting up dbt resource

#### ------- dbt
# DBT directories
# DBT_PROFILES_DIR = r"D:/a/week_3/dagster_dockerize/.dbt"
# DBT_PROJECT_DIR = r"D:/a/week_3/dagster_dockerize/dbt_tranformation/stock_trans"

# # Verify directories exist
# assert os.path.exists(DBT_PROFILES_DIR), f"DBT profiles directory does not exist: {DBT_PROFILES_DIR}"
# assert os.path.exists(DBT_PROJECT_DIR), f"DBT project directory does not exist: {DBT_PROJECT_DIR}"

# # Setup DBT resource
# dbt_resource = DbtCliResource(
#     project_dir=DBT_PROJECT_DIR,
#     profiles_dir=DBT_PROFILES_DIR,
# )

# # Load DBT assets
# dbt_assets = load_assets_from_dbt_project(
#     project_dir=DBT_PROJECT_DIR,
#     profiles_dir=DBT_PROFILES_DIR,
#     key_prefix=["dbt"],
# )