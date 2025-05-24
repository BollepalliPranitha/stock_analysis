# Scalable Stock Market Data Pipeline Project

## Project Overview
The primary goal of this project is to develop a scalable and efficient data pipeline that processes, stores, and visualizes stock market data. The aim is to empower stakeholders with actionable, data-driven insights for informed investment decisions and strategic planning.

### Features:
- Integration of **historical and real-time data** from internal business sources and stock market feeds.
- Data cleaning and transformation using **Visual ETL, AWS Glue, Lambda, and Kinesis Firehose**.
- Storage in **Snowflake** with backups in **AWS S3**.
- Real-time and historical analysis capabilities using **Tableau dashboards**.

---

### Objectives:
1. Identify **high-volume sectors** for investment opportunities.
2. Highlight **underperforming stocks** for potential divestment.
3. Track **upward market trends** for strategic growth.

---

## Key Data Model Attributes

### StockPerfKey (INT, PK)
- **Purpose**: Unique primary key for the `fact_stock_performance` table.
- **Use**: Facilitates data aggregation and querying across multiple dimensions (e.g., date, company).
- **Relevance**: Essential for tracking individual stock performance, supporting visualizations like Adjusted Close trends (Sheet 1).

### DateKey (INT, FK)
- **Purpose**: Links to the `DateKey` in `dim_date`, representing the date of each record.
- **Use**: Enables time-based analysis like year-over-year trends (Sheet 3) or daily performance (Sheet 5).
- **Relevance**: Aligns stock data with temporal patterns, aiding seasonal or long-term insights.

### CompanyKey (INT, FK)
- **Purpose**: Links to the `CompanyKey` in `dim_company`, identifying the associated company.
- **Use**: Facilitates company-specific analysis (e.g., sector and industry performance in Sheets 2 and 4).
- **Relevance**: Supports sector-based decisions, such as targeting Consumer Cyclical or Real Estate sectors.

### ExchangeKey (INT, FK)
- **Purpose**: Links to `ExchangeKey` in `dim_exchange`, representing the stock exchange (e.g., NYSE, NASDAQ).
- **Use**: Enables exchange-specific performance analysis.
- **Relevance**: Helps identify trends within specific markets.

### SP500Key (INT, FK)
- **Purpose**: Links to the `SP500Key` in `dim_sp500`, tying stock data to the S&P 500 index value.
- **Use**: Enables performance comparison against the market benchmark (e.g., Sheet 5).
- **Relevance**: Highlights outperforming or underperforming stocks.

---

### Key Metrics:
- **AdjustedClose (FLOAT)**: Adjusted closing price, reflecting the stock's true economic value. Central to identifying trends and opportunities.
- **Close, High, Low, Open (FLOAT)**: Core trading data, aiding in daily volatility and trading analysis.
- **Volume (FLOAT)**: Indicates market activity and liquidity, supporting sector analysis.
- **DailyReturn (FLOAT)**: Measures short-term performance and volatility.
- **MA5, MA20, MA50 (FLOAT)**: Moving averages for short, medium, and long-term trend analysis.
- **MarketCap (FLOAT)**: Company size indicator, aiding decisions in large-cap investments.
- **EBITDA, RevenueGrowth (FLOAT)**: Metrics reflecting financial health and growth potential.
- **VolumeSpike (FLOAT)**: Signals unusual trading activity.

---

## Business Insights

1. **Stock Price Trends (Sheet 1 & 5)**:
   - Upward trends in Adjusted Close and S&P 500 values indicate a growing market.
   - Moving averages (MA5, MA20) highlight short and long-term stability, guiding growth-focused strategies.

2. **Sector Volume Analysis (Sheet 2)**:
   - High volumes in sectors like Consumer Cyclical and Real Estate suggest strong activity, presenting investment opportunities or risks.

3. **Declining Stock Performance (Sheet 3)**:
   - Underperforming stocks with declining Adjusted Close values signal potential divestment areas.

4. **Industry Financial Health (Sheet 4)**:
   - Diverse EBITDA and RevenueGrowth metrics highlight strong performers (e.g., Insurance, Oil & Gas) and areas needing optimization.

5. **Volume Trends (Sheet 6)**:
   - Declining trading volumes suggest reduced market activity, emphasizing the need to explore new segments or products.

---

## Technologies Used
- **Visual ETL, AWS Glue, Lambda, Kinesis Firehose**: Data cleaning and transformation.
- **Snowflake & AWS S3**: Data storage and backup.
- **Tableau**: Real-time and historical data visualization.


https://prezi.com/view/710EWZdWDcTY5TZpRSMD/