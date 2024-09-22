# Snowpark_VaR_Demo

Snowpark allows you to write Python, Scala, or Java code to interact with Snowflake data and perform data processing inside the Snowflake database. This enables you to run computations and data transformations closer to the data, reducing the need for moving data between different systems. In this demo, we'll use Snowflake's native notebook to showcase how we can run VaR using Monte Carlo Simulation.

In this introduction, we'll cover:
1. **What is VaR?**
2. **Why Snowpark for VaR?**
3. **Key steps for implementing VaR using Snowpark**


---

### 1. What is Value at Risk (VaR)?
**Value at Risk (VaR)** is a statistical measure used to estimate the **maximum potential loss** of a portfolio over a defined period for a given confidence interval. It's a widely used risk metric in finance. For example, a 95% 1-day VaR of $1 million means that there’s a 95% chance that the portfolio will not lose more than $1 million in a day.

There are various methods to calculate VaR:
- **Historical VaR**: Uses historical data to estimate potential losses.
- **Parametric VaR**: Assumes the returns follow a normal distribution.
- **Monte Carlo VaR**: Simulates possible outcomes based on random sampling from the distribution of returns.

### 2. Why Snowpark for VaR?
Snowpark is useful for VaR because it allows you to:
- Run your risk models directly on **Snowflake** where your data resides, avoiding data movement.
- Use **Python** to perform complex operations, such as Monte Carlo simulations and VaR calculations.
- **Scale computations** seamlessly because Snowpark leverages Snowflake’s distributed architecture.

With Snowpark, you can run data transformations, simulations, and statistical calculations using familiar Python libraries (e.g., `pandas`, `numpy`, `scipy`) without having to move data out of Snowflake.

### 3. Key Steps for Implementing VaR using Snowpark

Here’s an overview of the steps to calculate VaR in Snowpark:

1. **Connect to Snowflake using Snowpark**:
   - You create a Snowpark **Session** that connects to your Snowflake account.

2. **Fetch Historical Stock Data**:
   - Pull historical stock data into Snowflake using **Snowflake tables** or external sources like **Yahoo Finance** using `yfinance` into a pandas dataframe.
   - Convert the pandas dataframe into Snowpark dataframe to persist in a Snowflake table for future use.

3. **Calculate Daily Returns**:
   - Compute the daily returns based on historical stock prices.

4. **Run Monte Carlo Simulation** (if using Monte Carlo VaR):
   - Simulate stock returns by generating random numbers based on the historical mean and volatility of the stock.

5. **Calculate VaR**:
   - Use Snowflake functions (e.g., **approx_percentile()**) to calculate the VaR from the simulated or historical returns.


### Advantages of Using Snowpark for VaR:
- **No data movement**: You don’t need to move data from Snowflake to another environment; everything runs inside Snowflake’s platform.
- **Scalable**: Snowflake’s architecture scales with your computations, allowing you to run large simulations or calculate VaR for large portfolios.
- **Python in SQL environment**: Snowpark allows you to leverage Python inside Snowflake, combining the flexibility of Python with Snowflake’s power.



### Conclusion:
Snowpark offers a seamless way to calculate VaR directly within Snowflake, leveraging Python and Snowflake’s scalable infrastructure. Whether you’re working with historical data or running Monte Carlo simulations, Snowpark can help you efficiently compute risk metrics like VaR without leaving the Snowflake environment.

### Snowflake Notebook Code
```python
!pip install yfinance
# Import python packages
import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, call_builtin

session = get_active_session()


def get_stock_data(symbol, start, end):
    stock_data = yf.download(symbol, start=start, end=end)
    stock_data['Daily Return'] = stock_data['Adj Close'].pct_change()
    stock_data = stock_data.dropna()  # Drop missing values
    return stock_data

# Fetch AAPL stock data
symbol = 'AAPL'
start_date = '2022-01-01'
end_date = '2023-12-31'
stock_data = get_stock_data(symbol, start=start_date, end=end_date)

snowpark_df = session.create_dataframe(stock_data)

snowpark_df.show(5)

# Step 3: Save the Snowpark DataFrame as a new table in Snowflake
table_name = "stock_data_table"  # Define your table name

# Save the Snowpark DataFrame as a table in the Snowflake database
snowpark_df.write.save_as_table(table_name, mode="overwrite")  # Use "append" if you want to add to an existing table


# Step 4: Calculate mean and volatility of daily returns
mean_return = stock_data['Daily Return'].mean()
volatility = stock_data['Daily Return'].std()

print(f"Mean Return: {mean_return}")
print(f"Volatility: {volatility}")

# Step 5: Monte Carlo Simulation of stock returns
num_simulations = 10000
time_horizon = 1  # 1 day


def simulate_stock_returns(num_simulations, mean_return, volatility):
    # Simulate stock returns based on historical mean and volatility
    simulated_returns = np.random.normal(loc=mean_return, scale=volatility, size=num_simulations)
    return pd.DataFrame(simulated_returns, columns=["return_value"])

# Generate simulated stock returns
simulated_returns_df = simulate_stock_returns(num_simulations, mean_return, volatility)

# Step 6: Load simulated returns to Snowflake as a temporary Snowpark DataFrame
simulated_returns_snowpark_df = session.create_dataframe(simulated_returns_df)

simulated_returns_snowpark_df.show(5)


# Step 7: Calculate Value at Risk (VaR) - at 95% confidence level using Snowpark's approx_percentile
confidence_level = 0.05
var_result = simulated_returns_snowpark_df.select(call_builtin('approx_percentile', col('"return_value"'), confidence_level).alias('VaR')).collect()

print(var_result)

var = var_result[0]['VAR']

print(f"Value at Risk (VaR) at 95% confidence level: {var}")

# Step 8: Close Snowpark session
session.close()

```

### Results
The VaR score interpretation:

For example, if your VaR scroe is -0.02978

The Value at Risk (VaR) score of -0.02978 means there's a 2.98% potential loss on your portfolio (or stock, e.g., AAPL) over the next day, with a 95% confidence level.

Here's a breakdown of what this means and how to interpret whether it's a "good" VaR score:

What This VaR Means:
Interpretation: There’s a 95% probability that the portfolio’s daily loss will not exceed 2.98% of its value. Conversely, there's a 5% chance that the loss will exceed 2.98% in one trading day.

Is This a "Good" VaR Score?
Whether this is a good VaR depends on several factors:

Risk Tolerance:

If you're a conservative investor with a low risk tolerance, a 2.98% daily loss might seem high, indicating that AAPL carries a reasonable degree of daily risk.
For aggressive investors, especially those in high-volatility markets (e.g., tech stocks), this level of risk might be acceptable.
Market Conditions:

Historical context is key. During periods of high market volatility (e.g., market crashes, economic downturns), higher VaR scores are expected. Conversely, in stable markets, you'd expect lower VaR scores.
Compare your VaR with the current market volatility (e.g., using the VIX index). If the overall market is volatile, your VaR is likely higher than usual.
Comparing with Other Assets:

It’s important to benchmark the VaR against other assets in your portfolio or similar stocks. AAPL tends to be a more stable, large-cap stock. If you're holding stocks in riskier sectors (e.g., biotech, cryptocurrencies), their VaR could be much higher.
Compare this VaR to other stocks you own or to an index like the S&P 500. If AAPL has a lower VaR than your other holdings, it might be a "safer" part of your portfolio.

Investment Horizon:

The VaR you calculated is for a 1-day time horizon. If your investment horizon is longer (e.g., 1 month, 1 year), consider calculating VaR over those periods. A 1-day VaR is primarily useful for traders or those managing daily risk exposure.
Portfolio Size and Diversification:

If AAPL is a part of a diversified portfolio, the portfolio VaR may be lower than the VaR for individual assets. Diversification generally reduces risk because losses in one stock may be offset by gains in another.
