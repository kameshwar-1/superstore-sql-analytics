import pandas as pd 
import sqlite3 
import matplotlib.pyplot as plt
#  file loading 
df = pd.read_csv("train.csv")
#cleaning column names:-
df.columns =df.columns.str.strip().str.lower().str.replace(" ", "_")#here all column has become the in the lower case even the column sales also
print(df.columns)
#Convert order_date to datetime before saving
df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

#create column DB
conn = sqlite3.connect("train1.db")
#save dataframe into sql table named "sales" 
df.to_sql("sales",conn,if_exists = "replace", index = False)

print("Data loaded into Sqlite database: train1.db (table: sales)")

query = "SELECT COUNT(*) AS rows, SUM(sales) AS revenue FROM sales;"
print(pd.read_sql(query, conn))

#Monthly revenue Trend
query ="""
SELECT
    strftime('%Y-%m', order_date) AS month,
    SUM(sales) AS monthly_revenue
FROM sales
GROUP BY month
ORDER BY month;
"""
monthly_df = pd.read_sql(query,conn)
print(monthly_df.head(10))

#every month should be treated as  datetime for plotting
monthly_df["month"] = pd.to_datetime(monthly_df["month"])

#plot
plt.figure(figsize = (12,6))
plt.plot(monthly_df["month"],monthly_df["monthly_revenue"],marker="o",color="b")
plt.title("Monthly revenue Trend",fontsize = 14)
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.grid(True)
plt.show()


#top 10 customers by spend 
query = """
select 
    customer_id,
    sum(sales) AS total_spent
from sales
group by customer_id
order by total_spent DESC
limit 10;
"""
top_customers_df = pd.read_sql(query, conn)
print(top_customers_df)
plt.figure(figsize=(10,6))
plt.bar(top_customers_df["customer_id"], top_customers_df["total_spent"], color="orange")
plt.title("Top 10 Customers by Spend")
plt.xlabel("Customer ID")
plt.ylabel("Total Revenue")
plt.xticks(rotation=45)
plt.show()

## List all columns in the table
query = "PRAGMA table_info(sales);"
print(pd.read_sql(query, conn))

#calculate total spend per customer.
#Order customers by spend.
#Compute cumulative % of revenue.
#Top 20% customers - how much revenue do they contribute?”

# PARETO Principle (20% customer = 80 revenue)
query = """
select
    customer_id,
    sum(sales) AS total_spent
from sales 
group by customer_id
order by total_spent DESC;
"""
customer_sales_df =pd.read_sql(query,conn)

#cumulative revenue%
# Get all customers sorted by spend
query = """
SELECT 
    customer_id,
    SUM(sales) AS total_spent
FROM sales
GROUP BY customer_id
ORDER BY total_spent DESC;
"""
customer_sales_df = pd.read_sql(query, conn)

# Calculate cumulative revenue %
customer_sales_df["cumulative_revenue"] = customer_sales_df["total_spent"].cumsum()
customer_sales_df["cumulative_percentage"] = 100 * customer_sales_df["cumulative_revenue"] / customer_sales_df["total_spent"].sum()

print(customer_sales_df.head(10))
plt.figure(figsize=(10,6))
plt.plot(range(1, len(customer_sales_df)+1), customer_sales_df["cumulative_percentage"], marker="*")
plt.axhline(y=80, color="r", linestyle="--", label="80% Revenue Line")
plt.title("Pareto Analysis (Customer Revenue Distribution)")
plt.xlabel("Customer Rank (sorted by spend)")
plt.ylabel("Cumulative % of Revenue")
plt.legend()
plt.show()

#  20 % costumer and their revenue
total_customer = len(customer_sales_df)
top_20_count = int(0.2*total_customer)
print("Total customer:-",total_customer)
print("top 20 customer:",top_20_count)

#take top 20 % customer
#it will extract top 20% from cutomer_sales_df unsing percentage function .head(top_20_count) 
top_20_customers = customer_sales_df.head(top_20_count)#takes who are top 20 % from   "customer_sales_df" using top_20_count in descending order
top_20_revenue = top_20_customers["total_spent"].sum()
total_revenue = customer_sales_df["total_spent"].sum()
percentage_revenue = 100 * top_20_revenue / total_revenue

print(f"Top 20% customers ({top_20_count} out of {total_customer}) contribute {percentage_revenue:.2f}% of total revenue.")

# revenue by region 
query = """
select
   region,
   sum(sales) as total_revenue
from sales
group by region
order by total_revenue DESC;
"""
region_revenue_df = pd.read_sql(query,conn)
print(region_revenue_df)
plt.figure(figsize=(8,6))
plt.bar(region_revenue_df["region"],region_revenue_df["total_revenue"], width=0.4,color ="teal")
plt.title("revenue by region")
plt.xlabel("Region")
plt.ylabel("Total revenue")
plt.show()

#Revenue by product category 
query= """
select
   category, 
   sum(sales) as total_revenue,
   count(order_id) as total_orders
from sales 
group BY category 
order by total_revenue DESC;
"""
revenue_by_product = pd.read_sql(query,conn)
print(revenue_by_product)
plt.figure(figsize=(8,6))
plt.bar(revenue_by_product["category"],revenue_by_product["total_revenue"], width=0.4,color ="teal")
plt.title("Revenue by product category")
plt.xlabel("category")
plt.ylabel("total_revenue")
plt.show()

#year-over_year growth rate 
query ="""
SELECT 
    strftime('%Y', order_date) AS year,
    SUM(sales) AS yearly_sales
FROM sales
GROUP BY year
ORDER BY year;
"""
year_growth_rate = pd.read_sql(query,conn) 
#calculate  growth rate
year_growth_rate["growth_rate"] = year_growth_rate["yearly_sales"].pct_change()*100
print(year_growth_rate)
#lets drop nan value from year 
year_growth_rate = year_growth_rate.dropna(subset = ["year"])
#convert year to string(for cleaning x axis)
year_growth_rate["year"] = year_growth_rate["year"].astype(str)
plt.figure(figsize=(8,6))
plt.plot(year_growth_rate["year"], year_growth_rate["yearly_sales"], marker="o", color="red")
plt.title("Yearly Sales Trend")
plt.xlabel("Year")
plt.ylabel("Total Sales")
plt.grid(True)
plt.show()
# knowing the max date
print(df["order_date"].max())


#churned customers(inactive for last 6 months)
query = """
SELECT 
    customer_id,
    MAX(order_date) AS last_purchase_date
FROM sales
GROUP BY customer_id
HAVING DATE(last_purchase_date) < DATE('2018-12-11', '-6 months');
"""
churned_customer = pd.read_sql(query,conn)
print(churned_customer)













