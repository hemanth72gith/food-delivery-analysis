import pandas as pd
import sqlite3

orders = pd.read_csv("orders.csv")
users = pd.read_json("users.json")

order_counts = orders.groupby("user_id").size()
users["is_gold"] = users["user_id"].map(order_counts).fillna(0) >= 5

conn = sqlite3.connect(":memory:")
with open("restaurants.sql", "r", encoding="utf-8") as f:
    conn.executescript(f.read())

restaurants = pd.read_sql("SELECT * FROM restaurants", conn)

df = orders.merge(users, on="user_id", how="left")
df = df.merge(restaurants, on="restaurant_id", how="left")

print("Q1:", df[df["is_gold"]].groupby("city")["total_amount"].sum().idxmax())

print("Q2:", df.groupby("cuisine")["total_amount"].mean().idxmax())

print("Q3:", (df.groupby("user_id")["total_amount"].sum() > 1000).sum())

df["rating_range"] = pd.cut(
    df["rating"],
    bins=[0, 2, 3, 4, 4.5, 5],
    labels=["0–2", "2–3", "3–4", "4–4.5", "4.6–5"]
)

print("Q4:", df.groupby("rating_range")["total_amount"].sum().idxmax())

print("Q5:", df[df["is_gold"]].groupby("city")["total_amount"].mean().idxmax())

print("Q6:", restaurants.groupby("cuisine")["restaurant_id"].count().idxmin())

print("Q7:", round((df[df["is_gold"]].shape[0] / df.shape[0]) * 100, 2), "%")

rest_stats = df.groupby("name").agg(
    avg_order=("total_amount", "mean"),
    total_orders=("order_id", "count")
)

print("Q8:\n", rest_stats[rest_stats["total_orders"] < 20]
      .sort_values("avg_order", ascending=False).head(1))

print("Q9:", df.groupby(["is_gold", "cuisine"])["total_amount"].sum().idxmax())

df["order_date"] = pd.to_datetime(df["order_date"], dayfirst=True)
df["quarter"] = df["order_date"].dt.to_period("Q")

print("Q10:", df.groupby("quarter")["total_amount"].sum().idxmax())
