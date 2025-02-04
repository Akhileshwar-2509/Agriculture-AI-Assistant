import pandas as pd
import numpy as np
import os

# Define paths
DATA_PATH = "../data/"
OUTPUT_PATH = "../data/processed/"

# Ensure output directory exists
os.makedirs(OUTPUT_PATH, exist_ok=True)

### 1️⃣ Load Datasets ###
print("Loading datasets...")

# Load pesticides data
pesticides_df = pd.read_csv(DATA_PATH + "pesticides.csv")
pesticides_df.columns = pesticides_df.columns.str.strip()  # Remove leading/trailing spaces
pesticides_df.rename(columns={"Value": "Unit"}, inplace=True)

# Load rainfall data
rainfall_df = pd.read_csv(DATA_PATH + "rainfall.csv")
rainfall_df.rename(columns={"average_rain_fall_mm_per_year": "rainfall_mm"}, inplace=True)

# Load temperature data
temp_df = pd.read_csv(DATA_PATH + "temp.csv")
temp_df.rename(columns={"country": "Area", "year": "Year", "avg_temp": "temperature"}, inplace=True)

# Load yield data (main target variable)
yield_df = pd.read_csv(DATA_PATH + "yield_df.csv")
yield_df.drop(columns=["Unnamed: 0"], inplace=True, errors="ignore")  # Drop extra column if exists

### 2️⃣ Merge All Data ###
print("Merging datasets...")

# Merge datasets on Area and Year
merged_df = yield_df.merge(pesticides_df, on=["Area", "Year"], how="left")
merged_df = merged_df.merge(rainfall_df, on=["Area", "Year"], how="left")
merged_df = merged_df.merge(temp_df, on=["Area", "Year"], how="left")

### 3️⃣ Handle Data Issues ###
print("Handling data issues...")

# Convert numeric columns from string to numbers
numeric_cols = ["pesticides_tonnes", "rainfall_mm", "temperature"]  # Add other relevant numeric columns
for col in numeric_cols:
    merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')  # Convert, setting invalid values to NaN

# Handle missing values using .loc
merged_df.loc[:, "pesticides_tonnes"] = merged_df["pesticides_tonnes"].fillna(merged_df["pesticides_tonnes"].median())
merged_df.loc[:, "rainfall_mm"] = merged_df["rainfall_mm"].fillna(merged_df["rainfall_mm"].mean())
merged_df.loc[:, "temperature"] = merged_df["temperature"].fillna(merged_df["temperature"].mean())

### 4️⃣ Save Processed Data ###
print("Saving cleaned data...")

output_file = OUTPUT_PATH + "cleaned_data.csv"
merged_df.to_csv(output_file, index=False)

print(f"✅ Preprocessing complete! Cleaned data saved to: {output_file}")
