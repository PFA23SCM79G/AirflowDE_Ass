import pandas as pd
from common_data_loader import df1, gdp_data
import matplotlib.pyplot as plt

def calculate_top_vaccination_rates(df):
    
    top_countries = df.groupby('country')['people_vaccinated_per_hundred'].max()
    
    top_10_countries = top_countries.sort_values(ascending=False).head(10)
    
    return top_10_countries

def calculate_daily_vaccination_change(df):
    # Step 1: Convert the date column to datetime format if it's not already
    df['date'] = pd.to_datetime(df['date'])
    
    # Step 2: Group by country and date, summing daily vaccinations
    daily_vaccination = df.groupby(['country', 'date'])['daily_vaccinations'].sum().reset_index()

    # Step 3: Calculate total and mean daily vaccinations for each country
    vaccination_summary = daily_vaccination.groupby('country').agg(
        total_daily_vaccinations=('daily_vaccinations', 'sum'),
        mean_daily_vaccinations=('daily_vaccinations', 'mean'),
        max_daily_vaccinations=('daily_vaccinations', 'max'),
        min_daily_vaccinations=('daily_vaccinations', 'min')
    ).reset_index()
    
    return vaccination_summary

def compare_income_groups(vaccination_data, gdp_data):
    # Ensure 'date' column in vaccination data is in datetime format
    vaccination_data['date'] = pd.to_datetime(vaccination_data['date'])
    
    # Calculate daily vaccination rates
    vaccination_data['daily_vaccination_rate'] = vaccination_data.groupby('iso_code')['total_vaccinations'].diff().fillna(0)
    
    # Prepare GDP data: Assuming 'gdp_data' has 'country' and 'income_group' columns
    gdp_data = gdp_data[['countryiso3code', 'value']]  # Use necessary columns
    gdp_data['income_group'] = gdp_data['value'].apply(lambda x: 'High-income' if x > 10000 else 'Low-income')  # Example threshold
    
    # Merge vaccination data with GDP data
    merged_data = vaccination_data.merge(gdp_data, how='left', left_on='iso_code', right_on='countryiso3code')

    # Group by income category and calculate the mean daily vaccination rate
    income_comparison = merged_data.groupby('income_group')['daily_vaccination_rate'].mean().reset_index()

    # Plotting the comparison
    plt.figure(figsize=(8, 5))
    plt.bar(income_comparison['income_group'], income_comparison['daily_vaccination_rate'], color=['blue', 'green'])
    plt.title('Average Daily Vaccination Rate by Income Group')
    plt.xlabel('Income Group')
    plt.ylabel('Average Daily Vaccination Rate')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    
# top_10_countries = calculate_top_vaccination_rates(df1)
# print(top_10_countries)
vaccination_summary = calculate_daily_vaccination_change(df1)
# print(vaccination_summary)
#calculate_daily_vaccination_change(df1)
#compare_income_groups(df1, gdp_data)