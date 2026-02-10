import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = (12, 6)

host = os.getenv('AIVEN_DB_HOST')
port = os.getenv('AIVEN_DB_PORT')
name = os.getenv('AIVEN_DB_NAME')
user = os.getenv('AIVEN_DB_USER')
password = os.getenv('AIVEN_DB_PASSWORD')

database_url = f"postgresql://{user}:{password}@{host}:{port}/{name}?sslmode=require"
engine = create_engine(database_url)

query = """
SELECT * FROM cleaned_listings
WHERE price_kes IS NOT NULL
"""

df = pd.read_sql(query, engine)
print(f"Total listings: {len(df)}")
print("\nData Overview:")
print(df.info())
print("\nStatistical Summary:")
print(df.describe())

print("\n=== LISTINGS BY SOURCE ===")
source_counts = df['source_site'].value_counts()
print(source_counts)

plt.figure(figsize=(10, 6))
source_counts.plot(kind='bar', color='steelblue')
plt.title('Listings by Source Website', fontsize=14, fontweight='bold')
plt.xlabel('Source Site')
plt.ylabel('Number of Listings')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('listings_by_source.png')
plt.close()

print("\n=== AVERAGE PRICE BY COUNTY (TOP 10) ===")
county_stats = df.groupby('county').agg({
    'price_kes': ['mean', 'median', 'count']
}).round(2)
county_stats.columns = ['avg_price', 'median_price', 'count']
county_stats = county_stats.sort_values('count', ascending=False).head(10)
print(county_stats)

plt.figure(figsize=(12, 6))
county_stats['avg_price'].plot(kind='barh', color='coral')
plt.title('Average Property Price by County (Top 10)', fontsize=14, fontweight='bold')
plt.xlabel('Average Price (KES)')
plt.ylabel('County')
plt.tight_layout()
plt.savefig('price_by_county.png')
plt.close()

print("\n=== PROPERTY TYPE DISTRIBUTION ===")
property_types = df['property_type'].value_counts()
print(property_types)

plt.figure(figsize=(10, 8))
plt.pie(property_types.values, labels=property_types.index, autopct='%1.1f%%', startangle=90)
plt.title('Property Type Distribution', fontsize=14, fontweight='bold')
plt.axis('equal')
plt.tight_layout()
plt.savefig('property_type_distribution.png')
plt.close()

print("\n=== PRICE DISTRIBUTION ===")
plt.figure(figsize=(12, 6))
df['price_kes'].hist(bins=50, color='teal', edgecolor='black')
plt.title('Price Distribution', fontsize=14, fontweight='bold')
plt.xlabel('Price (KES)')
plt.ylabel('Frequency')
plt.tight_layout()
plt.savefig('price_distribution.png')
plt.close()

print("\n=== AVERAGE PRICE BY NUMBER OF BEDROOMS ===")
bedroom_stats = df[df['bedrooms'].notna()].groupby('bedrooms').agg({
    'price_kes': ['mean', 'median', 'count']
}).round(2)
bedroom_stats.columns = ['avg_price', 'median_price', 'count']
bedroom_stats = bedroom_stats[bedroom_stats.index <= 6]
print(bedroom_stats)

plt.figure(figsize=(10, 6))
bedroom_stats['avg_price'].plot(kind='line', marker='o', color='purple', linewidth=2)
plt.title('Average Price by Number of Bedrooms', fontsize=14, fontweight='bold')
plt.xlabel('Number of Bedrooms')
plt.ylabel('Average Price (KES)')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('price_by_bedrooms.png')
plt.close()

print("\n=== NAIROBI NEIGHBORHOODS ANALYSIS ===")
nairobi_df = df[df['county'] == 'Nairobi']
neighborhood_stats = nairobi_df.groupby('neighborhood').agg({
    'price_kes': ['mean', 'count']
}).round(2)
neighborhood_stats.columns = ['avg_price', 'count']
neighborhood_stats = neighborhood_stats[neighborhood_stats['count'] >= 5].sort_values('avg_price', ascending=False).head(15)
print(neighborhood_stats)

plt.figure(figsize=(12, 8))
neighborhood_stats['avg_price'].plot(kind='barh', color='darkgreen')
plt.title('Average Price by Nairobi Neighborhood (Top 15, min 5 listings)', fontsize=14, fontweight='bold')
plt.xlabel('Average Price (KES)')
plt.ylabel('Neighborhood')
plt.tight_layout()
plt.savefig('nairobi_neighborhoods.png')
plt.close()

print("\n=== PRICE COMPARISON: NAIROBI VS MOMBASA ===")
comparison_df = df[df['county'].isin(['Nairobi', 'Mombasa'])]
comparison_stats = comparison_df.groupby('county')['price_kes'].agg(['mean', 'median', 'count']).round(2)
print(comparison_stats)

plt.figure(figsize=(12, 6))
comparison_df.boxplot(column='price_kes', by='county', figsize=(10, 6))
plt.title('Price Distribution: Nairobi vs Mombasa', fontsize=14, fontweight='bold')
plt.suptitle('')
plt.xlabel('County')
plt.ylabel('Price (KES)')
plt.tight_layout()
plt.savefig('nairobi_vs_mombasa.png')
plt.close()

print("\n=== KEY INSIGHTS SUMMARY ===")
insights = {
    'Total Listings': len(df),
    'Average Price (KES)': f"{df['price_kes'].mean():,.2f}",
    'Median Price (KES)': f"{df['price_kes'].median():,.2f}",
    'Most Common Property Type': df['property_type'].mode()[0] if not df['property_type'].mode().empty else 'N/A',
    'Most Listed County': df['county'].mode()[0] if not df['county'].mode().empty else 'N/A',
    'Active Sources': df['source_site'].nunique(),
}

for key, value in insights.items():
    print(f"{key}: {value}")

print("\nAll visualizations saved as PNG files!")