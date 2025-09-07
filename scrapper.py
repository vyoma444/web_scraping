import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2

#  Step 1: Fetch the HTML Content
url = "http://books.toscrape.com/"

try:
    response = requests.get(url)
    response.raise_for_status()
    html_content = response.text
    print("Successfully fetched the webpage content.")
except requests.exceptions.RequestException as e:
    print(f"Error fetching the page: {e}")
    exit()

# Step 2: Parse the HTML
soup = BeautifulSoup(html_content, 'html.parser')
print("Successfully parsed the HTML.")

#Step 3: Extract Data
books_data = []
book_articles = soup.find_all('article', class_='product_pod')

for book in book_articles:
    title = book.h3.a['title']

    price_str = book.find('p', class_='price_color').text
    price = float(price_str.replace('Â£', '').replace('£', ''))

    rating_str = book.find('p', class_='star-rating')['class'][1]
    rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
    rating = rating_map.get(rating_str, 0)

    book_info = {'title': title, 'price': price, 'rating': rating}
    books_data.append(book_info)

print("Successfully extracted data from the webpage.")

#Step 4: Store Data in a DataFrame
df = pd.DataFrame(books_data)

#Step 5: Load Data into Neon PostgreSQL
conn = None
cursor = None

try:
    conn = psycopg2.connect(
        dbname="neondb",
        user="neondb_owner",
        password="npg_3AGrzOu4htHY",
        host="ep-autumn-glade-a1ito3up-pooler.ap-southeast-1.aws.neon.tech",
        port="5432",
        sslmode="require"
    )
    cursor = conn.cursor()
    print("✅ Connected successfully to Neon PostgreSQL.")

    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw_products (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        price NUMERIC(10, 2),
        rating INT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("✅ Table 'raw_products' ready.")

    insert_query = "INSERT INTO raw_products (title, price, rating) VALUES (%s, %s, %s);"
    for _, row in df.iterrows():
        cursor.execute(insert_query, (row['title'], row['price'], row['rating']))

    conn.commit()
    print("✅ Data successfully inserted into Neon PostgreSQL.")

except (Exception, psycopg2.DatabaseError) as error:
    print(f"❌ Error: {error}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        print("🔒 Connection closed.")
