import sqlite3
import datetime
import requests
from bs4 import BeautifulSoup


# Step 1: Create the database and table.
# Get the current month and year
today = datetime.date.today()
month = str(today.month).zfill(2)  # pad with leading zeros if needed
year = str(today.year)

# Create the table name
table_name = f"reviews{month}{year}"

# Connect to the database
conn = sqlite3.connect('database.db')

# Create a cursor
cursor = conn.cursor()

# Create the table
cursor.execute(f'''CREATE TABLE {table_name} (SID INTEGER PRIMARY KEY,
                                         Product TEXT,
                                         User TEXT,
                                         Date TEXT,
                                         Message TEXT,
                                         Sentiment REAL)''')

# Commit the changes
conn.commit()

# Close the connection
conn.close()


# Step 2: Scrape the product reviews, usernames, and dates.



def scrape_reviews(urlbase,numpages):
    # Initialize lists to store the scraped data
    usernames = []
    dates = []
    review_content = []

    # Set the headers for the HTTP request
    headers = {
        'User-Agent': 'Mozilla / 5.0(Windows NT10.0; Win64;x64) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 108.0.0.0 Safari / 537.36'}

    # Iterate over the pages
    for i in range(1, numpages + 1):
        # Construct the URL for the current page
        url = urlbase + str(i)

        # Send the HTTP request and get the page content
        page = requests.get(url, headers=headers)
        bs = BeautifulSoup(page.content, "html.parser")

        # Extract the usernames from the page
        names = bs.find_all("span", class_="a-profile-name")
        for i in range(0, len(names)):
            usernames.append(names[i].get_text())

        # Extract the dates from the page and format them
        raw_date = bs.find_all("span", class_="review-date")
        for i in range(0, len(raw_date)):
            dates.append(raw_date[i].get_text())

        formatted_date_list = []
        for date in dates:
            date_string = date.split("on")[1]
            date_object = datetime.datetime.strptime(date_string, ' %d %B %Y')
            formatted_date = date_object.strftime('%d/%m/%Y')
            formatted_date_list.append(formatted_date)

        # Extract the review content from the page
        review = bs.find_all("span", {"data-hook": "review-body"})
        for i in range(0, len(review)):
            review_content.append(review[i].get_text())
        review_content[:] = [reviews.lstrip('\n') for reviews in review_content]
        review_content[:] = [reviews.rstrip('\n') for reviews in review_content]
        # Remove the first two elements from each list (they are not actual reviews)
        
    # Slice the lists to the same length as the smallest list
    smallest_list = len(review_content)
    usernames = usernames[:smallest_list]
    formatted_date_list = formatted_date_list[:smallest_list]
    return(usernames,formatted_date_list,review_content)

# Set the base URL for the reviews page
url_base ="https://www.amazon.de/-/en/Smartwatch-Wristband-Touchscreen-Waterproof-Stopwatch/product-reviews/B081GVYB65/ref=cm_cr_getr_d_paging_btm_prev_1?ie=UTF8&reviewerType=all_reviews&pageNumber=1"
# Set the number of pages you want to scrape
num_pages = 15

Users, Dates, Messages = scrape_reviews(url_base,num_pages)

final_list = []
for i in range(len(Messages)):
    final_list.append((Users[i], Dates[i], Messages[i]))



# Step 3: Insert the product reviews, usernames, and dates into the table created in Step 1.

# Connect to the database
conn = sqlite3.connect('database.db')

# Create a cursor
mycursor = conn.cursor()

# Insert the values into the table
mycursor.executemany('''INSERT INTO ''' + table_name + ''' (User, Date, Message) VALUES (?, ?, ?)''', final_list)

# Commit the changes
conn.commit()

# Close the connection
conn.close()

