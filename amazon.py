import aiohttp
from bs4 import BeautifulSoup
import asyncio
import pandas as pd
import re
import random
import urllib
import warnings
import asyncpg
import json
import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


warnings.filterwarnings("ignore")

products = []
with open("user-agents.txt", "r") as file:
    user_agents = file.readlines()
user_agents = [agent.strip() for agent in user_agents if agent.strip()]  # Remove empty lines and strip newline characters
async def scraper(session, url):
    await asyncio.sleep(2)
    response = await fetch_page(session, url)
    if response:
        soup = BeautifulSoup(response, 'html.parser')

        try:
            name = soup.select_one("#productTitle").get_text(strip=True)
        except:
            name = ""

        try:
            price_text = soup.select_one(".a-price").get_text(strip=True)
            price_match = re.search(r'\$\d+\.\d{2}', price_text)
            if price_match:
                price = price_match.group(0)
            else:
                price = ""
        except:
            price = ""
        ratings_value = 0.0
        try:
            ratings_element = soup.select_one('#acrPopover')
            ratings_text = ratings_element['title']
            ratings_value = float(ratings_text.split()[0])
        except Exception as e:
            print(f"Failed to fetch ratings: {e}")
        try:
            brand_name_element = soup.select_one("#bylineInfo")
            brand_name_text = brand_name_element.get_text(strip=True)
            if "Visit the " in brand_name_text:
                brand_name = brand_name_text.replace("Visit the ", "")
            else:
                brand_name = brand_name_text
        except:
            brand_name = ""
            
        try:
            image_url = soup.select_one("#imgTagWrapperId img")['src']
        except:
            image_url = ""
            
        asin_match = re.search(r'/dp/([A-Z0-9]{10})', response)
        asin_value = asin_match.group(1) if asin_match else None
        

    
        try:
            color_elements = soup.select("#variation_color_name .selection")
            available_colors = ', '.join([color.get_text(strip=True) for color in color_elements])
        except:
            available_colors = ""
        await asyncio.sleep(2)  
        try:
            description_elements = soup.select('.a-unordered-list.a-vertical.a-spacing-small .a-list-item, #feature-bullets .a-list-item')

            descriptions = [element.get_text(strip=True) for element in description_elements]
            description = ' '.join(descriptions)
        except Exception as e:
            print("Error extracting description:", e)
            description = ""

        await asyncio.sleep(2)  
        reviews=[]
        try:
            see_all_reviews_link = soup.find('a', class_='a-link-emphasis', href=True, text='See more reviews')
            if see_all_reviews_link:
                reviews_url = "https://www.amazon.com" + see_all_reviews_link['href']
                reviews.append(await fetch_reviews(session, reviews_url))
            else:
                raise ValueError("See all reviews link not found on the page.")
        except Exception as e:
            reviews_url = ""
        except Exception as e:
            print("Error fetching reviews:", e)
        
        cat=" "
        category_elements = soup.find_all('a', href=lambda href: href and 'gp/bestsellers' in href)
        if category_elements:
            for element in category_elements:
                category_text = element.text.strip()
                if category_text:
                    category_text = category_text
                    pattern = r'in (.+)$'
                    match = re.search(pattern, category_text)
                    if match:
                        category = match.group(1)
                        cat=category
                    else:
                        category = " "
        else:
            print("")
        rank=0
        bsr_element = soup.find(lambda tag: tag.name == 'span' or tag.name == 'th', text=re.compile(r'Best Sellers Rank', re.IGNORECASE))
        if bsr_element:
            try:
                bsr_text = bsr_element.find_next(lambda tag: tag.name == 'ul' or tag.name == 'td').text.strip()
                rank_number_match = re.search(r'#\d+', bsr_text)
                if rank_number_match:
                    rank_number = rank_number_match.group()
                    rank = rank_number
                else:
                    rank = None
            except Exception as e:
                print("Failed to extract Best Sellers Rank:", e)
        else:
            rank = None

        if isinstance(reviews, list):
            reviews = ", ".join(str(item) for item in reviews)
        else:
            reviews = str(reviews)

       
       
        products= {
            "Asin": asin_value,
            "Image URL": image_url,
            "Name": name,
            "Price": price,
            "Brand Name": brand_name,
            "Available Colors": available_colors,
            "Description": description,
            "Reviews": reviews,
            "Category":cat,
            "RANK":rank,
            "Rating":ratings_value
         }
        # print("now the data is moving to db")
        # await insert_data_to_db(products) 
        print(products) 
    else:
        print("Failed to fetch page:", url)


async def insert_data_to_db(listing_data):
    try:
        print("data has reached")
        conn = await asyncpg.connect(user='postgres',
        password='12345',
        
        host='localhost',
        port=5432)

       
        insert_query = """
        INSERT INTO AMAZON (Name,
            "IMAGE URL",
            Price,
           "Brand Name",
            Colors,
            Description,
            ASIN,
            Reviews,
             Category,
              RANK,Rating) 
        VALUES ($1, $2, $3, $4, $5, $6, $7,$8,$9,$10,$11)
        """

        img_url = listing_data["Image URL"]
        name = listing_data["Name"]
        description = listing_data["Description"]
        reviews_data = json.dumps(listing_data["Reviews"])
        asin = listing_data["Asin"]
        brand = listing_data["Brand Name"]
        avbcol = json.dumps(listing_data["Available Colors"])
        price = listing_data["Price"]
        categ= listing_data['Category']
        ran= listing_data['RANK']
        rat=listing_data['Rating']
        rate=str(rat)
        price = str(price)

        await conn.execute(insert_query,
                           name, img_url,price,brand,avbcol,description, asin,reviews_data,categ,ran,rate)
        print("Data inserted successfully!")

    except Exception as e:
        print("Error:", e)

    finally:
        await conn.close()



async def fetch_reviews(session, url):
    reviews = []
    try:
        html_content = await fetch_page(session, url)
        soup = BeautifulSoup(html_content, 'html.parser')
        review_elements = soup.select(".review-text-content")
        reviews = [review_element.get_text(strip=True) for review_element in review_elements]
    except Exception as e:
        print(f"Failed to fetch reviews: {e}")
    return reviews

async def fetch_page(session, url, retry=5):
    if retry <= 0:
        return None
    proxies = ["http://user0000:passproxy@38.154.227.167:5868",
            "http://user0000:passproxy@185.199.229.156:7492",
            "http://user0000:passproxy@185.199.228.220:7300",
            "http://user0000:passproxy@185.199.231.45:8382",
            "http://user0000:passproxy@188.74.210.207:6286",
            "http://user0000:passproxy@188.74.183.10:8279",
            "http://user0000:passproxy@188.74.210.21:6100",
            "http://user0000:passproxy@45.155.68.129:8133",
            "http://user0000:passproxy@154.95.36.199:6893",
            "http://user0000:passproxy@45.94.47.66:8110"
        ]

    proxy = random.choice(proxies)
    headers = {'User-Agent': random.choice(user_agents)}  

    try:
        async with session.get(url, headers=headers, proxy=proxy) as response:
            if response.status == 200:
                return await response.text()
            elif response.status == 503:
                print(f"Received 503 response, retrying with a new proxy. Retries left: {retry}")
                await asyncio.sleep(5)
                return await fetch_page(session, url, retry=retry - 1)
            else:
                print(f"Failed to fetch page: {response.status}")
                return None
    except Exception as e:
        print(f"Failed to fetch page: {e}")
        return None

async def scrape(session, url):
    try:
        product_data = []
        next_page_url = url
        while next_page_url:
            html_content = await fetch_page(session, next_page_url)
            if not html_content:
                print("No HTML content returned.")
                break
            soup = BeautifulSoup(html_content, 'html.parser')
            product_blocks = soup.select('.s-result-item')
            if not product_blocks:
                print("No product blocks found on the page.")
                break
            for block in product_blocks:
                product_link = block.select_one('h2 a')
                if product_link:
                    product_url = "https://www.amazon.com" + product_link['href']
                    product_data.append(product_url)
            next_page_element = soup.select_one('.s-pagination-next')
            next_page_url = "https://www.amazon.com" + next_page_element['href'] if next_page_element and 'href' in next_page_element.attrs else None
            await asyncio.sleep(2)  
        return product_data
    except Exception as e:
        print(f"Failed to scrape page: {e}")
        return []


async def main():
    i=1
    product_name = input("Enter the name of the product: ")
    encoded_product_name = urllib.parse.quote(product_name)
    url = f'https://www.amazon.com/s?k={encoded_product_name}'
    conn = await asyncpg.connect(
        user='postgres',
        password='12345',
     
        host='localhost',
        port=5432
    )

    try:
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS AMAZON(
            Name TEXT,
            "IMAGE URL" TEXT,
            Price TEXT,
           "Brand Name" TEXT,
            Colors TEXT,
            Description TEXT,
            ASIN TEXT,
            Reviews TEXT,
            Category TEXT,
            Rank TEXT,
            Rating TEXT
        )
        '''
        await conn.execute(create_table_query)
        async with aiohttp.ClientSession() as session:
            try:
                product_urls = await scrape(session, url)
            except Exception as e:
                print(e)
                return
            for product_url in product_urls:
                print("Pages Scrapped:",i)
                i += 1
                await asyncio.sleep(5)
                await scraper(session, product_url)
        df=pd.DataFrame(products)
        df.to_csv("Amazon.csv")

    except Exception as e:
        print("An error occurred:", e)

    finally:
         await conn.close()

if __name__ == "__main__":
    asyncio.run(main())