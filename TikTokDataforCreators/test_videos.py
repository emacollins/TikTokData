import requests
from bs4 import BeautifulSoup

username = input("Enter TikTok username: ")
cookie={}
verifyFp = cookie.get("s_v_web_id", "verify_kjf974fd_y7bupmR0_3uRm_43kF_Awde_8K95qt0GcpBk")
tt_webid = cookie.get("tt_webid", "6913027209393473025")
headers = {
            'Host': 't.tiktok.com',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0',
            'Referer': 'https://www.tiktok.com/',
            'Cookie': 'tt_webid_v2={}; tt_webid={}'.format(tt_webid, tt_webid)
        }
# Format the TikTok user URL
url = f"https://www.tiktok.com/@{username}?lang=en"

# Make a GET request to the TikTok user URL
response = requests.get(url, headers=headers)

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(response.content, "html.parser")

# Find all of the video links on the page
video_links = soup.find_all("a", class_="video-feed-item-wrapper")

# Extract the video URLs from the links
video_urls = []
for link in video_links:
    video_url = link.get("href")
    if video_url.startswith("/v/"):
        video_urls.append("https://www.tiktok.com" + video_url)

# Print the list of video URLs
print(video_urls)