Strategies Used to Bypass Amazon Blocks:

User-Agent Rotation: I used fake-useragent to generate random, modern browser fingerprints for every request. This prevents Amazon from flagging the traffic as coming from a single script.

Headers Management: The requests mimic a real browser by including Accept-Language, Referer (spoofing Google), and Accept-Encoding.

Error Handling (Back-off): The code includes try/except blocks. If a request fails (status 503), the system logs it rather than crashing.

Targeting Overview Pages: Instead of hitting product detail pages (which triggers stricter bot detection) for every item, I extracted as much data as possible from the Category Listing pages (grid view).

Future Improvement (Proxy Rotation): In a production environment, I would integrate a residential proxy service (like BrightData or Smartproxy) into the requests.get call to rotate IP addresses, which is the only reliable way to scrape Amazon at scale.