from bs4 import BeautifulSoup
import re
import requests

base_url = "http://pdb-002.ebi.ac.uk:12000"
dep_ids = ["D_1292133558", "D_1292128033", "D_1292123393", "D_1292137346", "D_1290025479", "D_1292109328"]


def extract_window_open_url(html: str) -> str:
    # Parse the HTML
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find the input element with name="Open"
    input_element = soup.find('input', {'name': 'Open'})
    
    if input_element and 'onclick' in input_element.attrs:
        # Extract the onclick attribute value
        onclick_value = input_element['onclick']
        
        # Use regex to extract the URL inside window.open()
        match = re.search(r"window\.open\(['\"](.*?)['\"]\)", onclick_value)
        if match:
            return match.group(1)  # Return the extracted URL
    
    return None  # Return None if no URL is found


def main():
    for d in dep_ids:
        url = f"{base_url}/service/workmanager/summary"
        params = {
            "identifier": d,
            "sessionid": "19b9ba3cb207258221263b2356aa09545bb0f1b2",
            "annotator": "EBI"
        }
        response = requests.get(url, params=params, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "http://pdb-002:12000/service/workmanager/login",
        }, cookies={
            "csrftoken": "ObcA5yvJKGD5D9HejpgiW3gxDBtY9b9v8mJDAVkPSypyMFd81EDs2mECkGMUEUwA",
            "__stripe_mid": "2efdb699-deb5-4008-97c8-7b9c9a657b35fb0f10"
        })

        if response.status_code == 200:
            html_content = response.text
            url = extract_window_open_url(html_content)
            if url:
                print(f"Extracted URL for {d}: {url}")
                response2 = requests.get(f"{base_url}{url}")
                if response2.status_code == 200:
                    print(f"Opened dep successfully for {d}")
                else:
                    print(f"Failed to open dep for {d}, status code: {response2.status_code}")
            else:
                print(f"No URL found for {d}")
        else:
            print(f"Failed to retrieve data for {d}, status code: {response.status_code}")


if __name__ == "__main__":
    main()
