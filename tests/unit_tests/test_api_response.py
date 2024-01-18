import requests
from requests.exceptions import RequestException

class Test:
    def __init__(self):
        self.run_test()
        
    def run_test(self):
        url = 'http://localhost:8080/api/v1/healthcheck'
        
        try:
            response = requests.get(url)
            
            # Print the status code and the returned data for debugging
            print(f"Status Code: {response.status_code}")
            print(f"Response Content: {response.json()}")

            # Assert that the status code is 200
            assert response.status_code == 200, f"Expected status code 200, but got {response.status_code}"

        except RequestException as e:
            print(f"An error occurred while trying to send a request to {url}: {e}")
            assert False, f"Request to {url} failed: {e}"
