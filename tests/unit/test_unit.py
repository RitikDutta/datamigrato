import requests

def test_api_response():
    url = 'http://localhost:8080/api/v1/healthcheck'
    response = requests.get(url)
    
    # Print the status code and the returned data for debugging
    print(f"Status Code: {response.status_code}")
    print(f"Response Content: {response.json()}")

    # Assert that the status code is 200
    assert response.status_code == 200, f"Expected status code 200, but got {response.status_code}"
