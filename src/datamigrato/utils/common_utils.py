import yaml
import requests
import json

class Common_utils:
    """docstring for Common_utils"""
    def __init__(self):
        pass
        
    def flatten_data(self, data, parent_key='', sep='_'):
            """
            Flatten a nested dictionary, separating nested keys with `sep`.
            """
            items = []
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(self.flatten_data(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)

    def read_creds(self, cred_file='creds'):
        try:
            with open(cred_file, 'r') as file:
                creds = yaml.safe_load(file)
        except FileNotFoundError:
            print(f"Cred file {cred_file} not found.")
        except yaml.YAMLError as e:
            print(f"An error occurred while parsing the YAML Cred file: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
        return creds

    def get_users_freeAPI(self, url):
        try:
            data = requests.get(url)
            return data.json()['data']['data']
        except:
            print("Failed to fetch data from API")
            return []