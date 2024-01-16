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
