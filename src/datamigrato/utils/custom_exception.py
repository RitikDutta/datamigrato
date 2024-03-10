import traceback
import sys

class CustomException(Exception):
    def __init__(self, error_message, error_details=None):
        super().__init__(error_message)
        self.error_message = error_message
        if error_details is None:
            _, _, exc_tb = sys.exc_info()
        else:
            exc_tb = error_details.__traceback__

        tb_info = traceback.extract_tb(exc_tb)
        if tb_info:
            final_tb = tb_info[-1]
            self.file_name = final_tb.filename
            self.lineno = final_tb.lineno
        else:
            self.file_name = None
            self.lineno = None
        
        # Include the type of the original exception in the custom message
        original_exception_type = type(error_details).__name__ if error_details else 'Exception'
        self.full_message = f"{original_exception_type}: {self.error_message} (File \"{self.file_name}\", line {self.lineno})"

    def __str__(self):
        return self.full_message