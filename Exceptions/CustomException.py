class CustomException(Exception):
    pass

'''
    Custom Exception Class
'''
class FileNotFoundException(CustomException):
    def __init__(self,message):
        self.message = message

    def __str__(self):
        return ("File Not Found in the given directory!")

class EmptyDataFrame(CustomException):
    def __init__(self,message):
        self.message = message

    def __str__(self):
        return ("The Dataframe is empty")

class DuplicateInDataFrame(CustomException):
    def __init__(self,message):
        self.message = message

    def __str__(self):
        return ("Duplicate In Datasets")