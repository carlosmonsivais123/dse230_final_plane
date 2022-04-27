import opendatasets as od

class Download_Data:
    def download_kaggle_data(self):
        dataset_url = "https://www.kaggle.com/datasets/usdot/flight-delays?select=flights.csv"
        od.download("{}".format(dataset_url))

download_data = Download_Data()
download_data.download_kaggle_data()