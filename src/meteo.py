import requests
import pandas as pd
import glob


def download_all_data(start_year, end_year):
    """Downloading all meteo data files from start_year to end_year"""
    years = range(start_year, end_year+1)
    months = range(1, 13)
    for year in years:
        for month in months:
            date = str(year)+str(month).zfill(2)
            url = "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop."+date+".csv.gz"
            r = requests.get(url, allow_redirects=True)
            if "DOCTYPE HTML" not in str(r.content):
                open("../data/synop/"+date+".csv", 'wb').write(r.content)
                print(date)
                print(r.content)


def merge_csv(directory):
    """Merging all CSV files in directory"""
    interesting_files = glob.glob(directory+"*.csv")
    df = pd.concat((pd.read_csv(f, header=0) for f in interesting_files))
    df.to_csv("../data/merged_data.csv", index=False)


def load_from_csv(path):
    """Create a dataframe from csv file, and dropping last column (there is an unwanted ; at the end of each line)"""
    df = pd.read_csv(path, sep=";")
    df.drop(df.columns[len(df.columns)-1], axis=1, inplace=True)
    return df


if __name__ == "__main__":
    download_all_data(1990, 2020)
    folder = "../data/synop/"
    df = load_from_csv(folder+"202006.csv")
    print(df.head())
    print(df["numer_sta"].unique().tolist())
    merge_csv("../data/synop/")
