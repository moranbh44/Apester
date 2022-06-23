import logging
import re
import sys
import traceback
import pandas as pd


class Apester:
    def __init__(self):
        self.csv_df = None
        self.json_df = None
        self.csv_file: str = "Please implement me"
        self.json_file: str = "Please implement me"
        self.local_agg_csv = "Please implement me"

    @staticmethod
    def main() -> int:
        return Apester().exec_main()

    def read_and_parse_csv(self) -> pd.DataFrame():
        try:
            self.csv_df = pd.read_csv(self.csv_file, skiprows=7)
            self.csv_df.columns = ['date', 'advertiser_name', 'domain', 'revenue']

            # Remove "www.", "https://", "http://" from the domain
            self.csv_df['domain'] = [re.sub(r'http(s)?(:)?(\/\/)?|(\/\/)?(www\.)', '', str(x)) for x in
                                     self.csv_df['domain']]

            return self.csv_df

        except Exception:
            logging.error("Failed to parse csv file")
            traceback.print_exc(file=sys.stdout)
            raise

    def read_and_parse_json(self) -> pd.DataFrame():
        try:
            self.json_df = pd.read_json(self.json_file)
            # exclude Display rows from dataframe
            condition = (self.json_df["Ad Format"] == 'Video')
            self.json_df = self.json_df[condition]
            # Drop Ad Format column
            self.json_df = self.json_df.drop('Ad Format', axis=1)
            # Add Advertiser Name column with 'Rubicon' fixed value
            self.json_df.insert(1, 'Advertiser Name', 'Rubicon')
            self.json_df.columns = ['date', 'advertiser_name', 'domain', 'revenue']
            self.json_df['date'] = self.json_df['date'].dt.strftime('%d/%m/%Y')

            # Remove "www.", "https://", "http://" from the domain
            self.json_df['domain'] = [re.sub(r'http(s)?(:)?(\/\/)?|(\/\/)?(www\.)', '', str(x)) for x in
                                      self.json_df['domain']]

            return self.json_df

        except Exception:
            logging.error("Failed to parse json file")
            traceback.print_exc(file=sys.stdout)
            raise

    def merge_csv_json_df(self) -> pd.DataFrame():
        try:
            # Merge two dataframes
            res = pd.concat([self.csv_df, self.json_df])
            res['date'] = pd.to_datetime(res['date'], format='%d/%m/%Y')

            return res

        except Exception:
            logging.error("Failed to merge data files")
            traceback.print_exc(file=sys.stdout)
            raise

    def agg_data(self) -> pd.DataFrame():
        try:
            agg_data = self.merge_csv_json_df()
            df_agg = agg_data.groupby(['date', 'advertiser_name', 'domain']).agg(revenue=('revenue', 'sum'),
                                                                                 ).reset_index()
            return df_agg

        except Exception:
            logging.error("Failed to aggregation data")
            traceback.print_exc(file=sys.stdout)
            raise

    def insert_data_to_local_csv(self):
        try:
            df_agg = self.agg_data()
            df_agg.to_csv(self.local_agg_csv, index=False)

        except Exception:
            logging.error("Failed to insert data to local csv")
            traceback.print_exc(file=sys.stdout)
            raise

    def exec_main(self):
        try:
            self.read_and_parse_csv()
            self.read_and_parse_json()
            self.insert_data_to_local_csv()

        except Exception:
            logging.error("Failed to run Apester process")
            traceback.print_exc(file=sys.stdout)
            raise


if __name__ == "__main__":
    Apester.main()
