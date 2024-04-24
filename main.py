import requests
import pandas as pd
import sqlite3
import argparse
from PIL import Image
from io import BytesIO


def main():
    parser = argparse.ArgumentParser(description='Coding test Infinitas')
    parser.add_argument('comm', help='Command to be executed')
    parser.add_argument('parm1', help='First parameter')
    args = parser.parse_args()

    conn = sqlite3.connect("infinitas.db")

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    if args.comm == "import_users":
        api_url = "https://randomuser.me/api/?results=" + str(args.parm1)
        response = requests.get(api_url)
        data = response.json()
        df = pd.json_normalize(data['results'])
        df.to_sql('users', conn, if_exists='replace')
        if str(args.parm1) == "":
            reg_qty = '1'
        else:
            reg_qty = str(args.parm1)
        print(reg_qty + ' user(s) imported with success')
    elif args.comm == "export_users":
        db_df = pd.read_sql_query("SELECT * FROM users", conn)
        if str(args.parm1) == '':
            f_name = 'export_users.csv'
        else:
            f_name = str(args.parm1)
        db_df.to_csv(f_name, index=False)
        print('File generated with success:' + f_name)
    elif args.comm == "show_users":
        last_name = args.parm1
        query_add = ' where "name.last" like '
        db_df = pd.read_sql_query("SELECT * FROM users" + query_add + '\'%' + last_name + '%\'', conn)
        print('List of users that contains on the last name: ' + str(args.parm1))
        print('')
        if not db_df.empty:
            print(db_df)
        else:
            print('No records found with the pattern supplied')
    elif args.comm == "download_images":
        db_df = pd.read_sql_query("select \"picture.large\" as c1, \"name.last\" || \"_\" || \"name.first\" || \".jpg\" as c2 from users", conn)
        for index, row in db_df.iterrows():
            api_url = row['c1']
            response = requests.get(api_url)
            img = Image.open(BytesIO(response.content))
            img.save(row['c2'])
            print(api_url, row['c2'])
    else:
        print(f'Unexpected value for command parameter. Values accepted: import_users, export_users, show_users')


if __name__ == "__main__":
    main()
