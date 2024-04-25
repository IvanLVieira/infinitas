import requests
import pandas as pd
import sqlite3
from PIL import Image
from io import BytesIO
import sys


def main():
    comm = sys.argv[1]
    if len(sys.argv) < 3:
        parm1 = ''
    else:
        parm1 = sys.argv[2]

    conn = sqlite3.connect("infinitas.db")

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    if comm == "import_users":
        api_url = "https://randomuser.me/api/?results=" + str(parm1)
        response = requests.get(api_url)
        data = response.json()
        df = pd.json_normalize(data['results'])
        df.to_sql('users', conn, if_exists='replace')
        if str(parm1) == "":
            reg_qty = '1'
        else:
            reg_qty = str(parm1)
        print(reg_qty + ' user(s) imported with success')
    elif comm == "export_users":
        db_df = pd.read_sql_query("SELECT * FROM users", conn)
        if str(parm1) == '':
            f_name = 'export_users.csv'
        else:
            if len(parm1) < 4:
                parm1 = parm1 + ".csv"
            if parm1.rfind(".csv") < 0:
                f_name = parm1 + ".csv"
            else:
                f_name = str(parm1)
        db_df.to_csv(f_name, index=False)
        print('File generated with success:' + f_name)
    elif comm == "show_users":
        last_name = parm1
        query_add = ' where "name.last" like '
        db_df = pd.read_sql_query("SELECT \"name.last\" || \",\" || \"name.first\" as \"User name\" FROM users" + query_add + '\'%' + last_name + '%\'', conn)
        if str(parm1) != '':
            print('List of users that contains on the last name: ' + str(parm1))
        else:
            print('List of users that contains on the last name: <all>')
        print('')
        if not db_df.empty:
            print(db_df)
        else:
            print('No records found with the pattern supplied')
    elif comm == "download_images":
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
