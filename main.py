      
import pandas as pd
import pyodbc
import os
import shutil

# server Details
server = '192.168.10.102'
database = 'NSETrade'
username = 'techteam'
password = 'avijayvis'
cnxn = pyodbc.connect('DRIVER={SQL server};server='+server +
                      ';DATABASE='+database+';UID='+username+';PWD=' + password)
cursor = cnxn.cursor()
path = r'C:\Users\mozzam\Desktop\PARLE-G_pandas\new data\Moz'

for filename in os.listdir(path):
    if filename.endswith(".CSV"):
        csv_file = os.path.join(path, filename)
        try:
            df = pd.read_csv(csv_file, low_memory=False)
            df.columns = df.columns.str.strip().str.replace(' ', '').str.lower()

            if (df.columns.tolist()[0] == 'server'):
                df = df[["ordertime", "exchangetradetime", "managerid", "exchangetradeid", "exchangeorderno", "userid", "ctclid", "memberid", "clientid", "securityid", "exchange", "optiontype",
                         "expirydate", "strikeprice", "symbol", "side", "quantity", "price", "securitytype", "maintradeid", "referencetext",
                         "pendingquantity"]]

                # Rename columns using a dictionary as per database name and reindex it again
                df = df.rename(columns={'ordertime': 'sqldatetime', 'exchangetradetime': 'datetime', 'exchangetradeid': 'tradenum', 'memberid': 'membercode',
                                        'clientid': 'accountcode', 'securityid': 'scripcode', 'optiontype': 'opttype', 'symbol': 'scripdescription', 'side': 'buysellflag',
                                        'quantity': 'tradeqty', 'price': 'tradeprice'})

                # make new columns
                df['smsflag'] = 0
                df['ordernum'] = 0
                df['sender'] = 0
                df['customid'] = 0

                # assign one column data to another column
                df = df.assign(ordernum=df['exchangeorderno'])
                df = df.assign(userid=df['managerid'])

                # Exclude column 'A'
                df = df.drop('managerid', axis=1)

            elif (df.columns.tolist()[0] == 'ManagerID'):
                pass

            elif (df.columns.tolist()[0] != 'server' and df.columns.tolist()[0] != 'sqldatetime'):
                shutil.copy(f"""C:/Users/mozzam/Desktop/PARLE-G_pandas/new data/NM01/{filename}""",
                            f"""C:/Users/mozzam/Desktop/PARLE-G_pandas/TrashFiles_new/{filename}""")

            df['terminalid'] = 0
            df['algoid'] = 0

            df.reset_index()
            df.fillna(0, inplace=True)
            file_c = 0
            count = 0
            for index, row in df.iterrows():
                try:
                    Query = f"""INSERT INTO NSETrade.dbo.[pythontrade](
                       [SQLDateTime],[DateTime]
                        ,[TradeNum],[OrderNum]
                        ,[UserID],[TerminalID]
                        ,[CTCLID],[AlgoID]
                        ,[AccountCode],[MemberCode]
                        ,[ScripCode],[Exchange]
                        ,[OptType],[ExpiryDate]
                        ,[StrikePrice],[ScripDescription]
                        ,[BuySellFlag],[TradeQty]
                        ,[TradePrice],[MainTradeID]
                        ,[SecurityType],[ReferenceText]
                        ,[PendingQty],[CustomID]
                        ,[Sender]                                  
                        ) 
                        values('{str(row.sqldatetime).strip()}', '{str(row.datetime).strip()}', '{str(row.tradenum).strip()}','{str(row.ordernum).strip()}', '{str(row.userid.strip()).strip()}', '{str(row.terminalid).strip()}', '{str(row.ctclid).strip()}',
                                    '{str(row.algoid).strip()}', '{str(row.accountcode).strip()}', '{str(row.membercode).strip()}', '{str(row.scripcode).strip()}', '{str(row.exchange).strip()}', '{str(row.opttype).strip()}', '{str(row.expirydate).strip()}',
                                    {row.strikeprice}, '{str(row.scripdescription).strip()}', '{str(row.buysellflag).strip()}', {row.tradeqty}, {row.tradeprice},
                                    {row.maintradeid}, '{str(row.securitytype).strip()}', '{str(row.referencetext).strip()}', {row.pendingquantity}, {row.customid}, '{str(row.sender).strip()}') """                    
                    cursor.execute(Query)
                    cnxn.commit()
                    count += 1

                except Exception as e:
                    print(e)

            print(f'Rows: {count}  || File  {filename}')

        except Exception as e:
            print(e, f' ***Exception *** for file {filename}')

cursor.close()
