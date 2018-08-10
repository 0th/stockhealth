

from firebase import firebase

# from firebase_admin import firebase


# app = Flask(__name__)
firebase = firebase.FirebaseApplication('https://stockhealth-caa9d.firebaseio.com/', None)



class FirebaseManager:


    def __init__(self):
        pass

    def put(self,dir,key,value):
        result = firebase.put(dir,key,value)
        print(result)

    def patch(self,dir,data):
        result = firebase.patch(dir, data)
        # print(result)

    def post(self,dir,data):
        result = firebase.post(dir, data)
        print(result)

    def get(self,dir):
        result = firebase.get(dir, '')
        print(result)

    # def upload_totalDB(self):
    #
    #     count = 0
    #
    #     rows = DB_Manager.db_control().viewDBdata_all(dir_result,'STG1_180525')
    #     for row in rows:
    #
    #         count = count + 1
    #         print('회사 카운트: ',count)
    #
    #         dir = '/Stock/Evaluation/'+row[0]+'/'
    #         data = {'Name': row[0],
    #                 'Code': row[1],
    #                 'Volatility': row[2],
    #                 'Average': row[3],
    #                 'Eval1': row[4],
    #                 'Eval2': row[5],
    #                 'Eval3': row[6],
    #                 'StockReturn': row[7],
    #                 'Stability': row[8],
    #                 'Result': row[9],
    #                 'Total': row[10]
    #                 }
    #
    #         FB_Manager.FirebaseManager().patch(dir,data)
    #         print(row[0])
    #
    #     print("마무리")
    #
    # def upload_RankDB(self):
    #
    #
    #     # 제이슨 파일로 만들어서 보냄
    #
    #     count = 0
    #
    #     rows = DB_Manager.db_control().viewDBdata_all(dir_result, 'STG1_180525')
    #
    #     for row in rows:
    #         count = count + 1
    #         print('회사 카운트: ', count)
    #
    #         dir = '/Stock/Rank/'
    #         data = {
    #                  row[0]+'('+row[1]+')': row[10]
    #                 }
    #
    #         FB_Manager.FirebaseManager().patch(dir, data)
    #         print(row[0])
    #
    #     print("마무리")
    #
    # def upload_RankDB_JSON(self):
    #
    #     # 방법: 딕셔너리로 만들고 그 다음 dump를 통해서 json 문자열로 변경하면된다.
    #     # 제이슨 파일로 만들어서 보냄
    #     # json_val = json.dump(dict1)
    #
    #     count = 0
    #
    #     rows = DB_Manager.db_control().viewDBdata_all(dir_result, 'STG1_180525')
    #
    #     data_json =''
    #
    #     for row in rows:
    #         count = count + 1
    #         print('회사 카운트: ', count)
    #
    #         data = '\''+row[0]+'('+str(row[1])+')'+'\''+':' + str(row[10]) + ', '
    #         data_json = data_json + data
    #
    #         print(data_json)
    #
    #
    #     dir = '/Stock/Rank/'
    #     data_json = data_json[:-2]
    #     print(data_json)
    #
    #
    #     data = '{'+ data_json +'}'
    #     data_dic = eval(data)
    #     print('data_dic: ',data_dic)
    #     print(len(data_dic))
    #
    #     # dic 파일로 해도 전달됨
    #     # data_json = json.dumps(data_dic, ensure_ascii=False)
    #     # print(data_json)
    #     # print(type(data_json))
    #
    #     FB_Manager.FirebaseManager().patch(dir, data_dic)
    #
    # def upload_totalDB_JSON(self):
    #
    #
    #     count = 0
    #
    #     rows = DB_Manager.db_control().viewDBdata_all(dir_result, 'STG1_180525')
    #
    #     data_json = ''
    #
    #     for row in rows:
    #         count = count + 1
    #         print('회사 카운트: ', count)
    #
    #
    #         data = '\''+ row[0] +'\'' + ': ' + '{'\
    #                + '\'' + 'Name'+'\'' +':'+'\'' + row[0] +'\'' +','\
    #                + '\'' + 'Code'+ '\'' +':'+ '\'' +str(row[1]) +'\'' +','\
    #                + '\'' + 'Volatility'+'\'' + ':' + str(row[2]) +','\
    #                + '\'' + 'Average' +'\'' + ':' + str(row[3]) + ','\
    #                + '\'' + 'Eval1' + '\'' +':' + str(row[4]) + ','\
    #                + '\'' + 'Eval2' + '\'' +':' + str(row[5]) + ','\
    #                + '\'' + 'Eval3' + '\'' +':' + str(row[6]) + ','\
    #                + '\'' + 'StockReturn' +'\'' + ':' + str(row[7]) + ','\
    #                + '\'' + 'Stability' +'\'' + ':' + str(row[8]) + ','\
    #                + '\'' + 'Result' +'\'' + ':' + str(row[9]) + ','\
    #                + '\'' + 'Total' + '\'' + ':' + str(row[10]) + '}'+','
    #
    #
    #         data_json = data_json + data
    #         print(data_json)
    #
    #     data_json = data_json[:-1]
    #     print('1. ',data_json)
    #
    #     data = '{'+ data_json +'}'
    #     print('2. ',data)
    #     print('3. ',type(data))
    #
    #     data_dic = eval(data)
    #     print('4. ',type(data_dic))
    #
    #     dir = '/Stock/Evaluation/'
    #
    #     FB_Manager.FirebaseManager().patch(dir, data_dic)


# def main():

    # data = {'Name': '동화약품',
    #         'Code': '000020',
    #         'Volatility': 0.020574174389194733,
    #         'Average': -0.0014343900075778349,
    #         'Eval1': -0.0018576866593748313,
    #         'Eval2': -0.0016460383334763332,
    #         'Eval3': -0.0697179862663864,
    #         'StockReturn': 0.5,
    #         'Stability': 0.0,
    #         'Result': 0.00011475847792724855,
    #         'Total': 0
    #         }
    # dir = '/Stock/STG1/'
    #
    # key ='Gilvert'
    # value = 'Goodboy'
    #
    # fm = FirebaseManager()
    # fm.put(dir,key,value)
    # fm.patch(dir,data)
    # fm.post(dir,data)


# if __name__ == '__main__':
#     main()





# if __name__ == '__main__':
#     app.run()

    # @app.route('/')
    # def test():
    #     firebase.patch('/test2', {'gil': 'youngsu'})
    #     firebase.patch('/test3', {'jo': 'haesoo'})
    #     firebase.patch('/test4', {'eun': 'king1'})
    #     firebase.patch('/test5', {'eun': 'king2'})
    #
    #     test2()
    #     test3()
    #     return 'Firebase upload data'


# for index, member in enumerate(data["members"]):
#     if index > 0: print(", ", end="")
#     print(member, end="")
#
# for album, title in data["albums"].items():
#     print("  * %s: %s" % (album, title))