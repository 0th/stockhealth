import datetime

fileName = "LogFile_"
now = datetime.datetime.now()
nowDate = now.strftime('%Y-%m-%d')
fileName = fileName + nowDate + ".log"

def LogPrint(*args):  # known special case of print
    f = open('./Log/'+fileName, 'a')  # 파일 열기
    print(*args, file=f)  # 파일 저장하기
    f.close()
    print(*args)
    pass
