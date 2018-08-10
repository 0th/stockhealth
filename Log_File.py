import datetime

fileName = "Log\LogFile_"
now = datetime.datetime.now()
nowDate = now.strftime('%Y-%m-%d')
fileName = fileName + nowDate + ".log"
# f = open(fileName, 'a')  # 파일 열기

def LogPrint(*args):  # known special case of print
    f = open(fileName, 'a')  # 파일 열기
    print(*args, file=f)  # 파일 저장하기
    f.close()
    print(*args)
    pass
