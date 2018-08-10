import threading


class AsyncTask:

    def __init__(self):
        pass

    def TaskA(self):
        print('Process A')
        threading.Timer(1,self.TaskA).start()

    def TaskB(self):
        print('Process B')
        threading.Timer(3, self.TaskB).start()

def main():
    print('Async Function')
    at = AsyncTask()
    at.TaskA()
    at.TaskB()

if __name__ == '__main__':
    main()


