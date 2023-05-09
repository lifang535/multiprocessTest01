import multiprocessing
import time
import threading
from math import ceil

# 定义一些数据
total = 160
rps = 20
batch_size = 4
duration = 0.4
lock = multiprocessing.Lock()
req_queue = multiprocessing.Queue()


# Client 发送请求
class Client(multiprocessing.Process):
    def __init__(self, frontend_list, backend_list):
        super().__init__()
        self.frontend_list = frontend_list
        self.backend_list = backend_list

    def run(self):
        Client_Sender = threading.Thread(target=self.Sender)
        Client_Receiver = threading.Thread(target=self.Receiver)
        Client_Sender.start()
        Client_Receiver.start()

    # 发送请求
    def Sender(self):
        with lock:
            print("[Client] Sender is working")
        time_start = time.time()
        time_end = time.time()
        for i in range(total):
            time_end = time.time()
            time.sleep(1 / rps - time_end + time_start)  # 动态
            time_start = time.time()
            self.frontend_list.append(time.time())  # 向 frontend_list 发送请求处理开始的时间
        self.frontend_list.append(-1)
        with lock:
            print("[Client] all requests were sent")

    # 计算 latency
    def Receiver(self):
        latency = []
        sum_latency = 0
        with lock:
            print("[Client] Receiver is working")
        while True:
            if len(self.backend_list) >= total:
                with lock:
                    for i in range(total):
                        sum_latency += self.backend_list[i] - self.frontend_list[i]
                        latency.append(round((self.backend_list[i] - self.frontend_list[i]) * 1000))
                    print("[Client] request latency =")
                    for i in range(len(latency)):
                        print(latency[i], end=" ")
                        if (i + 1) % 20 == 0:
                            print()
                    latency.sort()
                    print("[Client] max of latency =", latency[total - 1], "ms")
                    print("[Client] avg of latency =", sum(latency) / total, "ms")
                    print("[Client] 99% quantile latency =", latency[ceil(total * 99 / 100 - 1)], "ms")  # 向上取整
                break


# Frontend 分配请求
class Frontend(multiprocessing.Process):
    def __init__(self, frontend_list, backend_list, worker_queue_1, worker_queue_2):
        super().__init__()
        self.frontend_list = frontend_list
        self.backend_list = backend_list
        self.worker_queue_1 = worker_queue_1
        self.worker_queue_2 = worker_queue_2

    def run(self):
        Frontend_Sender = threading.Thread(target=self.Sender)
        Frontend_Receiver = threading.Thread(target=self.Receiver)
        Frontend_Sender.start()
        Frontend_Receiver.start()

    # 接收 Client 发送的请求
    def Receiver(self):
        with lock:
            print("[Frontend] Receiver is working")
        count = 0
        while True:
            if len(self.frontend_list) > count:
                req_queue.put(self.frontend_list[count])
                if self.frontend_list[count] == -1:
                    with lock:
                        print("[Frontend] all requests were received")
                    break
                count += 1

            if len(self.backend_list) >= total:
                break

    # 向 Workers 分配请求
    def Sender(self):
        with lock:
            print("[Frontend] Sender is working")
        count = 0
        while True:
            if req_queue.qsize() >= batch_size:
                if count % 2 == 0:
                    for i in range(batch_size):
                        req = req_queue.get()
                        self.worker_queue_1.put(req)
                    with lock:
                        print("[Frontend] a batch was sent to the worker", 1)
                else:
                    for i in range(batch_size):
                        req = req_queue.get()
                        self.worker_queue_2.put(req)
                    with lock:
                        print("[Frontend] a batch was sent to the worker", 2)
                count += 1

            if len(self.backend_list) >= total:
                break


# Worker_1 处理请求
class Worker_1(multiprocessing.Process):
    def __init__(self, frontend_list, backend_list, worker_queue_1):
        super().__init__()
        self.frontend_list = frontend_list
        self.backend_list = backend_list
        self.worker_queue_1 = worker_queue_1

    def run(self):
        self.Receiver()

    # 接收并处理请求
    def Receiver(self):
        time_start = time.time()
        time_end = time.time()
        while True:
            if self.worker_queue_1.qsize() >= batch_size:
                time.sleep(duration - time_end + time_start)  # 动态
                time_start = time.time()
                with lock:
                    print("[Worker_1] a batch is processed | Qsize =", self.worker_queue_1.qsize())
                for i in range(batch_size):
                    self.worker_queue_1.get()
                    self.backend_list.append(time.time())  # def Sender 向 backend_list 发送请求处理结束的时间
                time_end = time.time()

            if len(self.backend_list) >= total:
                break


# Worker_2 处理请求
class Worker_2(multiprocessing.Process):
    def __init__(self, frontend_list, backend_list, worker_queue_2):
        super().__init__()
        self.frontend_list = frontend_list
        self.backend_list = backend_list
        self.worker_queue_2 = worker_queue_2

    def run(self):
        self.Receiver()

    # 接收并处理请求
    def Receiver(self):
        time_start = time.time()
        time_end = time.time()
        while True:
            if self.worker_queue_2.qsize() >= batch_size:
                time.sleep(duration - time_end + time_start)  # 动态
                time_start = time.time()
                with lock:
                    print("[Worker_2] a batch is processed | Qsize =", self.worker_queue_2.qsize())
                for i in range(batch_size):
                    self.worker_queue_2.get()
                    self.backend_list.append(time.time())  # def Sender 向 backend_list 发送请求处理结束的时间
                time_end = time.time()

            if len(self.backend_list) >= total:
                break


if __name__ == '__main__':
    manager_1 = multiprocessing.Manager()
    manager_2 = multiprocessing.Manager()
    frontend_list = manager_1.list()  # 存储请求处理开始的时间
    backend_list = manager_2.list()  # 存储请求处理结束的时间
    worker_queue_1 = manager_1.Queue()  # Worker_1 的处理队列
    worker_queue_2 = manager_2.Queue()  # Worker_2 的处理队列

    client = Client(frontend_list, backend_list)
    frontend = Frontend(frontend_list, backend_list, worker_queue_1, worker_queue_2)
    worker_1 = Worker_1(frontend_list, backend_list, worker_queue_1)
    worker_2 = Worker_2(frontend_list, backend_list, worker_queue_2)

    client.start()
    frontend.start()
    worker_1.start()
    worker_2.start()

    client.join()
    frontend.join()
    worker_1.join()
    worker_2.join()
