#! Чтение из файла параметров и их подготовка к отправлению в формате JSON

import json
import time
import tornado.web
from tornado.httpserver import HTTPServer
from tornado.options import parse_command_line
from tornado.ioloop import IOLoop
from tornado import gen
import momoko
import threading
import csv
import ast
from datetime import date, datetime

solution_flag = 0

def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.strftime('%A, %d. %B %Y %I:%M:%S %p')
    raise TypeError ("Type %s not serializable" % type(obj))


ioloop = IOLoop.instance()
conn = momoko.Pool(dsn='dbname=mydb user=postgres host=localhost port=5432',
                                         size = 1, ioloop=ioloop)
fut = conn.connect()
ioloop.add_future(fut, lambda f: ioloop.stop())
ioloop.start()
fut.result()  # raises exception on connection error


class MyThread (threading.Thread):

    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    @gen.coroutine
    def run(self):
        if self.name == "to_db":
            ins = InsertHandler()
            yield ins.send_to_db()
        else:
            application = tornado.web.Application([
                (r'/', TutorialHandler)
            ])

            application.db = conn
            print("I am at client side")
            http_server = HTTPServer(application)
            http_server.listen(8888, 'localhost')
            ioloop.start()


class InsertHandler:
    @gen.coroutine
    def send_to_db(self):
        flag = 0
        i = 0
        '''
        query = """ 
                SELECT *
                FROM greenhouse 
                WHERE post_time >= '2018-03-23' AND post_time < '2018-03-25'
            """
        cur = yield conn.execute(query)
        newjson = dict()
        fulljson = dict()
        for line in cur:
            newjson["AirTemperature"] = line[1]
            newjson["AirHumidity"] = line[2]
            newjson["AirPress"] = line[3]
            newjson["GroundTemp"] = line[4]
            newjson["GroundGygro"] = line[5]
            newjson["Heating"] = line[6]
            newjson["Watering"] = line[7]
            newjson["Blowing"] = line[8]
            newjson["Light"] = line[9]
            fulljson[json_serial(line[10])] = newjson
        if not isinstance(fulljson, dict):
            print(type(fulljson))
        #newjson_send = json.dumps(fulljson, default=json_serial)
        print(fulljson)

        '''
        while flag == 0:
            yield gen.sleep(10)
            my_data = yield test.send_data()
            #print(my_data)

            str1 = "INSERT INTO greenhouse (tempe, hum, press, ground_tempe, watering, light) VALUES (" \
                  "{}, {}, {}, ARRAY{}, {}, {})"
            str = str1.format(20.67, 69.61, 789, my_data["SoilTemperatureLimits"], True, False)
            print("I've sent some data to db")
            yield conn.execute(str)
            # ioloop.add_future(future, lambda x: ioloop.stop())
            #ioloop.add_timeout(3, lambda x: ioloop.stop())
            # ioloop.start()
            i += 1
            if i == 5:
                flag = 1


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db


class TutorialHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        # yield gen.sleep(2)
        # my_data = yield test.send_data()
        # localtime = time.localtime()
        # print(localtime)
        # print(my_data["AirTemperatureLimits"])
        #num = yield self.db.execute('SELECT max(id) FROM greenhouse;')
        #r = num.fetchone()
        #if r[0] is None:
         #   e = 1
        #else:
         #   e = r[0] + 1
        '''
        str1 = "INSERT INTO greenhouse (tempe, hum, press, ground_tempe, watering, light) VALUES (" \
              "ARRAY{}, ARRAY{}, ARRAY{}, ARRAY{}, {}, {})"
        str = str1.format(my_data["AirTemperatureLimits"],
                             my_data["SoilTemperatureLimits"],
                             [200, 205, 201],
                             [45,45,21,35], True, False)

        yield self.db.execute(str)
        '''
        global solution_flag
        query = """ 
                 SELECT *
                 FROM greenhouse 
                 WHERE post_time >= '2018-03-23' AND post_time < '2018-03-25'
                """
        cursor = yield self.db.execute(query)
        fulljson = dict()
        for line in cursor:
            newjson = dict()
            newjson["AirTemperature"] = line[1]
            newjson["AirHumidity"] = line[2]
            newjson["AirPress"] = line[3]
            newjson["GroundTemp"] = line[4]
            newjson["GroundGygro"] = line[5]
            newjson["Heating"] = line[6]
            newjson["Watering"] = line[7]
            newjson["Blowing"] = line[8]
            newjson["Light"] = line[9]
            if solution_flag != 0:  # beta testing
                print("gotcha")
                newjson["Solution"] = solution_flag
                solution_flag = 0
            else:
                newjson["Solution"] = 0
            fulljson[json_serial(line[10])] = newjson
        # json_send = json.dumps(fulljson, default=json_serial) # тоже работает на отправку
        self.write(fulljson)
        self.write("\n")
        self.finish()


class Test:

    def __init__(self):
        self.current_light_state = 2    # показания света ( 1 - вкл, 0 - выкл, 2 - начальное)
        self.starting_hour = time.localtime().tm_hour   # когда был запущен процесс роста
        self.starting_min = time.localtime().tm_min
        self.current_day = -1   # счетчик текущего дня
        self.light_period = list()   # период освещения растения ( получаем из csv, изначально 0 )
        self.counter = 0    # счетчик изменения недели ( 1 - обновлено, 0 - не обновлено)
        self.ard_data = dict()

    @gen.coroutine
    def send_data(self):
        global solution_flag
        e = time.localtime()

        if (e.tm_hour == self.starting_hour) & \
                (e.tm_min == 1 + self.starting_min):
            self.counter = 0

        if (e.tm_hour == self.starting_hour) & \
                (e.tm_min == self.starting_min) & (self.counter != 1):
            self.current_day += 1
            self.counter = 1
            with open("plant2.csv", "r") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if self.current_day in list(range(ast.literal_eval(row["day"])[0], ast.literal_eval(row["day"])[1] + 1)):
                        self.ard_data["AirTemperatureLimits"] = ast.literal_eval(row["temp"])
                        self.ard_data["AirHumidityLimits"] = ast.literal_eval(row["hum"])
                        self.ard_data["SoilTemperatureLimits"] = ast.literal_eval(row["ground_temp"])
                        self.ard_data["SoilHumidityLimits"] = ast.literal_eval(row["hum_seed"])
                        self.ard_data["Light"] = ast.literal_eval(row["light"])
                        solution_flag = ast.literal_eval(row["solution"])
            print("I'm reading CSV :)")
            if self.ard_data["Light"] != 0:
                self.light_period = list(range(0, self.ard_data["Light"]+1))

        if (e.tm_hour in self.light_period) & (self.current_light_state != 1):
            self.ard_data["Light"] = 1
            # data_to_send = json.dumps(data)
            # print(data_to_send.encode('utf-8'))
            self.current_light_state = 1
            return self.ard_data
        elif (e.tm_hour not in self.light_period) & (self.current_light_state != 0):
            self.ard_data["Light"] = 0
            self.current_light_state = 0
            # data_to_send = json.dumps(data)
            # print(data_to_send.encode('utf-8'))
            return self.ard_data
        print("we've reached end of 'send_data' module")
        return self.ard_data


test = Test()

if __name__ == '__main__':
    # parse_command_line()
    thread1 = MyThread(1, "to_db")
    thread2 = MyThread(2, "to_client")

    # Start new Threads
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    '''
    application = tornado.web.Application([
        (r'/', TutorialHandler)
    ])

    ioloop = IOLoop.instance()

    application.db = momoko.Pool(dsn='dbname=mydb user=postgres host=localhost port=5432',
                                 size=1, ioloop=ioloop)
    # this is a one way to run ioloop in sync
    future = application.db.connect()
    ioloop.add_future(future, lambda f: ioloop.stop())
    ioloop.start()
    future.result()  # raises exception on connection error

    http_server = HTTPServer(application)
    http_server.listen(8888, 'localhost')
    ioloop.start()
    '''
