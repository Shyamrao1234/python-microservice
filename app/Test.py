from dataclasses import dataclass

from pydantic import BaseModel
from src import CharvakaFunction


class FakeKafka:

    def send(self, callback):
        err = None
        msg = "Message Delivered"
        callback(err, msg)


class App:




    def delivery_report(self, err, msg):
        if err is not None:
            print("Error : ", err)
        else:
            print("Success :", msg)

    def run(self):
        kafka = FakeKafka()
        kafka.send(self.delivery_report)


app = App()
app.run()



class Domain:

    def __init__(this, name2):
        print(name2)
        this.shyam = name2


d = Domain("shyam")


# Difference between @dataclass and BaseModel in class

@dataclass(frozen=True)
class Model1:
    x: int
print(Model1(x="10")) #will not throw  error


class Model2(BaseModel):
    x:int
print(Model2(x="10"))
