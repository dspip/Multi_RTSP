import datetime
import re
def datetime_to_timestamp(dt):
    return str(dt.replace(microsecond=0)).replace(" ", "_").replace(":", "-")


def get_timestamp():
    return datetime_to_timestamp(datetime.datetime.now())

# def timer(func):
#     def wrap(*args, **kwargs):
#         t1 = datetime.datetime.now()
#         res = func(*args, **kwargs)
#         t2 = datetime.datetime.now()
#         print(t2 - t1)
#         return res
#     return wrap

# @timer
# def datetime_from_timestamp(timestamp="2021-10-06_16-32-20"):
#     # year,month,day,hour,min,second = (int(x) for x in re.split("[-_]", timestamp))
#     # dt = datetime.datetime(year,month,day,hour,min,second)
#     dt = datetime.datetime(*(int(x) for x in re.split("[-_]", timestamp)))
#     ts = datetime_to_timestamp(dt)
#     print(dt)
#     print(ts)
#     datetime_from_timestamp(ts)
#     return dt

# if __name__ == "__main__":
#     datetime_from_timestamp(**{"timestamp":"2021-10-06_16-32-22"})

