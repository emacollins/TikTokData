import logging 
import datetime


def get_log_timestamp():
    date=datetime.datetime.now().strftime('%m-%d-%Y_%H:%M:%S')
    return date
