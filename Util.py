from datetime import datetime

def log(msg):
    print("[%s] %s"%(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))