import sys
import threading
import faulthandler

faulthandler.enable()

def load_app():
    print("starting import")
    import app.main
    print("finished import")

t = threading.Thread(target=load_app)
t.start()
t.join(3.0)
if t.is_alive():
    print("Import hung, dumping traceback...")
    faulthandler.dump_traceback()
    sys.exit(1)
