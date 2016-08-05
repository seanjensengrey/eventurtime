import sys
from . import eventurtime

# todo use click

if __name__ == "__main__":
    database = sys.argv[sys.argv.index("--database") + 1]
    port = int(sys.argv[sys.argv.index("--port") + 1])
    eventurtime.main(database,port)
