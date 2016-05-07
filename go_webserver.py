#!/usr/bin/env python3
from webserver import WebServer

########################################
#
# NOTE: this file initializes the webserver REST API.
#
# You should also initialize the server by running go_server.py before
# running this file.
#
# For easier communication with the webserver, you should also initialize
# a WebInterface object.
#
########################################


def main():
    wb = WebServer()
    wb.run()

if __name__ == '__main__':
    main()
