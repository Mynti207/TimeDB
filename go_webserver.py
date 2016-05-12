#!/usr/bin/env python3
from webserver import WebServer

########################################
#
# This file initializes the webserver REST API.
#
# You should also initialize the server by running go_server.py before
# running this file.
#
# For easier communication with the webserver, you should also initialize
# a WebInterface object.
#
# NOTE: the webserver can be used in conjunction with both the persistent
# and non-persistent database implementations.
#
########################################


def main():
    wb = WebServer()
    wb.run()

if __name__ == '__main__':
    main()
