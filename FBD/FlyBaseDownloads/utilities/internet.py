#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 10:50:25 2024

@author: javicolors
"""

import socket

class Check_internet():

    def check_internet_connection(msg = True):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        try:
            s.connect(("www.google.com", 80))
            return True
        except (socket.gaierror, socket.timeout):
            if msg:
                print("No internet connection")
            return False