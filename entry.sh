#!/bin/bash

python3 app.py worker -l INFO --without-web &
tail -f /dev/null
