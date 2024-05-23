#!/bin/bash
if [ -n "$TEXTBLOB" ] ; then apt-get update && apt-get install -y python3-pip && pip install textblob ; fi