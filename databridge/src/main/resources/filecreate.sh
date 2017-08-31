#!/bin/bash
cd $1
file="_DONE" 
if [ -f "$file" ]
then
  rm  _DONE
fi
touch _DONE
