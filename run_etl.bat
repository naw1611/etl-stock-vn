@echo off
cd /d D:\Visual Studio Code\python\etl-stock-vn
C:\Users\Admin\AppData\Local\Programs\Python\Python311\python.exe etl.py >> logs\etl.log 2>&1
echo %date% %time% >> logs\etl.log