@echo off
set N=5
set VENV_PATH=.venv
set /a N_minus_one=%N%-1

for /l %%i in (0,1,%N_minus_one%) do (
    echo Start consumer for partition %%i
    start "Sentence_Consumer_%%i" cmd /k "call "%VENV_PATH%\Scripts\activate.bat" && python .\all_consumer.py --partition %%i --consumer-count %N% && echo Partition %%i end. && pause"
    timeout /t 1 /nobreak > nul
)
pause