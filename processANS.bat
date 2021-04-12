@ECHO OFF

IF     "%~1"=="" GOTO Syntax
IF     "%~2"=="" GOTO Syntax
ECHO Initializing Batch process...

:: Use local variable
SETLOCAL

@SET "dir=%~1"
@SET "file=%~2"


timeout /t 10

:Process
    ECHO Trying to process %dir%%file% 
    PowerShell -NoProfile -Command "& {./convert-file.ps1 '%dir%' '%file%'}"
    timeout /t 5
    write-ans-to-nest
    ECHO Process finished sucessfully to %file%  	
:: Done
ENDLOCAL
GOTO:EOF

:Syntax
ECHO You neet to pass 2 parameter, the fist is the source directory and the second is the File Name :
ECHO processANS "C:\Users\carlo\Dropbox (Moura Assessoria)\(NEST) BI\Temp\" "ben201902_AP.csv"
ECHO Written by MVM