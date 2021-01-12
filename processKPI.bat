@ECHO OFF

IF     "%~1"=="" GOTO Syntax
IF     "%~2"=="" GOTO Syntax
ECHO Initializing Batch process...

:: Use local variable
SETLOCAL

@SET /A client = %1 
@SET /A limit = %2+1


timeout /t 10

:Process
    ECHO Trying to process client %client% 
    ECHO read-from-clinux %client%
    read-from-clinux %client%
    timeout /t 5
    ECHO write-to-nest
    write-to-nest
    timeout /t 5
    @SET /a "client=%client%+1"
    ECHO Checking if there is possible to process client %client% to the limit %~2
    IF %limit% GTR %client% GOTO Process

:: Done
ENDLOCAL
GOTO:EOF

:Syntax
ECHO You need to pass 2 parameter, first is the initial Code, the second is the final code:
ECHO processKPI 1 10
ECHO Written by MVM