@ECHO OFF

IF     "%~1"=="" GOTO Syntax
IF     "%~2"=="" GOTO Syntax
ECHO Initializing Batch process...

:: Use local variable
SETLOCAL

SET /A client = %1 

timeout /t 10
ECHO node read-from-clinux 1
node read-from-clinux 1
timeout /t 30
ECHO node read-from-clinux 2
node read-from-clinux 2
timeout /t 30
ECHO node read-from-clinux 3
node read-from-clinux 3
timeout /t 30
ECHO node read-from-clinux 4
node read-from-clinux 4
timeout /t 30
ECHO node read-from-clinux 5
node read-from-clinux 5
timeout /t 30
ECHO node read-from-clinux 6
node read-from-clinux 6
timeout /t 30
ECHO node read-from-clinux 7
node read-from-clinux 7
timeout /t 30
ECHO node read-from-clinux 8
node read-from-clinux 8
timeout /t 30
ECHO node read-from-clinux 9
node read-from-clinux 9
timeout /t 30
ECHO node read-from-clinux 10
node read-from-clinux 10
timeout /t 30
ECHO node read-from-clinux 11
node read-from-clinux 11
timeout /t 30
ECHO node read-from-clinux 12
node read-from-clinux 12
timeout /t 30
ECHO node read-from-clinux 13
node read-from-clinux 13
timeout /t 30
ECHO node read-from-clinux 14
node read-from-clinux 14
timeout /t 30
ECHO node read-from-clinux 15
node read-from-clinux 15

:: Done
ENDLOCAL
GOTO:EOF

:Syntax
ECHO You neet to pass 2 parameter, first is the initial Code, the second is the final code:
ECHO readfromclinux 1 10
ECHO Written by MVM