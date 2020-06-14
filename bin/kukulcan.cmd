@echo off

set ERROR_CODE=0

:init
if NOT "%OS%"=="Windows_NT" goto Win9xArg
if "%@eval[2+2]" == "4" goto 4NTArgs
set KUKULCAN_OPTS=%*
goto endInit

:4NTArgs
set KUKULCAN_OPTS=%$
goto endInit

:Win9xArg
set KUKULCAN_OPTS=

:Win9xApp
if %1a==a goto endInit
set KUKULCAN_OPTS=%KUKULCAN_OPTS% %1
shift
goto Win9xApp

:endInit

call "%~dp0find-kukulcan-home.cmd"

set /P JAVA_EXEC=<%TMP%\JAVA_EXEC
set /P KUKULCAN_CLASSPATH=<%TMP%\KUKULCAN_CLASSPATH

%JAVA_EXEC% %KUKULCAN_OPTS% -cp "%KUKULCAN_CLASSPATH%" com.github.mmolimar.kukulcan.repl.KukulcanRepl

if ERRORLEVEL 1 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=1

:end
if "%OS%"=="Windows_NT" goto endNT
set JAVA_EXEC=
set KUKULCAN_OPTS=
set KUKULCAN_CLASSPATH=
goto postExec

:endNT
@endlocal

:postExec
exit /B %ERROR_CODE%
