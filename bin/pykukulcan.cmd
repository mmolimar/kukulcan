@echo off

set ERROR_CODE=0

call "%~dp0find-kukulcan-home.cmd"

if "%KUKULCAN_PYTHON%" == "" set KUKULCAN_PYTHON=python
set /P KUKULCAN_CLASSPATH=<%TMP%\KUKULCAN_CLASSPATH

%KUKULCAN_PYTHON% -i -m pykukulcan.repl --classpath "%KUKULCAN_CLASSPATH%"

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
