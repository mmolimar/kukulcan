@echo off

if "%OS%"=="Windows_NT" @setlocal
if not "%JAVA_HOME%" == "" goto OkJHome

for /f %%j in ("java.exe") do (
  set JAVA_EXEC="%%~$PATH:j"
  goto setVars
)

:OkJHome
if exist "%JAVA_HOME%\bin\java.exe" (
 set JAVA_EXEC="%JAVA_HOME%\bin\java.exe"
 goto setVars
)

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory. 1>&2
echo JAVA_HOME = %JAVA_HOME% 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation 1>&2
echo. 1>&2
goto error

:setVars
if "%KUKULCAN_HOME%" == "" (set KUKULCAN_HOME=%~dp0..)
set KUKULCAN_LIBS_DIR=%KUKULCAN_HOME%\libs
set KUKULCAN_CLASSPATH=%KUKULCAN_LIBS_DIR%\*

echo %JAVA_EXEC%>%TMP%\JAVA_EXEC
echo %KUKULCAN_CLASSPATH%>%TMP%\KUKULCAN_CLASSPATH
