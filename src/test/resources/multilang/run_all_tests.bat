@echo off
REM Master script to run all AMQP compliance tests
REM
REM Usage:
REM   run_all_tests.bat                  Run all tests
REM   run_all_tests.bat amqp091          Run only AMQP 0-9-1 tests
REM   run_all_tests.bat amqp10           Run only AMQP 1.0 tests
REM   run_all_tests.bat python           Run only Python tests
REM   run_all_tests.bat nodejs           Run only Node.js tests
REM   run_all_tests.bat go               Run only Go tests

setlocal enabledelayedexpansion

cd /d "%~dp0"

REM Default environment variables
if not defined AMQP_HOST set AMQP_HOST=host.docker.internal
if not defined AMQP_PORT set AMQP_PORT=5672
if not defined AMQP_USER set AMQP_USER=guest
if not defined AMQP_PASS set AMQP_PASS=guest

echo ==============================================
echo AMQP Compliance Test Suite
echo ==============================================
echo Broker: %AMQP_HOST%:%AMQP_PORT%
echo User: %AMQP_USER%
echo ==============================================

set FILTER=%1
set FAILED=0

REM AMQP 0-9-1 Tests
if "%FILTER%"=="" goto run_python091
if "%FILTER%"=="amqp091" goto run_python091
if "%FILTER%"=="python" goto run_python091
goto skip_python091
:run_python091
echo.
echo ----------------------------------------------
echo Running: Python AMQP 0-9-1 (pika)
echo ----------------------------------------------
docker-compose build python-amqp091
docker-compose up --exit-code-from python-amqp091 python-amqp091
if errorlevel 1 set /a FAILED+=1
:skip_python091

if "%FILTER%"=="" goto run_nodejs091
if "%FILTER%"=="amqp091" goto run_nodejs091
if "%FILTER%"=="nodejs" goto run_nodejs091
goto skip_nodejs091
:run_nodejs091
echo.
echo ----------------------------------------------
echo Running: Node.js AMQP 0-9-1 (amqplib)
echo ----------------------------------------------
docker-compose build nodejs-amqp091
docker-compose up --exit-code-from nodejs-amqp091 nodejs-amqp091
if errorlevel 1 set /a FAILED+=1
:skip_nodejs091

if "%FILTER%"=="" goto run_go091
if "%FILTER%"=="amqp091" goto run_go091
if "%FILTER%"=="go" goto run_go091
goto skip_go091
:run_go091
echo.
echo ----------------------------------------------
echo Running: Go AMQP 0-9-1 (amqp091-go)
echo ----------------------------------------------
docker-compose build go-amqp091
docker-compose up --exit-code-from go-amqp091 go-amqp091
if errorlevel 1 set /a FAILED+=1
:skip_go091

REM AMQP 1.0 Tests
if "%FILTER%"=="" goto run_nodejs10
if "%FILTER%"=="amqp10" goto run_nodejs10
if "%FILTER%"=="nodejs" goto run_nodejs10
goto skip_nodejs10
:run_nodejs10
echo.
echo ----------------------------------------------
echo Running: Node.js AMQP 1.0 (rhea)
echo ----------------------------------------------
docker-compose build nodejs-amqp10
docker-compose up --exit-code-from nodejs-amqp10 nodejs-amqp10
if errorlevel 1 set /a FAILED+=1
:skip_nodejs10

if "%FILTER%"=="" goto run_python10
if "%FILTER%"=="amqp10" goto run_python10
if "%FILTER%"=="python" goto run_python10
goto skip_python10
:run_python10
echo.
echo ----------------------------------------------
echo Running: Python AMQP 1.0 (qpid-proton)
echo ----------------------------------------------
docker-compose build python-amqp10
docker-compose up --exit-code-from python-amqp10 python-amqp10
if errorlevel 1 set /a FAILED+=1
:skip_python10

if "%FILTER%"=="" goto run_go10
if "%FILTER%"=="amqp10" goto run_go10
if "%FILTER%"=="go" goto run_go10
goto skip_go10
:run_go10
echo.
echo ----------------------------------------------
echo Running: Go AMQP 1.0 (go-amqp)
echo ----------------------------------------------
docker-compose build go-amqp10
docker-compose up --exit-code-from go-amqp10 go-amqp10
if errorlevel 1 set /a FAILED+=1
:skip_go10

echo.
echo ==============================================
echo TEST SUITE COMPLETE
echo ==============================================

if %FAILED% GTR 0 (
    echo X %FAILED% test suite(s) failed
    exit /b 1
) else (
    echo All test suites passed
    exit /b 0
)
