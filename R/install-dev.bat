@echo off

rem
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem    http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem

rem Install development version of SparkR
rem

set SPARK_HOME=%~dp0..

MKDIR %SPARK_HOME%\R\lib

R.exe CMD INSTALL --library="%SPARK_HOME%\R\lib"  %SPARK_HOME%\R\pkg\

rem Zip the SparkR package so that it can be distributed to worker nodes on YARN
pushd %SPARK_HOME%\R\lib
%JAVA_HOME%\bin\jar.exe cfM "%SPARK_HOME%\R\lib\sparkr.zip" SparkR
popd

