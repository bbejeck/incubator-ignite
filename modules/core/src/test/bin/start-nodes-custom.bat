::  Licensed to the Apache Software Foundation (ASF) under one or more
::  contributor license agreements.  See the NOTICE file distributed with
::  this work for additional information regarding copyright ownership.
::  The ASF licenses this file to You under the Apache License, Version 2.0
::  (the "License"); you may not use this file except in compliance with
::  the License.  You may obtain a copy of the License at
::
::       http://www.apache.org/licenses/LICENSE-2.0
::
::  Unless required by applicable law or agreed to in writing, software
::  distributed under the License is distributed on an "AS IS" BASIS,
::  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
::  See the License for the specific language governing permissions and
::  limitations under the License.

set SCRIPT_DIR=%~dp0

if %SCRIPT_DIR:~-1,1% == \ set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%

:: -np option is mandatory, if it is not provided then we will wait for a user input,
:: as a result ggservice windows service hangs forever
call "%SCRIPT_DIR%\..\..\..\..\..\bin\ggstart.bat" -v -np modules\core\src\test\config\spring-start-nodes-attr.xml
