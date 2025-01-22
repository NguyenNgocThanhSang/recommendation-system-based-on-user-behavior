@echo off
rem Đặt đường dẫn Kafka root
set KAFKA_HOME=C:\Kafka\kafka_2.12-3.9.0

rem Khởi động Zookeeper trong Command Prompt bình thường
echo Starting Zookeeper...
start "Zookeeper" cmd /k "cd /d %KAFKA_HOME% && bin\windows\zookeeper-server-start.bat config\zookeeper.properties"
timeout /t 5 > nul

rem Khởi động Kafka Broker trong Command Prompt bình thường
echo Starting Kafka Broker...
start "Kafka Broker" cmd /k "cd /d %KAFKA_HOME% && bin\windows\kafka-server-start.bat config\server.properties"
timeout /t 5 > nul

rem Thông báo hoàn tất
echo Kafka cluster started successfully!
pause
