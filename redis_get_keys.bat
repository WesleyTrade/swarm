@echo off
for /f "tokens=*" %%A in ('redis-cli -p 6380 keys kafka:message:*') do (
    echo %%A
    redis-cli -p 6380 get %%A
)
