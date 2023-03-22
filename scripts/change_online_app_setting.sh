#!/bin/bash

# スレッド数を変更
sed -i  -E  '/^(master|history)\.(insert|update)\.thread/s/=.*/=1/' */*online-app

# TPMを変更
sed -i -E '/^(master|history)\.(insert|update)\..*per\.min/s/=.*/=-1/' */*online-app
