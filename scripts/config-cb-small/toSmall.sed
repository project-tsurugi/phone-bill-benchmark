#!/usr/bin/sed -f
s/number.of.contracts.records=.*$/number.of.contracts.records=1000/
s/duplicate.phone.number.rate=.*$/duplicate.phone.number.rate=100/
s/expiration.date.rate=4.*$/expiration.date.rate=400/
s/no.expiration.date.rate=4.*$/no.expiration.date.rate=400/
s/number.of.history.records=.*$/number.of.history.records=300000/
s/exec.time.limit.secs=.*$/exec.time.limit.secs=8/
