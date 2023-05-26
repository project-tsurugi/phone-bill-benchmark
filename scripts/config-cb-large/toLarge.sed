#!/usr/bin/sed -f
s/number.of.contracts.records=.*$/number.of.contracts.records=10000/
s/duplicate.phone.number.rate=.*$/duplicate.phone.number.rate=1000/
s/expiration.date.rate=4.*$/expiration.date.rate=4000/
s/no.expiration.date.rate=4.*$/no.expiration.date.rate=4000/
s/number.of.history.records=.*$/number.of.history.records=30000000/
s/exec.time.limit.secs=.*$/exec.time.limit.secs=600/
