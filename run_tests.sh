#!/usr/bin/env bash

# for forcing a failure for testing
if [ "$1" == 'f' ]
then
    exit 1
fi

# run pylint

srcfiles=`find . -path './venv/*' -prune -o -name "*py" -print`

echo "pylint..."

pylint --rcfile pylintrc --msg-template '{C}:{line:3d},{column}: {obj}: {msg} ({symbol}{msg_id})' --disable=fixme ${srcfiles}

#echo "TEMPORARILY IGNORING PYLINT ERRORS"
if [ $? -ne 0 ]
then
    echo "pylint failed"
    exit 1
fi


# run pycodestyle

echo "pycodestyle..."

pycodestyle ${srcfiles}

if [ $? -ne 0 ]
then
    echo "pycodestyle failed"
    # echo "TEMPORARILY IGNORING pycodestyle ERRORS"
#    exit 1
fi

# run bandit

echo "bandit..."
echo "(Not used yet)"
#/usr/local/bin/bandit -r -c bandit.yaml .
#
#if [ $? -ne 0 ]
#then
#    echo "bandit failed"
#    exit 1
#fi


# run tests

echo "tests..."
export PYTHONPATH=.
for i in `find tests test -name 'test_*py'`
do
    python $i
    if [ $? -ne 0 ]
    then
        echo "Test failed for $i"
    fi
done


# all is ok

exit 0
