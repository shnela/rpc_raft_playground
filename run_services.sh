#/bin/bash
# to kill background processes: `pkill -f service.py`

source ~/.venvs/raft/bin/activate

for port in 18850 18851 18852; do
    python service.py $port &
done;
