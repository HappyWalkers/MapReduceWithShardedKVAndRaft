VERBOSE=0 go test -run 2A -race
python3 parallelTest.py 2A -p 300 -n 1200 -o ./
VERBOSE=0 go test -run 2B -race
python3 parallelTest.py 2B -p 200 -n 1000 -o ./
