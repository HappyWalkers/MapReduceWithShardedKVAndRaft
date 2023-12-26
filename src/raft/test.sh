VERBOSE=0 go test -run 2A -race
python3 parallelTest.py 2A -p 100 -n 100 -o ./
python3 parallelTest.py 2A -p 300 -n 900 -o ./
VERBOSE=0 go test -run 2B -race
python3 parallelTest.py 2B -p 200 -n 1000 -o ./
VERBOSE=0 go test -run 2C -race
python3 parallelTest.py 2C -p 50 -n 300 -o ./
VERBOSE=0 go test -run 2D -race
python3 parallelTest.py 2D -p 100 -n 500 -o ./