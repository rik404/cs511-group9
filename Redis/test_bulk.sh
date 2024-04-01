GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "Running the sorting function test cases"
for i in $(seq 1 10);
do
    bash testsetup_docker.sh >> all_out.log 2>&1
    if [ $? -eq 0 ]; then
        echo -e "Test Case $i -  ${GREEN}PASS${NC}"
    else 
        echo -e "Test Case $i -  ${RED}FAIL${NC}"
    fi
done
