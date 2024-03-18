DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WORK_DIR=Sorting_Output
if [[ ! "$WORK_DIR" || ! -d "$WORK_DIR" ]]; then
  echo "Could not create temp dir"
  exit 1
fi
function cleanup {      
  rm -rf "$WORK_DIR"
  echo "Deleted temp working directory $WORK_DIR"
}
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
trap cleanup EXIT
spark-submit CsvFilterAndSort.py $WORK_DIR
if [ $? -eq 0 ]; then
    echo -e "Test Case  -  ${GREEN}PASS${NC}"
else
    echo -e "Test Case  -  ${RED}FAIL${NC}"
fi
