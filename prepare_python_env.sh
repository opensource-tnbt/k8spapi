VINEPROXENV_DIR="$HOME/vineproxenv"
VINEPROX_DIR="$HOME/anuket/vineprox"
# This should match the  TRAFFICGEN_PROX_CONF_DIR
PROXCONF_DIR="/tmp/prox"
OLD_FORMAT_FILE='format.yaml'
NEW_FORMAT_FILE=$PROXCONF_DIR'/'$OLD_FORMAT_FILE

echo $NEW_FORMAT_FILE

echo $VINEPROXENV_DIR

if [ -d "$VINEPROXENV_DIR" ] ; then
    echo "Directory $VINEPROXENV_DIR already exists. Skipping python virtualenv creation."
    exit
fi

(virtualenv "$VINEPROXENV_DIR" --python /usr/bin/python3
source "$VINEPROXENV_DIR"/bin/activate
pip install -U pip
pip install -r requirements.txt
git clone https://gerrit.opnfv.org/gerrit/samplevnf /tmp/samplevnf
cd /tmp/samplevnf/VNFs/DPPD-PROX/helper-scripts/rapid
sed -i "s|$OLD_FORMAT_FILE|$NEW_FORMAT_FILE|g" rapid_test.py
cp helper.lua $VINEPROX_DIR
mkdir $PROXCONF_DIR
cp format.yaml $PROXCONF_DIR
python setup.py build
python setup.py install
rm -rf /tmp/samplevnf)
