PACKAGE_VERSION=$(git describe --tags)
echo Render version: $PACKAGE_VERSION

sed "s/@PACKAGE_VERSION@/$PACKAGE_VERSION/g" setup.template.py > setup.py