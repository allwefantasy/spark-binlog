BYZER_SPARK_VERSION=${1-3.0}

if  [ "${BYZER_SPARK_VERSION}" == "3.0" ]; then
  SCALA_BINARY_VERSION=2.12
  ./dev/change-scala-version.sh 2.12
  python ./dev/python/convert_pom.py 3.0

elif [ "${BYZER_SPARK_VERSION}" == "2.4" ]; then
  SCALA_BINARY_VERSION=2.11
  ./dev/change-scala-version.sh 2.11
  python ./dev/python/convert_pom.py 2.4
else
  echo "Only accept 2.4|3.0"
  exit 1
fi