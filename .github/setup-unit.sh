echo $AZURE_APPLICATION | base64 -d > src/test/resources/application-test.yml
echo "kestra:" > src/test/resources/application-sp.yml
echo "  variables:" >> src/test/resources/application-sp.yml
echo "    globals:" >> src/test/resources/application-sp.yml
echo "      azure:" >> src/test/resources/application-sp.yml
echo "        sp:" >> src/test/resources/application-sp.yml
echo "          tenant: $AZURE_TENANT_ID" >> src/test/resources/application-sp.yml
echo "          username: $AZURE_CLIENT_ID" >> src/test/resources/application-sp.yml
echo "          secret: $AZURE_CLIENT_SECRET" >> src/test/resources/application-sp.yml
echo "        blobs:" >> src/test/resources/application-sp.yml
echo "          connection-string: $AZURE_CONNECTION_STRING" >> src/test/resources/application-sp.yml