Installing the AWS CLI

`any-node:~$ mkdir ~/Downloads`

`any-node:~$ curl -L https://s3.amazonaws.com/aws-cli/awscli-bundle.zip -o ~/Downloads/awscli-bundle.zip`

`any-node:~$ unzip ~/Downloads/awscli-bundle.zip`

`any-node:~$ sudo ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws`

Configuring AWS CLI

`any-node:~$ nano ~/.profile`

Add the following for the AWS environment variables (but with your own credentials)

`export AWS_ACCESS_KEY_ID=<your-access-key-id>`

`export AWS_SECRET_ACCESS_KEY=<your-secret-access-key>`

`export AWS_DEFAULT_REGION=<your-cluster-region>`

`any-node:~$ . ~/.profile`

How to run

`~$ spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.1 --master spark://ip:port --executor-memory 6G customers_ingestion.py`
