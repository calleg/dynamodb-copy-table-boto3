# dynamodb-copy-table-boto3
A simple python 3 script to copy dynamodb table based on boto3.

I wrote this script a while ago. Inspired by "[A simple python 3 script to copy dynamodb table based on old boto.](https://github.com/techgaun/dynamodb-copy-table)", I decided to publish it here. However, this script has not so many options as the original inspiration but uses boto3. In short, it can only copy data from one table to another table yet.

---

### Requirements

- Python 3.x
- boto3 (`pip install boto3`)

### Usage

A simple usage example:

```shell
$ python dynamodb-copy.py <src_table> <dst_table> [profile_name]
```
### References
- [A simple python 3 script to copy dynamodb table based on old boto.](https://github.com/techgaun/dynamodb-copy-table)
- [Import and Export DynamoDB Data using AWS Data Pipeline](http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-importexport-ddb.html)
