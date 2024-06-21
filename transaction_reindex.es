GET transactions/_search

POST _reindex
{
  "source": {
    "index": "transactions"
  },
  "dest": {
    "index": "transactions_1"
  },
  "script": {
    "source": """
    ctx._source.transactionDate = new Date(ctx._source.transactionDate).toString();
    """
  }
}

GET transaction_1/_search

POST _reindex
{
  "source": {
    "index": "transactions"
  },
  "dest": {
    "index": "transactions_new"
  },
  "script": {
    "source": """
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    formatter.setTimeZone(TimeZone.getTimeZone('UTC'));
    ctx._source.transactionDate = formatter.format(new Date(ctx._source.transactionDate))
    """
  }
}

GET transactions_2/_search
