## Environment

-   OS: Mac OS Mojave 10.14
-   docker: 18.09

## Instructions

After installed docker on your machine, send following command on machine

```sh
docker-compose up
```

Waiting a while to fetch and store documents, then you are ready to play with
documents. Port 9200 is exposed as elasticsearch, you can start query by using
`curl` or chrome extension webui [dejavu](https://chrome.google.com/webstore/detail/dejavu-elasticsearch-web/jopjeaiilkcibeohjdmejhoifenbnmlh?hl=en). Port 27017 is exposed as mongodb, you can access by `mongo` command line tool
or ui based like [mongodb_compass](https://www.mongodb.com/products/compass).
Port 5601 is exposed as kibana service.

mongodb only store metadata. Whereas, elasticsearch only store body content, they
can be cross referenced by id field.

## Configuration

There are two environment variables need to be configured. Check docker-compose.yml
file. There is one environment variable under warc-reader-service section called
`MAX_RECORD_FETCH` which can be modified based on following explanations.

-   0: take no action, this is typically useful when you have fetched certain
    number of docs, then you do not want to fetch more
-   \>0: when value greater than 0 means the number of records you want to fetch
-   <0: when value less than 0, it indicates clean up database and elasticsearch

Another is the `DB_NAME` which is simply the name of database and the index name
of elasticsearch.

## Caveats

set `MAX_RECORD_FETCH` in a small pace, so that even you forget to change this
variable, everytime when you start the service it will only fetch a small amount
of new records, otherwise it may easily blow up your storage.
