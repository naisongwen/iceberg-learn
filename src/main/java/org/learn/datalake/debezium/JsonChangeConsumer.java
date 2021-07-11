package org.learn.datalake.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

import java.util.List;

/***
{
        "schema": {
        "type": "struct",
        "fields": [
        {
        "type": "struct",
        "fields": [
        {
        "type": "int32",
        "optional": false,
        "field": "id"
        },
        {
        "type": "string",
        "optional": true,
        "field": "key"
        },
        {
        "type": "string",
        "optional": true,
        "field": "value"
        }
        ],
        "optional": true,
        "name": "mysql_binlog_source.cdc.cdc_source.Value",
        "field": "before"
        },
        {
        "type": "struct",
        "fields": [
        {
        "type": "int32",
        "optional": false,
        "field": "id"
        },
        {
        "type": "string",
        "optional": true,
        "field": "key"
        },
        {
        "type": "string",
        "optional": true,
        "field": "value"
        }
        ],
        "optional": true,
        "name": "mysql_binlog_source.cdc.cdc_source.Value",
        "field": "after"
        },
        {
        "type": "struct",
        "fields": [
        {
        "type": "string",
        "optional": false,
        "field": "version"
        },
        {
        "type": "string",
        "optional": false,
        "field": "connector"
        },
        {
        "type": "string",
        "optional": false,
        "field": "name"
        },
        {
        "type": "int64",
        "optional": false,
        "field": "ts_ms"
        },
        {
        "type": "string",
        "optional": true,
        "name": "io.org.learn.datalake.debezium.data.Enum",
        "version": 1,
        "parameters": {
        "allowed": "true,last,false"
        },
        "default": "false",
        "field": "snapshot"
        },
        {
        "type": "string",
        "optional": false,
        "field": "db"
        },
        {
        "type": "string",
        "optional": true,
        "field": "table"
        },
        {
        "type": "int64",
        "optional": false,
        "field": "server_id"
        },
        {
        "type": "string",
        "optional": true,
        "field": "gtid"
        },
        {
        "type": "string",
        "optional": false,
        "field": "file"
        },
        {
        "type": "int64",
        "optional": false,
        "field": "pos"
        },
        {
        "type": "int32",
        "optional": false,
        "field": "row"
        },
        {
        "type": "int64",
        "optional": true,
        "field": "thread"
        },
        {
        "type": "string",
        "optional": true,
        "field": "query"
        }
        ],
        "optional": false,
        "name": "io.org.learn.datalake.debezium.connector.mysql.Source",
        "field": "org.learn.datalake.source"
        },
        {
        "type": "string",
        "optional": false,
        "field": "op"
        },
        {
        "type": "int64",
        "optional": true,
        "field": "ts_ms"
        },
        {
        "type": "struct",
        "fields": [
        {
        "type": "string",
        "optional": false,
        "field": "id"
        },
        {
        "type": "int64",
        "optional": false,
        "field": "total_order"
        },
        {
        "type": "int64",
        "optional": false,
        "field": "data_collection_order"
        }
        ],
        "optional": true,
        "field": "transaction"
        }
        ],
        "optional": false,
        "name": "mysql_binlog_source.cdc.cdc_source.Envelope"
        },
        "payload": {
        "before": null,
        "after": {
        "id": 1,
        "key": "a",
        "value": "A"
        },
        "org.learn.datalake.source": {
        "version": "1.4.1.Final",
        "connector": "mysql",
        "name": "mysql_binlog_source",
        "ts_ms": 0,
        "snapshot": "true",
        "db": "cdc",
        "table": "cdc_source",
        "server_id": 0,
        "gtid": null,
        "file": "mysql-bin.081562",
        "pos": 531,
        "row": 0,
        "thread": null,
        "query": null
        },
        "op": "c",
        "ts_ms": 1624262374668,
        "transaction": null
        }
        }

        ****/
public class JsonChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
    @Override
    public void handleBatch(List<ChangeEvent<String, String>> list, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> recordCommitter) throws InterruptedException {
        System.out.println(list);
    }
}
