# canal-phoenix-adapter

canal client adapter for phoenix.

Usage
Download the release version that matches the canal, copy it to canal.adapter's plugin directory. Put table mapping file to conf/phoenix. Sample config file mytest_user.yml. Most of the configuration is the same as rdb, but there are some special configurations to control the synchronization of table schema.

notifyUrl: http://url/of/table/changed/listener
dbMapping:
  mapAll: true
  alter: true
  drop: false
  skipMissing: false
  limit: false
  excludeColumns:
  enumColumns:
    column1:
      - enum1
      - enum2
notifyUrl notify when phoenix table changed, POST data is {"database":"","table":"""}. It can also be placed in the properties property of application.xml in canal.adapter
mapAll Map all mysql columns to phoenix table, except the column in excludeColumns
alter Allow modify the phoenix table schema, if mapAll is true, then sync the newly added columns; if drop is true, then sync the newly deleted columns;
drop Allow drop the table columns, alter must be true
skipMissing If true, insert/update will only update phoenix exists columns; If false, and allow to add columns(mapAll is true and alter is true), it will add missing columns, firstly, and then insert/update the value.
limit Whether it is consistent with the mysql field definition. If false, it will add column with precision and scale.
enumColumns enum column mapping
Compile
Download the special version of canal.adapter from https://github.com/alibaba/canal/releases.

$ export CANAL_VERSION=1.1.4 # version must be matched
$ wget https://github.com/alibaba/canal/releases/download/canal-$CANAL_VERSION/canal.adapter-$CANAL_VERSION.tar.gz
$ tar -xzf canal.adapter-$CANAL_VERSION.tar.gz
$ mkdir -p repo/com/alibaba/otter/client-adapter.common/$CANAL_VERSION
$ cp canal.adapter-$CANAL_VERSION/lib/client-adapter.common-$CANAL_VERSION.jar \
  repo/com/alibaba/otter/client-adapter.common/$CANAL_VERSION/
$ mvn clean package
$ cp target/client-adapter.phoenix-$CANAL_VERSION-jar-with-dependencies.jar canal.adapter-$CANAL_VERSION/plugin/
