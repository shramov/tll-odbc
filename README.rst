ODBC Channel for TLL framework
==============================

Channel implements read and write operations using ODBC API for flat messages - no lists,
submessages or unions.

For example channel with url ``odbc://;dsn=testdb;quote-mode=sqlite`` will connect to ``testdb``
ODBC database (specified either in user config ``~/.odbc.ini`` or in global one ``/etc/odbc.ini``)
with ``sqlite`` quoting mode.

Name quoting
~~~~~~~~~~~~

Number of table and field name quoting methods are supported:

* ``sqlite``: quote with backticks as ```table```;
* ``psql``: quote with double quotes as ``"table"``;
* ``sybase``: quote with square brackets as ``[table]``;

Method is selected with ``quote-mode`` channel parameter.

Statement templates
-------------------

SQL statements are built on channel open from message options. Statement can be either write-only
(insert or procedure call) or read-write (function call or raw query), where ``sql.output:
message-name`` controls what messages are filled from result dataset. Possible templates
(``sql.template`` message option) are: 

* ``none`` - don't create any statement;
* ``insert`` - insert statement of form ``INSERT INTO {table}(i0, i1, ...) VALUES ?, ?, ...``;
* ``function`` -  select of form ``SELECT o0, o1, ... FROM {func}(?, ?, ...)``, or if
  ``function-mode=empty`` is given - ``SELECT FROM {func}(?, ?, ...)``;
* ``procedure`` - call of form ``CALL {func}(?, ?, ...)``;

Message sequence (``msg.seq``) number is implicitly included in dataset as ``_tll_seq`` field but can be
excluded with ``sql.with-seq: no`` option.

Example of function call, where ``Input`` initiates call and generates bunch of ``Output`` messages with
swapped fields:

.. code::

  - name: Input
    options.sql.table: TestFunction
    options.sql.output: Output
    options.sql.template: function
    options.sql.with-seq: no
    id: 10
    fields:
      - {name: a, type: int32}
      - {name: b, type: double}
  - name: Output
    options.sql.with-seq: no
    options.sql.template: none
    id: 20
    fields:
      - {name: a, type: double}
      - {name: b, type: int32}

Selecting data
--------------

There is no template for select statements - it would be either very limited or needs lot of options
to provide enough functionality. Two ways to query data are implemented:

* dynamic queries using ``Query`` control message
* writing raw query in message ``sql.query`` option

Example of prepared SELECT statement, where data is stored in table ``Table`` with ``Insert`` and
queried with ``Select`` messages (providing stream of ``Insert``).

.. code::

  - name: Insert
    options.sql.table: Table
    id: 10
    fields:
      - {name: f0, type: int32}
      - {name: f1, type: double}

  - name: Select
    options.sql.with-seq: no
    options.sql.query: 'SELECT "_tll_seq", "f0", "f1" FROM "Table" WHERE "f0" < ?'
    options.sql.output: Insert
    id: 20
    fields:
      - {name: upper_bound, type: int32}

..
  vim: sts=2 sw=2 et tw=100
