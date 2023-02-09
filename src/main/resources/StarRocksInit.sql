create database if not exists `sr_demo`;
use sr_demo;

CREATE TABLE if not exists `lineorder_flat` (
    `lo_orderdate` date NOT NULL COMMENT '',
    `lo_orderkey` int(11) NOT NULL COMMENT '',
    `lo_linenumber` int(11) NOT NULL COMMENT '',
    `lo_custkey` int(11) NOT NULL COMMENT '',
    `lo_partkey` int(11) NOT NULL COMMENT '',
    `lo_suppkey` int(11) NOT NULL COMMENT '',
    `lo_orderpriority` varchar(65533) NOT NULL COMMENT '',
    `lo_shippriority` int(11) NOT NULL COMMENT '',
    `lo_quantity` int(11) NOT NULL COMMENT '',
    `lo_extendedprice` int(11) NOT NULL COMMENT '',
    `lo_ordtotalprice` int(11) NOT NULL COMMENT '',
    `lo_discount` int(11) NOT NULL COMMENT '',
    `lo_revenue` int(11) NOT NULL COMMENT '',
    `lo_supplycost` int(11) NOT NULL COMMENT '',
    `lo_tax` int(11) NOT NULL COMMENT '',
    `lo_commitdate` date NOT NULL COMMENT '',
    `lo_shipmode` varchar(65533) NOT NULL COMMENT '',
    `c_name` varchar(65533) NOT NULL COMMENT '',
    `c_address` varchar(65533) NOT NULL COMMENT '',
    `c_city` varchar(65533) NOT NULL COMMENT '',
    `c_nation` varchar(65533) NOT NULL COMMENT '',
    `c_region` varchar(65533) NOT NULL COMMENT '',
    `c_phone` varchar(65533) NOT NULL COMMENT '',
    `c_mktsegment` varchar(65533) NOT NULL COMMENT '',
    `s_name` varchar(65533) NOT NULL COMMENT '',
    `s_address` varchar(65533) NOT NULL COMMENT '',
    `s_city` varchar(65533) NOT NULL COMMENT '',
    `s_region` varchar(65533) NOT NULL COMMENT '',
    `s_nation` varchar(65533) NOT NULL COMMENT '',
    `s_phone` varchar(65533) NOT NULL COMMENT '',
    `p_name` varchar(65533) NOT NULL COMMENT '',
    `p_mfgr` varchar(65533) NOT NULL COMMENT '',
    `p_category` varchar(65533) NOT NULL COMMENT '',
    `p_brand` varchar(65533) NOT NULL COMMENT '',
    `p_color` varchar(65533) NOT NULL COMMENT '',
    `p_type` varchar(65533) NOT NULL COMMENT '',
    `p_size` int(11) NOT NULL COMMENT '',
    `p_container` varchar(65533) NOT NULL COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`lo_orderdate`, `lo_orderkey`)
COMMENT "OLAP"
PARTITION BY RANGE(`lo_orderdate`)
(PARTITION p1 VALUES [('0000-01-01'), ('1993-01-01')),
PARTITION p2 VALUES [('1993-01-01'), ('1994-01-01')),
PARTITION p3 VALUES [('1994-01-01'), ('1995-01-01')),
PARTITION p4 VALUES [('1995-01-01'), ('1996-01-01')),
PARTITION p5 VALUES [('1996-01-01'), ('1997-01-01')),
PARTITION p6 VALUES [('1997-01-01'), ('1998-01-01')),
PARTITION p7 VALUES [('1998-01-01'), ('1999-01-01')))
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48 
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "groupxx1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);

create database if not exists `m_demo`;
use m_demo;

create table if not exists `t_source` (
    `id` int(11) not null,
    `c_string` string
)ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);