create database if not exists `m_demo`;
use `m_demo`;

create table if not exists `t_source` (
    `id` int not null auto_increment primary key,
    `c_string` varchar(32) not null default ''
) engine=Innodb default charset=utf8mb4;