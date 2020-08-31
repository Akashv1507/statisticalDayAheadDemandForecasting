create table raw_blockwise_demand
(
id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
time_stamp date,
entity_tag varchar2(100),
demand_value number,
constraints unique_raw_blockwise_demand unique(time_stamp,entity_tag),
constraints pk_raw_blockwise_demand primary key(id)
)