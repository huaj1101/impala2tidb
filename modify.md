# 因tidb的bug，需要改变写法

## ceiling/floor函数

ceiling(omei.month/3)=stat_cycle.last_stat_quarter) 绕行写为：

ceiling(0+omei.month/3)=stat_cycle.last_stat_quarter) 

SQL1：

```sql
/* from:'dp-index-service', addr:'10.180.253.170' */
with stat_cycle as (
    select dd.org_id,
       dd.stat_quarter_start,
       dm.stat_month_start last_stat_month_start,
       dm.stat_month_end last_stat_month_end,
       dm.stat_year_start last_stat_year_start,
       dm.stat_year last_stat_year,
       dm.stat_month  last_stat_month,
       dm.stat_quarter_start last_stat_quarter_start,
       dm.stat_quarter_end last_stat_quarter_end,
       dm.stat_quarter last_stat_quarter
    from global_dwb.d_date dd
    inner join global_dwb.d_month dm
        on dm.org_id=dd.org_id
            and dm.stat_month_end=date_sub(dd.stat_month_start, 1)
    where dd.org_id=713535955170304 
        and dd.catalog='pp-p'
        and dd.stat_date=to_date(now())
        and dm.catalog='pp-p' 
),
year_amount as (
    select 
        org.child_org_id,
        sum(prpq.amount)/100000000  year_amount
    from global_platform.org_relation org
    inner join global_platform.project p
        on org.child_org_id = p.org_id
    inner join global_ipm.project_record pr 
        on pr.org_id=org.child_org_id
    inner join global_ipm.project_record_progress_quantity prpq 
        on pr.id=prpq.project_record_id
            and pr.org_id=prpq.org_id
    inner join stat_cycle 
        on stat_cycle.org_id=org.org_id
    where org.org_id=713535955170304
          and pr.is_removed=false
          and pr.record_date>=stat_cycle.last_stat_year_start
          and pr.record_date <= stat_cycle.last_stat_month_end
          and prpq.is_removed=false
          and org.child_ext_type='project'
          and p.is_removed=false
    group by org.child_org_id
),

month_amount as (
    select 
        org.child_org_id,
        sum(prpq.amount)/100000000  month_amount
    from global_platform.org_relation org 
    inner join global_platform.project p
        on org.child_org_id = p.org_id
    inner join global_ipm.project_record pr 
        on pr.org_id=org.child_org_id
    inner join global_ipm.project_record_progress_quantity prpq 
        on pr.id=prpq.project_record_id 
            and pr.org_id=prpq.org_id
    inner join stat_cycle
        on stat_cycle.org_id=org.org_id
    where org.org_id=713535955170304
          and pr.is_removed=false
          and pr.record_date>=stat_cycle.last_stat_month_start
          and pr.record_date <= stat_cycle.last_stat_month_end
          and prpq.is_removed=false
    group by org.child_org_id
),

month_plan as (
    select 
        org.child_org_id,
        sum(pmp.plan_amount)/100000000  month_plan
    from global_platform.org_relation org
    inner join global_platform.project p
        on org.child_org_id = p.org_id 
    inner join global_ipm.project_month_plan pmp 
        on pmp.org_id=org.child_org_id
    inner join stat_cycle
        on stat_cycle.org_id=org.org_id
    where org.org_id=713535955170304
        and org.child_ext_type='project'
        and p.is_removed=false
        and pmp.is_removed=false
        and pmp.month=stat_cycle.last_stat_month
        and pmp.year=stat_cycle.last_stat_year
    group by org.child_org_id
),

quarter_plan as (
    select omei.org_id,
        sum(omei.production_value)/100000000  quarter_plan
    from global_ipm.org_month_extras_info omei 
    inner join stat_cycle
        on stat_cycle.org_id=omei.org_id 
    where omei.org_id=713535955170304
        and omei.is_removed=false
        and omei.year=stat_cycle.last_stat_year
        and ceiling(omei.month/3)=stat_cycle.last_stat_quarter
    group by omei.org_id
),
quarter_amount as (
    select org.child_org_id,
          sum(prpq.amount)/100000000  quarter_amount
    from global_platform.org_relation org 
    inner join stat_cycle 
        on stat_cycle.org_id=org.org_id
    inner join global_ipm.project_record pr 
        on pr.org_id=org.child_org_id  
    inner join global_ipm.project_record_progress_quantity prpq 
        on pr.id=prpq.project_record_id
            and pr.org_id=prpq.org_id
            and pr.record_date >= stat_cycle.last_stat_quarter_start 
            and pr.record_date <= stat_cycle.last_stat_month_end
          and prpq.is_removed=false
          and pr.is_removed=false
    group by org.child_org_id
)

select
    ifnull(production_value.production_value,0)/100000000 year_plan,
    ifnull(quarter_plan.quarter_plan,0) quarter_plan,
    sum(ifnull(year_amount.year_amount,0)) year_amount,
    sum(ifnull(month_amount.month_amount,0)) month_amount,
    sum(ifnull(month_plan.month_plan,0)) month_plan,
    sum(ifnull(quarter_amount.quarter_amount,0)) quarter_amount
from global_platform.org_relation org
inner join global_platform.project pro 
    on pro.org_id=org.child_org_id
inner join stat_cycle 
    on stat_cycle.org_id=org.org_id
left join global_ipm.org_year_extras_info oyei
    on org.child_org_id=oyei.org_id
left join year_amount                                                                                                     
    on year_amount.child_org_id=org.child_org_id
left join month_amount
    on month_amount.child_org_id=org.child_org_id
left join month_plan
    on month_plan.child_org_id=org.child_org_id
left join global_ipm.org_year_extras_info production_value
    on org.org_id=production_value.org_id
        and production_value.is_removed=false
        and production_value.year=year(now())
left join quarter_plan
    on quarter_plan.org_id=org.org_id
left join quarter_amount
    on quarter_amount.child_org_id=org.child_org_id
where org.org_id=713535955170304
    and org.child_ext_type="project"
    and pro.is_removed=false
    and oyei.is_removed=false
    and oyei.in_hand=true
    and oyei.year=year(now())
    
group by 
    production_value.production_value,
    quarter_plan.quarter_plan
/*& tenant:'cr16g' */
/*& $replace:tenant */
```

SQL2：

```sql
select da.org_id as '公司id', 
       isnull(info.production_value,0)/10000/da.stat_month_day_count as '日均计划',
       isnull(info.production_value,0)/10000 as '月度计划',
       (DATEDIFF('2022-05-18',stat_month_start)+1)/stat_month_day_count as '月度应完',
       sum(isnull(info_qu.production_value/10000,0)) as '季度计划',
       isnull(info_year.production_value/10000,0) as '年度计划',
       (DATEDIFF('2022-05-18',stat_year_start)+1)/stat_year_day_count  as '年度应完'
  from global_dwb.d_date  da
  left join global_ipm.org_month_extras_info info on da.org_id=info.org_id and info.year=da.stat_year and info.month=da.stat_month and info.is_removed=false
  left join global_ipm.org_month_extras_info info_qu on da.org_id=info_qu.org_id and info_qu.year=da.stat_year and CEILING(info_qu.month/3)=da.stat_quarter and info_qu.is_removed=false
  left join global_ipm.org_year_extras_info info_year on da.org_id=info_year.org_id and info_year.year=da.stat_year and info_year.is_removed=false
 where da.id = CONCAT('pp-p-',REPLACE(LEFT(CAST('2022-05-18' AS string),10),'-',''),'-',CAST(436128129487360 AS string))
 group by da.org_id, 
       info.production_value,
	   da.stat_month_day_count,
       stat_month_start,
       info_year.production_value,
       stat_year_start,stat_year_day_count;
/*& tenant:crfeb */
/*& $replace:tenant */
```

## 常量列导致的bug

貌似是union的两个部分里有limit，外面再加一个常量列app_origin，触发了tidb的bug

把常量列改写到union的各个部分里即可

```sql
/* from:'node-mb2-public-data-service', addr:'10.180.88.155' */
select *, 0 app_origin from (
        -- 租户大库
        SELECT name as change_name, name_initials, id, name, address, short_name, short_name_initials, credit_code, legal_person_name, contact_person_name, 
          contact_person_position, bank_deposit, registered_capital, business_scope, 
          phone_number, bank_account, remark, company_type, cooperation_type_json, origin, !is_black_list is_disable
          FROM company_org WHERE is_removed=false and scope_org_id = 651386286258176 AND (name LIKE '%上海%' or name_initials like '%上海%')
          limit 20
          union all
        -- 租户更名库关联大库信息
        select a.supplier_name as change_name, a.initials name_initials, b.* from global_mtlp.g_supply_change a
          join (
            select id, name, address, short_name, short_name_initials, credit_code, legal_person_name, contact_person_name, 
            contact_person_position, bank_deposit, registered_capital, business_scope, 
            phone_number, bank_account, remark, company_type, cooperation_type_json, origin, !is_black_list is_disable
            FROM company_org WHERE is_removed=false and scope_org_id = 651386286258176 and id in (
              select supplier_id from global_mtlp.g_supply_change where is_removed = false AND (supplier_name LIKE '%上海%' or initials like '%上海%') 
            )
          ) b
          on a.supplier_id = b.id
          where a.is_removed = false AND (a.supplier_name LIKE '%上海%' or a.initials like '%上海%')
          limit 20
        ) a
        limit 20
```

# 拼接的SQL太长

service：pr-timer-service

SQL：

```sql
UPSERT INTO crssg_custom.crssg_bi_finance_index_analysis  (org_id,id,year,month,org_code,org_name,parent_code,flag,index_type,index_name,value,is_removed,creator,created_at,reviser,updated_at,version) 
VALUES (552383621616640, 'T02050010010330000000_2022_4_营业收入_本月值', 2022, 4, 'T02050010010330000000', '中铁十四局集团水利水电分公司清欠中心', 'C02050160000000000009', '季报', '营业收入', '本月值', -723.49, false, 10001, '2022-05-19 03:57:44.1000', 10001, '2022-05-19 03:57:44.1000', 1275355537397224), ...
```

发现拼出来的SQL有18M之多，几万行。改为按批次拼接和执行，最多1000条一个批次。

# SQL语法错误

impala语法检查相当宽松，有一些SQL写的不太妥当，在impala里能执行，在tidb里会报错

## 没有limit有offset

去掉offset

```
/* from:'node-mq2-rds-service', addr:'10.180.137.1' */
SELECT `id`, `org_id`, `order_id`, `labour_id`, `labour_name`, `gh_id`, `gh_name`, `gh_full_id`, `gh_full_name`, `ori_gh_id`, `material_id`, `material_code`, `material_name`, `material_model`, `material_unit`, `auxiliary_unit`, `class_id`, `class_full_id`, `inventory_quantity`, `net_quantity`, `has_sale`, `residue_quantity`, `last_check_quantity`, `last_labour_rest_quantity`, `labour_consume_quantity`, `diff_quantity`, `receive_quantity`, `delivery_quantity`, `is_check`, `price`, `item_remark`, `ori_org_id`, `ori_material_id`, `ori_labour_id`, `ori_common_id`, `ori_class_id`, `ori_item_id`, `ori_order_id`, `sort_code`, `is_removed`, `creator_id`, `creator_name`, `created_at`, `modifier_id`, `modifier_name`, `updated_at`, `version`, `storage_place`, `adjustment_reasons` 
FROM `q_check_store_item` AS `qCheckStoreItem` 
WHERE `qCheckStoreItem`.`org_id` = 1229202466600936 AND `qCheckStoreItem`.`order_id` = 1268412608460728 AND `qCheckStoreItem`.`is_removed` = false AND `qCheckStoreItem`.`is_check` = true ORDER BY `qCheckStoreItem`.`id` 
OFFSET 0;
```

## 没有聚合函数写了group

没看懂group的意义何在，如果是要去重复记录用distinct

SQL1：

```sql
/* from:'node-ca-concrete-service', addr:'10.180.38.176' */
with lopsum as
    (
      select  concat( material,cast( isnull(material_id,0) as string)) col_name,material,isnull(material_id,0)material_id,material_name,material_model,auxiliary_unit  
      from q_manual
      where org_id=1245096272288768  and pro_line='undefined' and dat_tim>= '2022-05-01 00:00:00' and dat_tim<='2022-05-18 14:38:42'
      group by material,isnull(material_id,0),material_name,material_model,auxiliary_unit
    ),
    bulusum as
    (
      select concat( b.material,cast( isnull(b.material_id,0) as string)) col_name,b.material,isnull(b.material_id,0)material_id,b.material_name,b.material_model,b.material_unit
      from q_manual_supplement as a
      inner join q_manual_supplement_item b 
      on a.id=b.order_id
      where a.org_id=1245096272288768  and pro_line='undefined'and a.is_audit=true and dat_tim>= '2022-05-01 00:00:00' and dat_tim<='2022-05-18 14:38:42' and a.is_removed=false and b.is_removed=false
      group by b.material,b.material_id,b.material_name,b.material_model,b.material_unit
    ),
    dtcoltabletemp as
    (
      select col_name,material,material_id,material_name,material_model,auxiliary_unit from lopsum
      union all
      select col_name,material,material_id,material_name,material_model,material_unit from bulusum
    ),
    dtcoltable as
    (
      select col_name,material,material_id,isnull(material_name,'')material_name,isnull(material_model,'')material_model,isnull(auxiliary_unit,'')auxiliary_unit from dtcoltabletemp
      group by col_name,material,material_id,material_name,material_model,auxiliary_unit
    )
    select * from dtcoltable order by material
```

## group by的内容跟select里的内容对不上

要么写表达式里的所有字段，要么写别名，都对不上就会报错

SQL1

```sql
/* from:'node-ca-concrete-service', addr:'10.180.38.176' */
with lopsum as
    (
      select  concat( material,cast( isnull(material_id,0) as string)) col_name,round(sum(fact_amnt)/1000,3) fact_amnt  
      from q_manual
      where org_id=1245096272288768  and pro_line='undefined' and dat_tim>= '2022-05-01 00:00:00' and dat_tim<='2022-05-18 14:38:42'
      group by material,isnull(material_id,0)
    )
    select * from lopsum order by col_name
```

SQL2

```sql
select 
    pro.name as '项目全称',
    pro.short_name as '项目简称',
    concat(pro.construct_type,if(pro.second_construct_type is null,'',concat('-',pro.second_construct_type))) as '工程类别',
    isnull(this_m_report.actual_start,pro.actual_start) as '开工日期',
    pro.closing_date as '收尾日期',
    isnull(this_m_report.actual_delivery_date,pro.field02) as '交工日期',
    isnull(this_m_report.actual_completion_date,pro.field04) as '竣工日期',
    case isnull(this_m_report.construct_status,pro.construct_status) when 'none' then '未开工' 
                              when 'begining' then '新开'
                              when 'building' then '在建'
                              when 'stop' then '停工'
                              when 'finished' then '已移交'
                              when 'conclusion' then '收尾'
                              when 'termination' then '合同终止' end as '工程状态',
    isnull(this_m_report.contract_amount,pro.construction_master_plan/10000) as '有效合同额',
    pro.field38 as '结算金额',
    sum(if(report.year = if(month('2022-04-01') = 1, year("2022-04-01") - 1, year("2022-04-01")) and report.month = if(month("2022-04-01") = 1, 12, month("2022-04-01") - 1), 
        isnull(report.contract_amount, 0) - isnull(report.surplus_contract_amount, 0), 0)) as '查询期间前累计完成产值',
    sum(if(report.year = if(month('2022-04-01') = 1, year("2022-04-01") - 1, year("2022-04-01")) and report.month = if(month("2022-04-01") = 1, 12, month("2022-04-01") - 1), 
        report.month_complete_amount, 0)) as '查询期间前一个月计完成产值',
    sum(if(to_date(concat(cast(report.year as string),'-',if(report.month<10,'0',''), cast(report.month as string),'-', cast(day('2022-04-01') as string))) >= to_date('2022-04-01')
        and to_date(concat(cast(report.year as string),'-',if(report.month<10,'0',''), cast(report.month as string),'-', cast(day('2022-04-01') as string))) <= to_date('2022-04-01'),
        report.month_complete_amount, 0)) as '查询期间实际完成产值',
    sum(if(report.year = if(month('2022-04-01') = 12, year("2022-04-01") + 1, year("2022-04-01")) and report.month = if(month("2022-04-01") = 12, 1, month("2022-04-01") + 1), 
        report.month_complete_amount, 0)) as '查询期间后一个月计完成产值',
    max(total.total_amount) as '当前开累完成产值',
    max(total.surplus_amount) as '剩余产值'
    
    
from global_platform.org_relation org
    inner join global_platform.project pro  
        on org.child_org_id = pro.org_id
        and pro.is_removed = false
    left  join gxlq_custom.gxlq_project_month_report report 
        on report.org_id = pro.org_id
        and (report.year < year('2022-04-01') or (report.year = year('2022-04-01') and report.month <= month('2022-04-01')+1))
        and report.is_removed = false
        and report.status = 1
    inner  join (
        select *
        from (
            select 
                row_number() over(partition by org_id order by year desc, month desc) as rid,
                org_id,
                year,
                month,
                isnull(contract_amount, 0) - isnull(surplus_contract_amount, 0) as total_amount,
                surplus_contract_amount as surplus_amount
                

            from gxlq_custom.gxlq_project_month_report 
            where org_id in (select child_org_id from global_platform.org_relation where org_id = 843929707672064 and child_ext_type = 'project')
                and is_removed = false
                and status = 1
        ) new
        where new.rid = 1
    ) total on total.org_id = pro.org_id
    left  join gxlq_custom.gxlq_project_month_report this_m_report
        on this_m_report.is_removed = false
        and this_m_report.status = 1
        and this_m_report.year = year("2022-04-01")
        and this_m_report.month = month("2022-04-01")
        and this_m_report.org_id = pro.org_id

where org.child_ext_type = "project"
    and org.org_id = 843929707672064
    
group by 
    org.child_order_no,
    pro.name,
    pro.short_name,
    concat(pro.construct_type,if(pro.second_construct_type is null,'',concat('-',pro.second_construct_type))),
    isnull(this_m_report.actual_start,pro.actual_start),
    pro.closing_date,
    isnull(this_m_report.actual_delivery_date,pro.field02),
    isnull(this_m_report.actual_completion_date,pro.field04),
    isnull(this_m_report.construct_status,pro.construct_status),
    isnull(this_m_report.contract_amount,pro.construction_master_plan/10000),
    pro.field38
order by 
    org.child_order_no
/*& tenant:gxlq */
/*& $replace:tenant */
```

SQL3：

这个SQL有些奇怪，可能触发了tidb的bug，需要这么改写才能通过

if(year(pro.contract_sign_date)=year('2022-05-01'),sum(temp.计划),0)/10000 as '本年计划',

改写为：

sum(if(year(pro.contract_sign_date)=year('2022-05-01'),temp.计划,0)/10000) as '本年计划',

```sql
select
pro.org_id,
if(year(pro.contract_sign_date)=year('2022-05-01'),sum(temp.计划),0)/10000 as '本年计划',
if(year(pro.contract_sign_date)=year('2022-05-01'),sum(temp.完成),0)/10000 as '本年完成',
if(year(pro.contract_sign_date)=year('2022-05-01'),sum(temp.实物产值),0) as '本年实物产值',
if(year(pro.contract_sign_date)!=year('2022-05-01'),sum(temp.计划),0)/10000 as '上年结转计划',
if(year(pro.contract_sign_date)!=year('2022-05-01'),sum(temp.完成),0)/10000 as '上年结转完成',
if(year(pro.contract_sign_date)!=year('2022-05-01'),sum(temp.实物产值),0) as '上年结转实物产值'
from global_platform.project pro
left join (
  select 
        org_id,
        sum(isNull(quantity,0)) as '计划',
        null as '完成',
        null as '实物产值'
    from global_ipm.project_year_plan_detail
    where is_removed=false
      and year=year('2022-05-01')
      and item_type='production_task'
    group by org_id
  union all
    select 
        org_id,
        null as '计划',
        sum(isNull(price,0)) as '完成',
        null as '实物产值'
  from global_ipm.ec_project_monthly_production
  where is_removed=false 
    and is_released=true
    and year=year('2022-05-01')
    and month<=month('2022-05-01')
  group by org_id
  union all
    select 
      org_id,
      null as '计划',
      null as '完成',
      sum(isNull(actual_amount,0)) as '实物产值'
  from global_dw_1.agg_pp_project_monthly
  where stat_year=year('2022-05-01')
    and stat_month<=month('2022-05-01')
  group by org_id
) temp on temp.org_id=pro.org_id
where pro.is_removed=false
    and left(cast(contract_sign_date as string),10)<=(
                              SELECT 
                              left(cast(stat_month_end as string),10) as stat_month_end 
                            from global_dwb.d_month 
                            where stat_month=month('2022-05-01') 
                              and stat_year=year('2022-05-01') 
                              and org_id=655718387915264 and catalog='pr-g'
                              )
group by pro.org_id,year(pro.contract_sign_date)
/*& tenant:cr11gcsgd */
/*& $replace:tenant */
```

## 表别名不存在

SQL1

f.update_at = now()，没有别名为f的表

```sql
UPDATE p
SET
  p.is_removed = TRUE,
  p.reviser = 1219772653868544,
  p.version = 1274980879054336,
  f.updated_at = now()
FROM project_quantity_param AS p
JOIN (
SELECT q.id
FROM project_entry_work_quantity AS q
JOIN project_entry_work_quantity AS qp
  ON qp.id = q.parent_id
  AND qp.org_id = q.org_id
  AND qp.is_removed = FALSE
WHERE
  q.org_id = 1059206399841280
  AND qp.project_entry_work_id IN (1234486545464900, 1234486545464901, 1234486545464902, 1234486545464903, 1234486545464904)
  AND qp.component_id = 1235369754909184
  AND q.parent_id > 0
  AND q.is_removed = FALSE
UNION ALL
SELECT id
FROM project_entry_work_quantity
WHERE
  org_id = 1059206399841280
  AND project_entry_work_id IN (1234486545464900, 1234486545464901, 1234486545464902, 1234486545464903, 1234486545464904)
  AND component_id = 1235369754909184
  AND parent_id = -1
  AND is_removed = FALSE
) quantity 
  ON quantity.id = p.project_entry_work_quantity_id
WHERE p.is_removed = FALSE AND p.param_type <> 'feature'
  AND p.org_id = 1059206399841280
```

