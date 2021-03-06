





# 0. 测试方法

由于目前还没有开放tidb的验证环境，可以用postman通过这个api来验证：

http://192.168.1.146:9527/transfer-by-text?db=xxxx&tenant=xxxx&dryrun=true

POST方法，body用raw直接贴impala里的SQL

response的body里有会执行结果，如果有异常会给出异常信息

另外关注api调用的返回时长，基本代表了sql执行的性能

# 1. 因tidb的特性或bug，需要改变写法

## 1.1 ceiling/floor函数

感觉这属于tidb的bug，绕行

ceiling(omei.month/3)=stat_cycle.last_stat_quarter) 绕行写为：

ceiling(0+omei.month/3)=stat_cycle.last_stat_quarter) 

SQL 1.1.1：

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

SQL 1.1.2：

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

## 1.2 常量列导致的问题

貌似是union的两个部分里有limit，外面再加一个常量列app_origin，触发了tidb的bug

把常量列改写到union的各个部分里即可

SQL 1.2.1

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

## 1.3 with语句块

with子句，在impala里是语法糖，运行时会嵌入到主SQL中

但是在tidb里，with是标准的CTE实现，会把with先行运算，结果取到内存中，再进行后续操作

如果with子句的结果集不大，那没多少差异，可能还会更快

但是有的结果集非常大，百万千万条，在tidb里效率就很低了

此时为了兼容考虑，需要取消with子句，直接嵌入到主SQL中

以下是搜到的需要改的SQL

SQL 1.3.1：

把orgs嵌入到dosage语句块，不然会把q_dosage符合条件的结果都取到内存中再与cte去join，几千万条

嵌入之后会发现依然慢，先不用管，这是其他问题，这一波不解决

```sql
/* from:'node-ca-concrete-service', addr:'10.180.38.176' */
with orgs as ( -- pc门户--类别汇总应耗实耗
              select child_org_id from global_platform.org_relation where org_id=814036348678608 and child_type='project'
          ),
          dosage as (
            select  material_id,
            isnull(sum(plan_amn),0)/1000 plan_amn, isnull(sum(fact_amnt),0)/1000 fact_amnt
            from q_dosage 
            join  orgs
            on q_dosage.org_id=orgs.child_org_id    
            where  is_removed=false   
                   and dat_tim>= '1900-01-01 00:00:00' and dat_tim<= '2022-05-18 10:08:31'
            group by material_id
          ),
          static_class as (
            select category_name,category_id,material_id,material_unit from (select b.id as category_id,b.name category_name,b.material_unit
            from g_statistic_category as b 
              where b.is_removed=false
                  and b.dict_type = 'statistic'
                  and b.custome_code in ('statistic_shuini','statistic_shizi','statistic_shazi','statistic_waijiaji','statistic_fenmeihui')) c
              inner join g_statistic_category_material as a on a.statistic_id = c.category_id
          )
          select category_id,isnull(category_name,'暂无类别') as statistic_class_name,
              cast(isnull(sum(plan_amn),0) as decimal(28,3)) as plan_amn,
              cast(isnull(sum(fact_amnt),0) as decimal(28,3)) as fact_amnt,
              cast(isnull(sum(fact_amnt),0) as decimal(28,3))-cast(isnull(sum(plan_amn),0) as decimal(28,3)) as deviation
          from static_class
          left join dosage
          on dosage.material_id=static_class.material_id 
          group by category_name,category_id
          order by category_id
```

SQL 1.3.2：

同SQL 1.3.1

```mysql
/* from:'node-ca-concrete-service', addr:'10.180.38.133' */
with orgs as (select child_org_id from global_platform.org_relation where org_id=969222554611712 and child_type='project'),
          static_class as ( -- 门户 消耗数量
             select category_name,category_id,material_id,material_unit 
                from (select b.id as category_id,b.name category_name,b.material_unit
                      from g_statistic_category as b 
                      where b.is_removed=false
                        and b.dict_type = 'statistic'
                        and b.custome_code in ('statistic_shuini','statistic_shizi','statistic_shazi','statistic_waijiaji','statistic_fenmeihui')
                ) c
             inner join g_statistic_category_material as a on a.statistic_id = c.category_id
          ),
          supplementData as(
             select isnull(b.material_id,0)material_id,
                cast(isnull(b.fact_amnt/1000,0) as decimal(28,8))quantity 
             from q_manual_supplement as a
                inner join q_manual_supplement_item b 
             on a.id=b.order_id
                where a.org_id in(select child_org_id from orgs) 
                  and a.is_productionsystem = true 
                  and a.is_audit=true 
                  and dat_tim>= '1900-01-01 00:00:00' and dat_tim<= '2022-06-01 20:21:15'
                  and a.is_removed=false and b.is_removed=false
          ),
          dosageData as (
            select  material_id,
            cast(isnull(sum(plan_amn),0)/1000 as decimal(28,8)) plan_amn, 
            cast(isnull(sum(fact_amnt),0)/1000 as decimal(28,8)) fact_amnt 
            from q_dosage 
            join  orgs
            on q_dosage.org_id=orgs.child_org_id    
            where  is_removed=false   
                   and dat_tim>= '1900-01-01 00:00:00' and dat_tim<= '2022-06-01 20:21:15'
            group by material_id
          ),
          dosage as(
                select 
                    material_id,
                    sum(plan_amn)plan_amn,
                    sum(fact_amnt) fact_amnt
                from(
                    select material_id,plan_amn,fact_amnt from dosageData
                    union all
                    select material_id,quantity,quantity from supplementData
                ) as a
                group by material_id
          )
      select category_id as statistic_class_id, isnull(category_name,'暂无类别') as statistic_class_name,
            cast(isnull(sum(plan_amn),0) as decimal(28,3)) as plan_amn,
            cast(isnull(sum(fact_amnt),0) as decimal(28,3)) as fact_amnt
        from static_class
        left join dosage
        on  dosage.material_id=static_class.material_id 
        group by category_id,category_name
        order by category_id
```

SQL 1.3.3：

把orgs、produceTmp两个CTE展开到使用的地方，能快一大半，但是依然要3秒多

这个SQL比较复杂，数据量也大，格式也乱，没细看还能不能优化

```sql
/* from:'node-mp-common-service', addr:'10.180.38.136' */
WITH orgs AS(SELECT child_org_id FROM global_platform.org_relation WHERE org_id = 1012251829238272  AND child_type = 'project'),produceTmp AS(SELECT org_id,dat_tim,schedule_id,labour_name,vehicle,car_amnt,trans_mete,material_unit,reciepe_no,bet_lev,material_name,material_model,pro_line,operator,customer,project_name,cons_pos,station,concat(isnull(material_name,''),' | ',isnull(material_model,'')) AS mater_info FROM q_produce WHERE org_id in(SELECT child_org_id FROM orgs) AND is_removed = FALSE   and dat_tim between '2017-01-01 00:00:00'  and '2022-05-18 19:15:01' ),produceData AS(SELECT sum(b.plan_amn)plan_amn,sum(b.fact_amnt)fact_amnt,a.pro_line,a.schedule_id,a.org_id,b.material,concat(isnull(b.material_name,''),' | ',isnull(b.material_model,'')) AS mater_info FROM produceTmp as a LEFT JOIN q_dosage b ON b.org_id = a.org_id AND a.pro_line = b.pro_line AND a.schedule_id = b.schedule_id GROUP BY mater_info,a.pro_line,a.schedule_id,a.org_id,material),supplementTmp AS ( SELECT a.id,cast(a.id as string) as schedule_id,a.org_id,dat_tim,vehicle,CAST(isnull(prod_mete,0) AS decimal(28,3)) car_amnt,isnull(labour_name,'')                      labour_name,CAST(isnull(prod_mete,0) AS decimal(28,3)) trans_mete,pro_line,reciepe_no,bet_lev,'' customer,''project_name,a.auditor  AS                                  operator,cons_pos,a.org_name AS                                  station,a.material_name,a.material_unit,a.material_model,concat(isnull(material_name,''),' | ',isnull(material_model,'')) AS mater_info FROM q_manual_supplement as a  WHERE a.org_id IN (SELECT child_org_id FROM orgs) AND a.is_audit = TRUE AND a.is_removed = FALSE AND a.is_productionsystem = TRUE   and dat_tim between '2017-01-01 00:00:00'  and '2022-05-18 19:15:01'  ),supplementData AS (SELECT a.dat_tim,cast(a.id as string) as schedule_id,a.org_id,a.vehicle,a.car_amnt,a.labour_name,a.trans_mete,a.pro_line,'' material_unit,a.reciepe_no,a.bet_lev,'' material_name,'' material_model,a.operator,'' customer,'' project_name,a.cons_pos,a.station,'' material,concat(isnull(b.material_name,''),isnull(b.material_model,'')) AS mater_info,CAST(isnull(b.fact_amnt,0) AS decimal(28,3))plan_amn,CAST(isnull(b.fact_amnt,0) AS decimal(28,3))fact_amnt FROM supplementTmp AS a INNER JOIN q_manual_supplement_item AS b ON a.id = b.order_id AND a.org_id = b.org_id AND b.is_removed = FALSE),itemsData as(select plan_amn,fact_amnt,mater_info,pro_line,org_id,schedule_id,material from produceData union all select plan_amn,fact_amnt,mater_info,pro_line,org_id,schedule_id,material from supplementData),allData as(select org_id,dat_tim,schedule_id,mater_info,labour_name,vehicle,car_amnt,trans_mete,material_unit,reciepe_no,bet_lev,material_name,material_model,pro_line,operator,customer,project_name,cons_pos,station from produceTmp union all select org_id,dat_tim,schedule_id,mater_info,labour_name,vehicle,car_amnt,trans_mete,material_unit,reciepe_no,bet_lev,material_name,material_model,pro_line,operator,customer,project_name,cons_pos,station from supplementTmp),A as(select  pro_line,org_id,schedule_id,sum(case when material='0' and mater_info='普通硅酸盐水泥 | P · O 42.5 散装'  then plan_amn else 0 end) '0|普通硅酸盐水泥 | P · O 42.5 散装|应耗量',sum(case when material='0' and mater_info='普通硅酸盐水泥 | P · O 42.5 散装'  then fact_amnt else 0 end) '0|普通硅酸盐水泥 | P · O 42.5 散装|实耗量',sum(case when material='0-5' and mater_info='碎石 | 石灰岩 5-10'  then plan_amn else 0 end) '0-5|碎石 | 石灰岩 5-10|应耗量',sum(case when material='0-5' and mater_info='碎石 | 石灰岩 5-10'  then fact_amnt else 0 end) '0-5|碎石 | 石灰岩 5-10|实耗量',sum(case when material='0.5' and mater_info='碎石 | 石灰岩 5-10'  then plan_amn else 0 end) '0.5|碎石 | 石灰岩 5-10|应耗量',sum(case when material='0.5' and mater_info='碎石 | 石灰岩 5-10'  then fact_amnt else 0 end) '0.5|碎石 | 石灰岩 5-10|实耗量',sum(case when material='1-2' and mater_info='碎石 | 石灰岩 10-20'  then plan_amn else 0 end) '1-2|碎石 | 石灰岩 10-20|应耗量',sum(case when material='1-2' and mater_info='碎石 | 石灰岩 10-20'  then fact_amnt else 0 end) '1-2|碎石 | 石灰岩 10-20|实耗量',sum(case when material='1-3' and mater_info='碎石 | 石灰岩 10-20'  then plan_amn else 0 end) '1-3|碎石 | 石灰岩 10-20|应耗量',sum(case when material='1-3' and mater_info='碎石 | 石灰岩 10-20'  then fact_amnt else 0 end) '1-3|碎石 | 石灰岩 10-20|实耗量',sum(case when material='2' and mater_info='普通硅酸盐水泥 | P · O 42.5 散装'  then plan_amn else 0 end) '2|普通硅酸盐水泥 | P · O 42.5 散装|应耗量',sum(case when material='2' and mater_info='普通硅酸盐水泥 | P · O 42.5 散装'  then fact_amnt else 0 end) '2|普通硅酸盐水泥 | P · O 42.5 散装|实耗量',sum(case when material='CFB灰渣' and mater_info=' | '  then plan_amn else 0 end) 'CFB灰渣| | |应耗量',sum(case when material='CFB灰渣' and mater_info=' | '  then fact_amnt else 0 end) 'CFB灰渣| | |实耗量',sum(case when material='中粗机制砂' and mater_info='水泥混凝土用机制砂 | Ⅱ类'  then plan_amn else 0 end) '中粗机制砂|水泥混凝土用机制砂 | Ⅱ类|应耗量',sum(case when material='中粗机制砂' and mater_info='水泥混凝土用机制砂 | Ⅱ类'  then fact_amnt else 0 end) '中粗机制砂|水泥混凝土用机制砂 | Ⅱ类|实耗量',sum(case when material='中粗机制砂' and mater_info='水泥混凝土用机制砂 | Ⅰ类'  then plan_amn else 0 end) '中粗机制砂|水泥混凝土用机制砂 | Ⅰ类|应耗量',sum(case when material='中粗机制砂' and mater_info='水泥混凝土用机制砂 | Ⅰ类'  then fact_amnt else 0 end) '中粗机制砂|水泥混凝土用机制砂 | Ⅰ类|实耗量',sum(case when material='减水剂' and mater_info='高效减水剂 | '  then plan_amn else 0 end) '减水剂|高效减水剂 | |应耗量',sum(case when material='减水剂' and mater_info='高效减水剂 | '  then fact_amnt else 0 end) '减水剂|高效减水剂 | |实耗量',sum(case when material='减水剂' and mater_info='普通减水剂 | '  then plan_amn else 0 end) '减水剂|普通减水剂 | |应耗量',sum(case when material='减水剂' and mater_info='普通减水剂 | '  then fact_amnt else 0 end) '减水剂|普通减水剂 | |实耗量',sum(case when material='机制砂' and mater_info='水泥混凝土用机制砂 | Ⅱ类'  then plan_amn else 0 end) '机制砂|水泥混凝土用机制砂 | Ⅱ类|应耗量',sum(case when material='机制砂' and mater_info='水泥混凝土用机制砂 | Ⅱ类'  then fact_amnt else 0 end) '机制砂|水泥混凝土用机制砂 | Ⅱ类|实耗量',sum(case when material='机制砂' and mater_info='水泥混凝土用机制砂 | Ⅰ类'  then plan_amn else 0 end) '机制砂|水泥混凝土用机制砂 | Ⅰ类|应耗量',sum(case when material='机制砂' and mater_info='水泥混凝土用机制砂 | Ⅰ类'  then fact_amnt else 0 end) '机制砂|水泥混凝土用机制砂 | Ⅰ类|实耗量',sum(case when material='水' and mater_info=' | '  then plan_amn else 0 end) '水| | |应耗量',sum(case when material='水' and mater_info=' | '  then fact_amnt else 0 end) '水| | |实耗量',sum(case when material='水泥' and mater_info='普通硅酸盐水泥 | P · O 42.5 散装'  then plan_amn else 0 end) '水泥|普通硅酸盐水泥 | P · O 42.5 散装|应耗量',sum(case when material='水泥' and mater_info='普通硅酸盐水泥 | P · O 42.5 散装'  then fact_amnt else 0 end) '水泥|普通硅酸盐水泥 | P · O 42.5 散装|实耗量',sum(case when material='水泥1' and mater_info='高炉磨细矿渣粉 | S95'  then plan_amn else 0 end) '水泥1|高炉磨细矿渣粉 | S95|应耗量',sum(case when material='水泥1' and mater_info='高炉磨细矿渣粉 | S95'  then fact_amnt else 0 end) '水泥1|高炉磨细矿渣粉 | S95|实耗量',sum(case when material='水泥1' and mater_info='普通硅酸盐水泥 | P · O 42.5 散装'  then plan_amn else 0 end) '水泥1|普通硅酸盐水泥 | P · O 42.5 散装|应耗量',sum(case when material='水泥1' and mater_info='普通硅酸盐水泥 | P · O 42.5 散装'  then fact_amnt else 0 end) '水泥1|普通硅酸盐水泥 | P · O 42.5 散装|实耗量',sum(case when material='水泥2' and mater_info='磨细粉煤灰 | Ⅱ级'  then plan_amn else 0 end) '水泥2|磨细粉煤灰 | Ⅱ级|应耗量',sum(case when material='水泥2' and mater_info='磨细粉煤灰 | Ⅱ级'  then fact_amnt else 0 end) '水泥2|磨细粉煤灰 | Ⅱ级|实耗量',sum(case when material='水泥3' and mater_info='普通硅酸盐水泥 | P · O 42.5 散装'  then plan_amn else 0 end) '水泥3|普通硅酸盐水泥 | P · O 42.5 散装|应耗量',sum(case when material='水泥3' and mater_info='普通硅酸盐水泥 | P · O 42.5 散装'  then fact_amnt else 0 end) '水泥3|普通硅酸盐水泥 | P · O 42.5 散装|实耗量',sum(case when material='液剂1' and mater_info='普通减水剂 | '  then plan_amn else 0 end) '液剂1|普通减水剂 | |应耗量',sum(case when material='液剂1' and mater_info='普通减水剂 | '  then fact_amnt else 0 end) '液剂1|普通减水剂 | |实耗量',sum(case when material='炉渣' and mater_info='CFB炉渣 | CFB灰渣'  then plan_amn else 0 end) '炉渣|CFB炉渣 | CFB灰渣|应耗量',sum(case when material='炉渣' and mater_info='CFB炉渣 | CFB灰渣'  then fact_amnt else 0 end) '炉渣|CFB炉渣 | CFB灰渣|实耗量',sum(case when material='矿渣粉' and mater_info='高炉磨细矿渣粉 | S95'  then plan_amn else 0 end) '矿渣粉|高炉磨细矿渣粉 | S95|应耗量',sum(case when material='矿渣粉' and mater_info='高炉磨细矿渣粉 | S95'  then fact_amnt else 0 end) '矿渣粉|高炉磨细矿渣粉 | S95|实耗量',sum(case when material='矿粉' and mater_info='高炉磨细矿渣粉 | S95'  then plan_amn else 0 end) '矿粉|高炉磨细矿渣粉 | S95|应耗量',sum(case when material='矿粉' and mater_info='高炉磨细矿渣粉 | S95'  then fact_amnt else 0 end) '矿粉|高炉磨细矿渣粉 | S95|实耗量',sum(case when material='砂子' and mater_info='水泥混凝土用机制砂 | Ⅰ类'  then plan_amn else 0 end) '砂子|水泥混凝土用机制砂 | Ⅰ类|应耗量',sum(case when material='砂子' and mater_info='水泥混凝土用机制砂 | Ⅰ类'  then fact_amnt else 0 end) '砂子|水泥混凝土用机制砂 | Ⅰ类|实耗量',sum(case when material='碎石（10-20mm）' and mater_info='碎石 | 石灰岩 10-20'  then plan_amn else 0 end) '碎石（10-20mm）|碎石 | 石灰岩 10-20|应耗量',sum(case when material='碎石（10-20mm）' and mater_info='碎石 | 石灰岩 10-20'  then fact_amnt else 0 end) '碎石（10-20mm）|碎石 | 石灰岩 10-20|实耗量',sum(case when material='碎石（5-10m)' and mater_info=' | '  then plan_amn else 0 end) '碎石（5-10m)| | |应耗量',sum(case when material='碎石（5-10m)' and mater_info=' | '  then fact_amnt else 0 end) '碎石（5-10m)| | |实耗量',sum(case when material='碎石（5-10mm)' and mater_info='碎石 | 石灰岩 5-10'  then plan_amn else 0 end) '碎石（5-10mm)|碎石 | 石灰岩 5-10|应耗量',sum(case when material='碎石（5-10mm)' and mater_info='碎石 | 石灰岩 5-10'  then fact_amnt else 0 end) '碎石（5-10mm)|碎石 | 石灰岩 5-10|实耗量',sum(case when material='碎石（5-10mm）' and mater_info='碎石 | 石灰岩 5-10'  then plan_amn else 0 end) '碎石（5-10mm）|碎石 | 石灰岩 5-10|应耗量',sum(case when material='碎石（5-10mm）' and mater_info='碎石 | 石灰岩 5-10'  then fact_amnt else 0 end) '碎石（5-10mm）|碎石 | 石灰岩 5-10|实耗量',sum(case when material='粉煤灰' and mater_info='磨细粉煤灰 | Ⅱ级'  then plan_amn else 0 end) '粉煤灰|磨细粉煤灰 | Ⅱ级|应耗量',sum(case when material='粉煤灰' and mater_info='磨细粉煤灰 | Ⅱ级'  then fact_amnt else 0 end) '粉煤灰|磨细粉煤灰 | Ⅱ级|实耗量',sum(case when material='粉煤灰（备用）' and mater_info='磨细粉煤灰 | Ⅱ级'  then plan_amn else 0 end) '粉煤灰（备用）|磨细粉煤灰 | Ⅱ级|应耗量',sum(case when material='粉煤灰（备用）' and mater_info='磨细粉煤灰 | Ⅱ级'  then fact_amnt else 0 end) '粉煤灰（备用）|磨细粉煤灰 | Ⅱ级|实耗量',sum(case when material='null' and mater_info=' | '  then plan_amn else 0 end) 'null| | |应耗量',sum(case when material='null' and mater_info=' | '  then fact_amnt else 0 end) 'null| | |实耗量',sum(plan_amn) plan_amn,sum(fact_amnt) fact_amnt from itemsData GROUP BY pro_line,org_id,schedule_id)select B.dat_tim,B.labour_name,B.vehicle,B.car_amnt,B.trans_mete,B.material_unit,B.reciepe_no,B.bet_lev,B.material_name,B.material_model,B.operator,B.customer,B.project_name,B.cons_pos,B.station,A.* from A as A left join allData as B on A.pro_line = B.pro_line and A.org_id=B.org_id and A.schedule_id = B.schedule_id  order by B.dat_tim desc limit 1000000 offset 0
    
```

SQL 1.3.4:

同1.3.3

```sql
/* from:'node-mp-common-service', addr:'10.180.21.157' */
select temp_a.material,temp_a.mater_info from ( WITH orgs AS(SELECT child_org_id FROM global_platform.org_relation WHERE org_id = 1012251829238272  AND child_type = 'project'),produceTmp AS(SELECT org_id,dat_tim,schedule_id,labour_name,vehicle,car_amnt,trans_mete,material_unit,reciepe_no,bet_lev,material_name,material_model,pro_line,operator,customer,project_name,cons_pos,station,concat(isnull(material_name,''),' | ',isnull(material_model,'')) AS mater_info FROM q_produce WHERE org_id in(SELECT child_org_id FROM orgs) AND is_removed = FALSE   and dat_tim between '2018-01-01 00:00:00'  and '2022-05-18 19:10:36' ),produceData AS(SELECT sum(b.plan_amn)plan_amn,sum(b.fact_amnt)fact_amnt,a.pro_line,a.schedule_id,a.org_id,b.material,concat(isnull(b.material_name,''),' | ',isnull(b.material_model,'')) AS mater_info FROM produceTmp as a LEFT JOIN q_dosage b ON b.org_id = a.org_id AND a.pro_line = b.pro_line AND a.schedule_id = b.schedule_id GROUP BY mater_info,a.pro_line,a.schedule_id,a.org_id,material),supplementTmp AS ( SELECT a.id,cast(a.id as string) as schedule_id,a.org_id,dat_tim,vehicle,CAST(isnull(prod_mete,0) AS decimal(28,3)) car_amnt,isnull(labour_name,'')                      labour_name,CAST(isnull(prod_mete,0) AS decimal(28,3)) trans_mete,pro_line,reciepe_no,bet_lev,'' customer,''project_name,a.auditor  AS                                  operator,cons_pos,a.org_name AS                                  station,a.material_name,a.material_unit,a.material_model,concat(isnull(material_name,''),' | ',isnull(material_model,'')) AS mater_info FROM q_manual_supplement as a  WHERE a.org_id IN (SELECT child_org_id FROM orgs) AND a.is_audit = TRUE AND a.is_removed = FALSE AND a.is_productionsystem = TRUE   and dat_tim between '2018-01-01 00:00:00'  and '2022-05-18 19:10:36'  ),supplementData AS (SELECT a.dat_tim,cast(a.id as string) as schedule_id,a.org_id,a.vehicle,a.car_amnt,a.labour_name,a.trans_mete,a.pro_line,'' material_unit,a.reciepe_no,a.bet_lev,'' material_name,'' material_model,a.operator,'' customer,'' project_name,a.cons_pos,a.station,'' material,concat(isnull(b.material_name,''),isnull(b.material_model,'')) AS mater_info,CAST(isnull(b.fact_amnt,0) AS decimal(28,3))plan_amn,CAST(isnull(b.fact_amnt,0) AS decimal(28,3))fact_amnt FROM supplementTmp AS a INNER JOIN q_manual_supplement_item AS b ON a.id = b.order_id AND a.org_id = b.org_id AND b.is_removed = FALSE),itemsData as(select plan_amn,fact_amnt,mater_info,pro_line,org_id,schedule_id,material from produceData union all select plan_amn,fact_amnt,mater_info,pro_line,org_id,schedule_id,material from supplementData),allData as(select org_id,dat_tim,schedule_id,mater_info,labour_name,vehicle,car_amnt,trans_mete,material_unit,reciepe_no,bet_lev,material_name,material_model,pro_line,operator,customer,project_name,cons_pos,station from produceTmp union all select org_id,dat_tim,schedule_id,mater_info,labour_name,vehicle,car_amnt,trans_mete,material_unit,reciepe_no,bet_lev,material_name,material_model,pro_line,operator,customer,project_name,cons_pos,station from supplementTmp),A as(select  pro_line,org_id,schedule_id,material,mater_info,sum(plan_amn) plan_amn,sum(fact_amnt) fact_amnt from itemsData GROUP BY pro_line,org_id,schedule_id,material,mater_info)select B.dat_tim,B.labour_name,B.vehicle,B.car_amnt,B.trans_mete,B.material_unit,B.reciepe_no,B.bet_lev,B.material_name,B.material_model,B.operator,B.customer,B.project_name,B.cons_pos,B.station,A.* from A as A left join allData as B on A.pro_line = B.pro_line and A.org_id=B.org_id and A.schedule_id = B.schedule_id  order by B.dat_tim desc limit 1000000 offset 0 ) as temp_a group by temp_a.material,temp_a.mater_info order by temp_a.material,temp_a.mater_info desc
```

SQL 1.3.5:

orgs 嵌入 productData

```sql
/* from:'node-ca-concrete-service', addr:'10.180.38.176' */
with orgs as ( -- 实际,标准,偏差
      select child_org_id from global_platform.org_relation where org_id= 814036348678608 and child_type='project'
    ),
    productData as ( -- 拌合站机楼生产记录
        select  
         isnull(material_id,0)material_id,
         isnull(material_name,'')material_name,
         isnull(material_model,'')material_model,
         isnull(material_unit,'')material_unit,
         isnull(plan_amn,0) plan_amn,
         isnull(fact_amnt,0) fact_amnt 
          from q_dosage
          where org_id in(select child_org_id from orgs)
         
          and is_removed=false and dat_tim>='2020-01-01 00:00:00' and dat_tim<= '2022-05-18 11:01:07'
    ),
     supplementData as(
        SELECT isnull(b.material_id, 0) AS material_id, 
              isnull(b.material_name, '') AS material_name, 
              isnull(b.material_model, '') AS material_model, 
              isnull(b.material_unit, '') AS material_unit , 
              isnull(fact_amnt,0)plan_amn, -- 标准
              isnull(fact_amnt,0)fact_amnt  -- 实际
        from q_manual_supplement as a
        inner join q_manual_supplement_item as b
        on a.id=b.order_id
        where a.org_id IN (SELECT child_org_id FROM orgs) and a.is_productionsystem = true 
        and a.is_audit=true 
        and dat_tim>='2020-01-01 00:00:00' and dat_tim<= '2022-05-18 11:01:07'
        and a.is_removed=false and b.is_removed=false
          

        union all
        
        -- 手工下料数据
        SELECT isnull(material_id, 0) AS material_id, 
              isnull(material_name, '') AS material_name, 
              isnull(material_model, '') AS material_model, 
              isnull(material_unit, '') AS material_unit , 
              isnull(fact_amnt,0)plan_amn, -- 标准
              isnull(fact_amnt,0)fact_amnt  -- 实际
        FROM q_manual 
        where is_removed=false
        and dat_tim>='2020-01-01 00:00:00' and dat_tim<= '2022-05-18 11:01:07'
        and org_id in (select child_org_id from orgs)
        
     ),
        
      result as (
        select material_id,material_name,trim(material_model)material_model,material_unit,
          CAST(isnull(SUM(fact_amnt), 0)/1000 AS decimal(28, 3))fact_amnt,-- 实际
          CAST(isnull(SUM(plan_amn), 0)/1000 AS decimal(28, 3))plan_amn,-- 标准
          cast((sum(fact_amnt) - sum(plan_amn))/1000 as decimal(28,3)) deviation
        from(
          select material_id,material_name,material_model,material_unit,fact_amnt,plan_amn from productData
          union all
          select material_id,material_name,material_model,material_unit,fact_amnt,plan_amn from supplementData
        ) as a
        group by material_id,material_name,trim(material_model),material_unit
      )
        select * from result order by material_name
```

SQL 1.3.6：

s_material嵌入dosage语句块，避免把几千万条数据从q_dosage取出来

```mysql
/* from:'node-mr-mquantity-product-service', addr:'10.180.59.223' */
with 
    -- 指定统计类下挂接的材料id
    s_material as (
      select distinct material_id from g_statistic_category_material
      where statistic_id = 450949003816960
      and is_removed=false
    ),
    
    -- 查询开累计划量
    kl_plan as (
      select material_id,material_unit,sum(isnull(quantity,0)) quantity from m_gh_plan_check
      where org_id in (...)
      and is_removed=false
      and is_audit = true
      and gh_id is not null
      and material_id in (
        select material_id from s_material
      )
      group by material_id,material_unit
    ),
    -- 本期收料量
    receive as (
      select material_id,material_unit,sum(quantity) receive_quantity from (
        select material_id,material_unit,isnull(case when service_type*order_type >0 then net_quantity else -net_quantity end,0) quantity 
        from q_receive_more_material
        where org_id in (...)
        and is_removed=false
        and order_id in (
          select id from q_receive
          where org_id in (...)
          and (order_origin=0 or (order_origin!=0 and is_audit=true))
          and exit_time>= '1900-01-01 00:00:00' and exit_time<= '2022-06-01 17:20:46'
          and is_removed=false
        )
        and material_id in (
          select material_id from s_material
        )
        union all
        select material_id,material_unit,isnull(case when service_type*order_type >0 then net_quantity else -net_quantity end,0) quantity 
        from q_receive_more_material_temp_data
        where org_id in (...)
        and is_removed=false
        and order_id in (
          select id from q_receive_temp_data
          where org_id in (...)
          and exit_time>= '1900-01-01 00:00:00' and exit_time<= '2022-06-01 17:20:46'
          and is_removed=false
        )
        and material_id in (
          select material_id from s_material
        )
        and is_marched=false
        union all
        select material_id,material_unit,isnull(b.quantity,0) quantity  
        from q_waste a 
        inner join q_waste_item b
        on a.org_id=b.org_id and a.id=b.order_id
        where a.org_id in (...) 
        and a.recycle_date>= '1900-01-01 00:00:00' and recycle_date<= '2022-06-01 17:20:46'
        and is_audit=1  
        and a.is_removed=false  
        and b.is_removed=false
        and material_id in (
          select material_id from s_material
        )
      ) a
      group by material_id,material_unit
      ),
      -- 本期发料数据
      delivery as (
        select material_id,material_unit,sum(quantity) delivery_quantity from (
          select material_id,material_unit,isnull(case when service_type*order_type>0 then net_quantity else -net_quantity end,0) quantity
          from q_delivery_more_material
          where org_id in (...)
          and is_removed=false
          and order_id in (
            select id from q_delivery
            where org_id in (...)
            and (order_origin=0 or (order_origin!=0 and is_audit=true))
            and exit_time>= '1900-01-01 00:00:00' and exit_time<= '2022-06-01 17:20:46'
            and is_removed=false
            and id not in (
              select id from q_delivery_weight 
              where org_id in (...)
              and isnull(weight_type,'')='进料过磅'
              )
          )
          and material_id in (select material_id from s_material)
          union all
          select material_id,material_unit,isnull(case when service_type*order_type>0 then net_quantity else -net_quantity end,0) quantity
          from q_delivery_more_material_temp_data
          where org_id in (...)
          and is_removed=false
          and order_id in (
            select id from q_delivery_temp_data
            where org_id in (...)
            and exit_time>= '1900-01-01 00:00:00' and exit_time<= '2022-06-01 17:20:46'
            and is_removed=false
            and isnull(weight_type,'')!='进料过磅'
          )
          and material_id in (select material_id from s_material)
          and is_marched=false
        ) a
        group by material_id,material_unit
      ),
      -- 本期机楼消耗数据 + 手工下料 + 补录
      dosage as (
        select material_id,material_unit,sum(isnull(fact_amnt,0)) fact_amnt from (
        select material_id,'吨' material_unit,sum(isnull(fact_amnt,0))/1000 fact_amnt
          from q_dosage
          where org_id in (...)
          and is_removed=false
          and material_id in (select material_id from s_material)
          and dat_tim >= '1900-01-01 00:00:00' and dat_tim <= '2022-06-01 17:20:46'
          group by material_id
          union all
          select material_id,'吨' material_unit,sum(isnull(fact_amnt,0))/1000 fact_amnt 
          from q_manual
          where org_id in (...) 
          and dat_tim>= '1900-01-01 00:00:00'  and dat_tim<= '2022-06-01 17:20:46' 
          and is_removed=false
          and material_id in (select material_id from s_material)
          group by material_id
          union all
          select b.material_id,'吨' material_unit,sum(isnull(fact_amnt,0))/1000 fact_amnt 
          from q_manual_supplement as a
          inner join q_manual_supplement_item b 
          on a.org_id=b.org_id and a.id=b.order_id
          where a.org_id in (...)
          and b.org_id in (...)
          and a.is_audit=true
          and a.is_productionsystem = true 
          and dat_tim>= '1900-01-01 00:00:00' and dat_tim<= '2022-06-01 17:20:46'
          and b.material_id in (select material_id from s_material)
          and a.is_removed=false 
          and b.is_removed=false
          group by b.material_id
          ) a
          group by material_id,material_unit
      ), 


      -- 合并材料信息
      all_material as(
        select material_id, material_unit,receive_quantity,delivery_quantity,fact_amnt,type from (
          select material_id,material_unit,receive_quantity,0 delivery_quantity,0 fact_amnt,'receive' type from receive
          union all
          select material_id,material_unit,0 receive_quantity,delivery_quantity,0 fact_amnt,'delivery' type from delivery
          
             union all 
             select material_id,material_unit,0 receive_quantity,0 delivery_quantity,fact_amnt,'dosage' type from dosage
             
        ) a
      ),
      convert_quantity as (
        select sum(receive_quantity/isnull(conversion_factor,1)) receive_quantity,
        sum(delivery_quantity/isnull(conversion_factor,1)) delivery_quantity,
        sum(fact_amnt/isnull(conversion_factor,1)) fact_amnt
        from all_material a
        left join(
          select distinct statistic_id,conversion_unit,
                 case isnull(conversion_factor,0) when 0 then 1  else conversion_factor end conversion_factor
            from g_statistic_category_unit
          where statistic_id = 450949003816960 and is_removed=false
        ) b
        on a.material_unit = b.conversion_unit
      ),
      kl_quantity as (
        select sum(quantity/isnull(conversion_factor,1)) plan_quantity
        from kl_plan a
        left join (
          select distinct statistic_id,conversion_unit,
          -- conversion_factor
           case isnull(conversion_factor,0) when 0 then 1 else conversion_factor end conversion_factor
           from g_statistic_category_unit
          where statistic_id = 450949003816960 and is_removed=false
        ) b
        on a.material_unit = b.conversion_unit
      )
      select round(isnull(a.receive_quantity,0),0) receive_quantity,
      round(isnull(a.delivery_quantity,0),0) mq_delivery_quantity,
      round(isnull(a.fact_amnt,0),0) fact_amnt,
      round(isnull(b.plan_quantity,0),0) plan_quantity,
      round(isnull(a.delivery_quantity,0)+isnull(a.fact_amnt,0),0) delivery_quantity
      from convert_quantity a, kl_quantity b
   
```



## 1.4 cast返回结果不同

SQL 1.4.1：

cast(str as double) ，在impala里返回null，在tidb里返回0，而且在有的场景下，会报错

这个SQL看了一下表结构，有value_type字段可以来判断是否数字，把这一行改为用value_type判断

AND cast(pewp.property_value as double) is not null

```sql
/* from:'isubcontract-service', addr:'10.180.59.139' */
 INSERT INTO project_procedure_quantity_param (
       org_id,
       id,
       project_procedure_quantity_id,
       name,
       value,
       remark,
       param_type,
       entry_work_feature_id,
       creator,
       created_at,
       reviser,
       updated_at,
       version,
       is_removed
     )
       SELECT 
         ppq.org_id,
         __fn.sequence() id,
         ppq.id project_procedure_quantity_id,
         pewp.name,
         cast(pewp.property_value as double) value,
         null remark,
         'component' param_type,
         null,
         1236815222224896 creator,
         NOW() created_at,
         1236815222224896 reviser,
         NOW() updated_at,
         1275549184734208 version,
         false
       FROM project_procedure_quantity ppq
       JOIN project_entry_work_property pewp
         ON pewp.org_id = ppq.org_id
         AND pewp.project_unit_work_id = ppq.project_unit_work_id
         AND pewp.project_entry_work_id = ppq.project_entry_work_id
         AND pewp.is_removed = false
         AND cast(pewp.property_value as double) is not null
       WHERE ppq.org_id = 1180669010630144
         AND ppq.version = 1275549184734208
         AND ppq.procedure_ref_id = 1274801349745665
         AND ppq.is_removed = false
         AND ppq.parent_id = -1
```

## 1.5 性能问题

有一些sql，在tidb中执行计划有毛病，需要做一些改写

SQL 1.5.1：

这个SQL写的确实不够合理，impala能处理好，tidb执行就很慢，这里：

AND p.project_bill_quantity_id IN (
      SELECT project_bill_quantity_id 

加一个distinct，记录条数从几千条变成2条

```mysql
/* from:'cq3-cq-service', addr:'10.180.253.168' */
WITH bill_quantity AS (
  SELECT
    p.id,
    IF(p.is_leaf = false, null, ROUND(SUM(pq.review_quantity * IFNULL(pq.bill_quantity_factor, 1)), 3)) AS review_quantity,
    IF(p.is_leaf = false, null, ROUND(SUM(ROUND(pq.review_quantity * IFNULL(pq.bill_quantity_factor, 1),3) * c.price), 3)) AS review_price
  FROM global_ipm.project_bill_quantity_detail AS p
  JOIN global_ipm.project_bill_quantity_detail AS c
    ON c.is_removed = false
    AND c.org_id = p.org_id
    AND c.is_leaf = true
    AND c.project_bill_quantity_id = p.project_bill_quantity_id
    AND LEFT(c.full_id_ex, LENGTH(p.full_id_ex)) = p.full_id_ex
  LEFT JOIN project_quantity AS pq
    ON pq.is_removed = false
    AND pq.org_id = 1242235052178432
    AND pq.project_unit_work_id = 1246538536090113
    AND pq.project_bill_quantity_detail_id = c.id
  WHERE p.is_removed = false
    AND p.org_id = 1242235052178432
    AND p.project_bill_quantity_id IN (
      SELECT project_bill_quantity_id 
      FROM project_quantity
      WHERE org_id = 1242235052178432
        AND is_removed = false
        AND project_unit_work_id = 1246538536090113
    )
  GROUP BY p.id, p.is_leaf
)
SELECT
  pbqd.id,
  pbqd.parent_id,
  pbqd.name,
  pbqd.code,
  pbqd.unit,
  pbqd.price,
  bq.review_quantity,
  bq.review_price,
  bq.review_quantity AS reside_quantity,
  bq.review_price AS reside_price
FROM global_ipm.project_bill_quantity_detail AS pbqd
JOIN bill_quantity AS bq
  ON bq.id = pbqd.id
WHERE pbqd.is_removed = false
  AND pbqd.org_id = 1242235052178432
ORDER BY pbqd.level, pbqd.order_no
```

SQL 1.5.2:

这段SQL写的逻辑混乱，导致执行计划乱七八糟

这个子句里的where条件毫无必要，而且有个is_removed条件，在CTE：qLabourAllot中并无此列

and item_bar_code in (select item_bar_code from qLabourAllot  where org_id= 749816950517760 and order_id = 1275015682283008 and is_removed=false    and material_name like '%%'  and material_model like '%%'  )

```mysql
/* from:'node-mq-mquantity-service', addr:'10.180.137.85' */
with qLabourAllot as (
               select id , order_id , org_id , material_id,ori_material_id, material_code, material_name, material_model,material_unit,quantity,
                      class_id,ori_class_id,class_full_id, ori_org_id , item_bar_code,sort_code,auxiliary_unit from q_labour_allot_item
                      where org_id= 749816950517760 and order_id = 1275015682283008 and is_removed=false    and material_name like '%%'  and material_model like '%%'  )
              SELECT a.*,b.manufacturer,b.batch_no, b.test_report_no, b.storage_place,b.skill_card_no,b.remark
              FROM qLabourAllot a
               left join
               (select manufacturer,batch_no, test_report_no, storage_place,skill_card_no, auxiliary_unit, item_bar_code,remark
                from q_receive_more_material WHERE  org_id= 749816950517760 and is_removed=false and service_type>0 AND order_type=4 
                and item_bar_code in (select item_bar_code from qLabourAllot  where org_id= 749816950517760 and order_id = 1275015682283008 and is_removed=false    and material_name like '%%'  and material_model like '%%'  ))b
                on a.item_bar_code = b.item_bar_code
                order by a.sort_code
```

SQL 1.5.3

这个跟SQL2问题一样，应该是拷贝的

```mysql
/* from:'node-mq-mquantity-service', addr:'10.180.125.199' */
with delivery as (
               select id , order_id , org_id , material_id,ori_material_id, material_code, material_name, material_model,material_unit,net_quantity,class_id,ori_class_id,class_full_id, ori_org_id , item_bar_code,sort_code,auxiliary_unit, remark from q_delivery_more_material
                      where org_id= 873711305765376 and order_id = 1284897477776384 and is_removed=false    and material_name like '%%'  and material_model like '%%'  )
              SELECT a.*,b.manufacturer,b.batch_no, b.test_report_no, b.storage_place,b.skill_card_no,a.remark,b.remark item_remark,b.service_type,b.order_type,b.quality_certificate,b.tax_free_price,b.tax_free_sum
              FROM delivery a
               left join
               (select manufacturer,batch_no, test_report_no, storage_place,skill_card_no, auxiliary_unit, item_bar_code,service_type,order_type,quality_certificate,remark,tax_free_price,tax_free_sum
                from q_receive_more_material WHERE  org_id= 873711305765376 and is_removed=false and service_type>0 AND order_type=4
                and item_bar_code in (select item_bar_code from delivery  where org_id= 873711305765376 and order_id = 1284897477776384 and is_removed=false    and material_name like '%%'  and material_model like '%%'  ))b 
                on a.item_bar_code = b.item_bar_code
                order by a.sort_code
```

SQL 1.5.4:

这个SQL写的本身没毛病，但是很依赖引擎的优化能力

 from project_bill_quantity_detail as bc
  join project_bill_quantity_detail as bp

在tidb的执行计划里，这两个表的join，发生在bc的in条件应用之前了

因为in本身也会翻译成join，哪个join先执行不同引擎会有不同策略

建议把SQL改写为，bc连带其条件，再放入一个with语句块中

```mysql
/* from:'iquantity-template-service', addr:'10.180.38.79' */
with bill_detail as (
  select distinct
    b.project_bill_quantity_id
    , q.project_bill_quantity_detail_id
    , b.org_id
  from cq_quantity_resume_part_quantity as q
  join project_bill_quantity_detail as b
    on b.is_removed = false
    and b.id = q.project_bill_quantity_detail_id
  where q.is_removed = false
    and q.org_id = 795688512802816
    and q.quantity_resume_part_id = 1253560065365437
)
select
  t.id
  , t.parent_id
  , t.full_id
  , t.code
  , t.name
  , t.unit
from (
  select
    b.id
    , -1 as parent_id
    , cast(b.id as string) as full_id
    , ifnull(b.code, '') as code
    , ifnull(b.name, '') as name
    , null as unit
    , 1 as level
    , b.id as order_no
  from project_bill_quantity as b
  where b.is_removed = false
    and b.id in (select project_bill_quantity_id from bill_detail)

  union all

  select distinct
    bp.id
    , if(bp.parent_id = -1, bp.project_bill_quantity_id, bp.parent_id) as parent_id
    , concat_ws('|', cast(bp.project_bill_quantity_id as string), bp.full_id) as full_id
    , ifnull(bp.code, '') as code
    , ifnull(bp.name, '') as name
    , bp.unit
    , 2 as level
    , bp.order_no
  from project_bill_quantity_detail as bc
  join project_bill_quantity_detail as bp
    on bp.is_removed = false
    and bp.org_id = bc.org_id
    and bp.project_bill_quantity_id = bc.project_bill_quantity_id
    and locate(bp.full_id, bc.full_id) > 0
  where bc.is_removed = false
    and bc.org_id in (select org_id from bill_detail)
    and bc.id in (select project_bill_quantity_detail_id from bill_detail)
) as t
order by
  t.level
  , t.order_no
;
```

SQL 1.5.5:

d_date表数据量很大，join的时候加上org_id的关联条件

```
select temp.id,
           temp.org_id,
           temp.editor,
           temp.reviewer,
           temp.stat_year,
           temp.stat_month,
           temp.stat_month_start,
           temp.stat_date,
           temp.complete_amount,
           temp.amount,
           sum(daily_task_detail.plan_amount) plan_amount_other,
           sum(daily_task_detail.complete_amount) complete_amount_other
from(
with plan as( select    quarter_plan.org_id,
                      quarter_plan.year,
                      quarter_plan_detail.month,
                      ifnull(sum(quarter_plan_detail.company_amount),0) as amount
           from       ys2_custom.crfeb_company_quarter_plan quarter_plan
           INNER JOIN ys2_custom.crfeb_company_quarter_plan_detail quarter_plan_detail on quarter_plan.id=quarter_plan_detail.quarter_plan_id
           INNER JOIN global_platform.org_relation org                                         on org.child_org_id=quarter_plan_detail.project_id
           INNER JOIN global_ipm.org_year_extras_info extras_info                              on extras_info.org_id=org.child_org_id and extras_info.year=quarter_plan.year and extras_info.in_hand=true and extras_info.is_removed=false
           where      quarter_plan.is_removed=false and quarter_plan_detail.is_removed=false and org.child_ext_type="project" and org.org_id=5336179959952360 and org.top_project=true and quarter_plan.status=1
           group by   quarter_plan.org_id,
                      quarter_plan.year,
                      quarter_plan_detail.month )
select     month_task.id,
           month_task.org_id,
           month_task.editor,
           month_task.reviewer,
           da.stat_year,
           da.stat_month,
           da.stat_month_start,
           month_task.stat_date,
           sum(daily.complete_amount)    complete_amount,
           plan.amount
from       global_platform.org_relation org
INNER JOIN ys2_custom.crfeb_project_month_task month_task               on org.org_id=month_task.org_id and month_task.is_removed=false
INNER JOIN global_dwb.d_date da                                                 on da.id=concat('pr-c-',REPLACE(LEFT(CAST(month_task.stat_date as string),10),'-',''),'-',CAST(5336179959952360 as string))
INNER JOIN global_ipm.org_year_extras_info extras_info                          on extras_info.org_id=org.child_org_id and extras_info.year=da.stat_year and extras_info.in_hand=true and extras_info.is_removed=false
LEFT JOIN ys2_custom.crfeb_project_daily_report daily                  on org.child_org_id=daily.org_id and daily.is_removed=false and LEFT(CAST(daily.stat_date as string),10)>=da.stat_month_start and LEFT(CAST(daily.stat_date as string),10)<=month_task.stat_date
LEFT JOIN  plan                                                                 on plan.org_id=month_task.org_id and plan.year=da.stat_year and plan.month=da.stat_month
where      org.org_id=5336179959952360
and org.child_ext_type="project" and org.top_project=true
group by   month_task.id,
           month_task.org_id,
           month_task.editor,
           month_task.reviewer,
           da.stat_year,
           da.stat_month,
            da.stat_month_start,
           month_task.stat_date,
           plan.amount)temp
 LEFT JOIN  ys2_custom.crfeb_company_dispatch_daily  daily_task_detail 
on  daily_task_detail.org_id= temp.org_id and daily_task_detail.is_removed=false 
and LEFT(CAST(daily_task_detail.stat_date as string),10)>=temp.stat_month_start
and LEFT(CAST(daily_task_detail.stat_date as string),10)<=temp.stat_date     
where 1=1
group by temp.id,
           temp.org_id,
           temp.editor,
           temp.reviewer,
           temp.stat_year,
           temp.stat_month,
           temp.stat_month_start,
           temp.stat_date,
           temp.complete_amount,
           temp.amount
order by   temp.stat_date desc
/*& tenant:ys2 */
/*& $replace:tenant */
```

SQL 1.5.6：

tidb对这个SQL优化的不好，扫描记录数太多

在project_quantity_formula上补条件使能命中主键索引：

where \_\_t.org_id in (612846781755491) and \_\_t.id in (1786706395246, 1786706395248, ......)

```mysql
/* from:'iquantity-jit-stats-service', addr:'10.180.12.148' */
UPDATE __t SET
__t.`result_formula` = __tmp.`result_formula`
FROM project_quantity_formula AS __t
INNER JOIN (
VALUES (1786706395246 AS `id`, 612846781755491 AS `org_id`, '41.76 * 1' AS `result_formula`),
(1786706395248, 612846781755491, '3.1 * 8 * 41 / 40'),
(1786706395252, 612846781755491, '41.76 * 1'),
...
(1786706395298, 612846781755491, '41.76 * 1'),
(1786706395299, 612846781755491, '3 * 8 * 1'),
(1786706395303, 612846781755491, '41.76 * 1')
) AS __tmp ON __t.`id`=__tmp.`id` AND __t.`org_id`=__tmp.`org_id`
```

SQL 1.5.7:

同上

```mysql
UPDATE __t
SET __t.`guid` = __tmp.`guid`, __t.`project_unit_work_id` = __tmp.`project_unit_work_id`, __t.`project_quantity_id` = __tmp.`project_quantity_id`, 
  __t.`name` = __tmp.`name`, __t.`unit` = __tmp.`unit`, __t.`order_no` = __tmp.`order_no`, __t.`formula` = __tmp.`formula`, __t.`digit` = __tmp.`digit`, 
  __t.`result` = __tmp.`result`, __t.`result_formula` = __tmp.`result_formula`, __t.`is_result` = __tmp.`is_result`, __t.`remark` = __tmp.`remark`, 
  __t.`creator` = __tmp.`creator`, __t.`reviser` = __tmp.`reviser`, __t.`version` = __tmp.`version`, __t.`updated_at` = __tmp.`updated_at`
FROM
  global_cq3.project_quantity_formula AS __t
  INNER JOIN (
      VALUES
        (
          1204793257497600 AS `org_id`, 1301767944615425 AS `id`, 'c7e36ab83a544aaf99b6e11f41d5fcfd' AS `guid`, 1301701571632080 AS `project_unit_work_id`, 1301767943896065 AS `project_quantity_id`, '#L1' AS `name`, null AS `unit`, 1 AS `order_no`, '1179' AS `formula`, 'double3' AS `digit`, 1179 AS `result`, '1179' AS `result_formula`, true AS `is_result`, null AS `remark`, 809443201651152 AS `creator`, 809443201651152 AS `reviser`, 1301796509127680 AS `version`, '2022-06-25 12:32:01.787000' AS `updated_at`
        ),  
        ......
        (
          1204793257497600, 1301767944615439, '9b2fcb3f614d4c7d9589f89f6ed976f5', 1301701571632080, 1301767943896079, '#L1', null, 1, '20', 'double3', 20, '20', true, null, 809443201651152, 809443201651152, 1301796509127680, '2022-06-25 12:32:01.787000'
        )
      ) AS __tmp
      ON __t.`org_id` = __tmp.`org_id`
      AND __t.`id` = __tmp.`id`
```



# 2. SQL语法错误

impala语法检查相当宽松，有一些SQL写的不太妥当，在impala里能执行（但可能是隐藏的bug），在tidb里会报错

## 2.1 没有limit有offset

去掉offset

SQL 2.1.1

```mysql
/* from:'node-mq2-rds-service', addr:'10.180.137.1' */
SELECT `id`, `org_id`, `order_id`, `labour_id`, `labour_name`, `gh_id`, `gh_name`, `gh_full_id`, `gh_full_name`, `ori_gh_id`, `material_id`, `material_code`, `material_name`, `material_model`, `material_unit`, `auxiliary_unit`, `class_id`, `class_full_id`, `inventory_quantity`, `net_quantity`, `has_sale`, `residue_quantity`, `last_check_quantity`, `last_labour_rest_quantity`, `labour_consume_quantity`, `diff_quantity`, `receive_quantity`, `delivery_quantity`, `is_check`, `price`, `item_remark`, `ori_org_id`, `ori_material_id`, `ori_labour_id`, `ori_common_id`, `ori_class_id`, `ori_item_id`, `ori_order_id`, `sort_code`, `is_removed`, `creator_id`, `creator_name`, `created_at`, `modifier_id`, `modifier_name`, `updated_at`, `version`, `storage_place`, `adjustment_reasons` 
FROM `q_check_store_item` AS `qCheckStoreItem` 
WHERE `qCheckStoreItem`.`org_id` = 1229202466600936 AND `qCheckStoreItem`.`order_id` = 1268412608460728 AND `qCheckStoreItem`.`is_removed` = false AND `qCheckStoreItem`.`is_check` = true ORDER BY `qCheckStoreItem`.`id` 
OFFSET 0;
```

## 2.2 没有聚合函数写了group

没看懂group的意义何在，如果是要去重复记录用distinct

SQL 2.2.1：

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

SQL 2.2.2：

这个SQL跟SQL 2.2.1看起来一样，但是服务名变化了

```mysql
/* from:'node-ca2-report-service', addr:'10.180.180.214' */
with lopsum as
    (
      select  concat( material,cast( isnull(material_id,0) as string)) col_name,material,isnull(material_id,0)material_id,material_name,material_model,auxiliary_unit  
      from q_manual
      where org_id=1235443508916664  and dat_tim>= '2022-05-01 00:00:00' and dat_tim<='2022-05-30 16:46:32'
      group by material,isnull(material_id,0),material_name,material_model,auxiliary_unit
    ),
    bulusum as
    (
      select concat( b.material,cast( isnull(b.material_id,0) as string)) col_name,b.material,isnull(b.material_id,0)material_id,b.material_name,b.material_model,b.material_unit
      from q_manual_supplement as a
      inner join q_manual_supplement_item b 
      on a.id=b.order_id
      where a.org_id=1235443508916664 and a.is_audit=true and dat_tim>= '2022-05-01 00:00:00' and dat_tim<='2022-05-30 16:46:32' and a.is_removed=false and b.is_removed=false
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



## 2.3 没有group写了having

没有group写什么having，用where不香吗

SQL 2.3.1:

```sql
/* from:'node-mq2-module-custom-service', addr:'10.180.37.3' */
with orgs as(
                    select child_org_id from global_platform.org_relation where org_id = 1270636408607720
                ),
                monthQuantity as(
                    SELECT -- 月需计划材料
                        isnull(sum(b.quantity),0) as quantity,
                        a.org_id,b.material_id,b.material_model,b.material_code,b.material_name,b.material_unit
                        from(
                            SELECT id,org_id from q_plan_month WHERE is_removed=FALSE AND is_submit=TRUE AND org_id in(select child_org_id from orgs)
                    ) as a
                    join q_plan_month_item as b
                    on a.org_id = b.org_id and a.id = b.order_id
                    WHERE b.is_removed=FALSE
                    GROUP BY a.org_id,b.material_id,b.material_model,b.material_code,b.material_name,b.material_unit
                ),
                supplyQuantity as(
                    SELECT -- 供应计划材料
                        isnull(sum(b.quantity),0) as quantity,
                        a.org_id,b.material_id,b.material_model,b.material_code,b.material_name,b.material_unit
                        from(
                            SELECT id,org_id from q_purchase_plan WHERE is_removed=FALSE AND org_id in(select child_org_id from orgs) AND module_type=1
                    ) as a
                    join q_purchase_plan_item as b
                    on a.org_id = b.org_id and a.id = b.order_id
                    WHERE b.is_removed=FALSE
                    GROUP BY a.org_id,b.material_id,b.material_model,b.material_code,b.material_name,b.material_unit
                ),
                result as(
                  select 
                  cast(a.quantity as decimal(28,4)) as plan_quantity,-- 计划量
                  cast(isnull(b.quantity,0) as decimal(28,4)) as purchase_quantity, -- 已采购量
                  cast((a.quantity - isnull(b.quantity,0)) as decimal(28,4)) as need_quantity, -- 剩余购量
                  
                  a.org_id,a.material_id,a.material_model,a.material_code,a.material_name,a.material_unit
                  from monthQuantity as a
                  left join supplyQuantity as b
                  on a.org_id = b.org_id and a.material_id = b.material_id
                  where 1 =1 
                  
                  
                  having (a.quantity - isnull(b.quantity,0))>0 -- 剩余量大于0
                )
                select isnull(b.inventory_quantity,0)inventory_quantity,a.* from result as a
                left join (
                    select 
                      material_id, org_id,
                      sum(isnull(quantity,0)) inventory_quantity -- 实时库存
                    from q_inventory
                        where org_id = 1270636408607720 and is_removed=false
                    group by material_id, org_id
                ) as b on a.org_id = b.org_id and a.material_id = b.material_id
                order by a.need_quantity desc
```

SQL 2.3.2:

```mysql
/* from:'sc-api-service', addr:'10.180.59.153' */
SELECT
  plt.contractor_id,
  CASE WHEN plt.contractor_id IS NOT NULL THEN
    CASE WHEN c.original_id IS NOT NULL THEN
      oc.company_name
    ELSE
      c.company_name
    END
  ELSE plt.company_name
  END AS contractor_name,
  NULL AS integrated_contractor_id,
  NULL AS raw_contractor_id,
  ps.id AS contract_id,
  ps.subcontract_no AS contract_no,
  plt.id AS team_id,
  plt.name AS team_name,
  ps.contract_signer AS leader_name,
  ssvi.procedure_ref_id AS cost_item_id,
  sp.name AS cost_item_name,
  ssvi.project_unit_work_id AS project_wbs_id,
  ssvi.entry_work_id,
  NULL AS project_part_id,
  NULL AS cost_type_id,
  sp.unit AS measure_unit,
  IF(ssvi.is_temporary, 'temporaryConstructionCost', 'laborCost') AS cost_type_system_code,
  ssvi.price AS unit_price ,
  ssvi.price_without_tax AS unit_price_non_tax,
  ROUND((ssvi.quantity-ssvi.monthly_quantity), 6) AS quantity,
  ROUND((ssvi.quantity-ssvi.monthly_quantity) * ssvi.price, 2) AS amount,
  ROUND((ssvi.quantity-ssvi.monthly_quantity) * ssvi.price - ssvi.quantity  * ssvi.price_without_tax, 2) AS tax,
  ROUND((ssvi.quantity-ssvi.monthly_quantity) , 6) AS valuation_quantity,
  ROUND((ssvi.quantity-ssvi.monthly_quantity)  * ssvi.price, 2) AS valuation_amount,
  ROUND((ssvi.quantity-ssvi.monthly_quantity)  * ssvi.price_without_tax, 2) AS valuation_tax,
  NULL AS deducted_amount,
  NULL AS deducted_tax,
  ssv.settlement_date as valuation_end_date
FROM project_subcontract ps
JOIN project_labor_team plt
  ON plt.id = ps.team_id
  AND plt.org_id = ps.org_id
  AND plt.is_removed = FALSE
JOIN sc_settlement_valuation ssv
    on ssv.subcontract_id = ps.id
JOIN sc_settlement_valuation_item ssvi
    ON ssvi.valuation_id = ssv.id
    AND ssvi.org_id = ssv.org_id
    AND ssvi.is_removed = FALSE
JOIN project_subcontract_procedure_ref pspr
  ON pspr.id = ssvi.procedure_ref_id
  AND pspr.org_id = ps.org_id
  AND pspr.is_removed = FALSE
JOIN subcontract_procedure sp
  ON sp.id = pspr.procedure_id
  AND sp.org_id = pspr.release_org_id
  AND sp.is_removed = FALSE
LEFT JOIN global_platform.contractor c
  ON c.id = plt.contractor_id
  AND c.is_removed = FALSE
LEFT JOIN global_platform.contractor oc
  ON oc.id = c.original_id
  AND oc.is_removed = FALSE
WHERE
  ps.org_id = 749814257143808
  AND ps.is_removed = FALSE
  AND ssv.audit_status = 'audited'
  AND ssv.is_removed = FALSE
  AND TO_DATE(ssv.updated_at) >= TO_DATE('2022-04-21 00:00:00.000000')
  AND TO_DATE(ssv.updated_at) <= TO_DATE('2022-05-20 23:59:59.999000')
HAVING ssvi.quantity-ssvi.monthly_quantity <> 0
```

SQL 2.3.3：

```mysql
/* from:'node-mq2-plan-service', addr:'10.180.38.126' */
with orgs as(
                  select child_org_id from global_platform.org_relation where org_id = 1270576095704576
              ),
              planData as(
                  select material_id,org_id,material_model,material_name,material_unit,material_code,
                  sum(isnull(quantity,0)) quantity, -- 总计划量
                  avg(book_price) book_price --计划价
                  from m_gh_plan_check
                    where org_id in(select child_org_id from orgs) and is_removed=false
                    and gh_id>0 and is_audit=true
                    group by material_id,org_id,material_model,material_name,material_unit,material_code
              ),
              supplyQuantity as(-- 已经采购数量(包含未提交数据)
                  SELECT -- 采购计划材料
                      isnull(sum(b.quantity),0) as quantity,
                      a.org_id,b.material_id
                      from(
                          SELECT id,org_id from q_purchase_plan WHERE is_removed=FALSE and is_submit = true
                          AND org_id in(select child_org_id from orgs) AND module_type=0
                  ) as a
                  join q_purchase_plan_item as b
                  on a.org_id = b.org_id and a.id = b.order_id
                  WHERE b.is_removed=FALSE
                  GROUP BY a.org_id,b.material_id
              ),
              result as(
                select
                cast(isnull(a.quantity,0) as decimal(28,4)) as plan_quantity,-- 计划量
                cast(isnull(a.book_price,0) as decimal(28,4)) as book_price,-- 计划价
                cast(isnull(b.quantity,0) as decimal(28,4)) as purchase_quantity, -- 已采购量
                cast((a.quantity - isnull(b.quantity,0)) as decimal(28,4)) as not_quantity, -- 剩余购量
                a.org_id,a.material_id,a.material_model,a.material_code,a.material_name,a.material_unit
                from planData as a
                left join supplyQuantity as b
                on a.org_id = b.org_id and a.material_id = b.material_id
                where 1 =1
                
                
                having (a.quantity - isnull(b.quantity,0))>0 -- 剩余量大于0
              )
              select isnull(b.inventory_quantity,0)inventory_quantity,a.* from result as a
              left join (
                  select
                    material_id, org_id,
                    sum(isnull(quantity,0)) inventory_quantity -- 实时库存
                  from q_inventory
                      where org_id in(select child_org_id from orgs) and is_removed=false
                  group by material_id, org_id
              ) as b on a.org_id = b.org_id and a.material_id = b.material_id
              order by a.not_quantity desc
```



## 2.4 group by的内容跟select里的内容对不上

impala对group by字段的处理十分的宽泛，很多SQL在tidb里跑不过

group by中应包含所有非聚合字段，对于select中时表达式的，要么写表达式里的所有字段，要么写别名，要么照抄表达式，都对不上就会报错 

对于比较复杂的表达式，可能照抄都不对（跟tidb内部处理机制有关，没细研究），这种建议按更清晰的方式处理，要么把表达式计算放入一个子查询，要么先group完，再去join其他表计算关联字段

SQL 2.4.1

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

SQL 2.4.2

```mysql
/* from:'node-ca2-report-service', addr:'10.180.180.214' */
with lopsum as
    (
      select  concat( material,cast( isnull(material_id,0) as string)) col_name,round(sum(fact_amnt)/1000,3) fact_amnt  
      from q_manual
      where org_id=1235443508916664  and dat_tim>= '2022-05-01 00:00:00' and dat_tim<='2022-05-30 16:46:32'
      group by material,isnull(material_id,0)
    )
    select * from lopsum order by col_name
```

SQL 2.4.3

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

SQL 2.4.4：

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

SQL 2.4.5：

这个SQL group by里的表达式和select里对不上

原样照抄表达式是能够通过的，但是建议改写的更合理一些，毕竟拖着这么多字段去group效率差，完全没必要

应该group完了，再去join report带出其他字段

```mysql
select 
    report.id,
    project.name,
    report.org_id,
    report.year,
    report.month,
    report.stat_start,
    report.stat_end,
    report.progress_devilate as '进度偏差原因分析', 
    report.devilate_measures as '进度纠偏措施', 
    report.company_help as '需公司协助解决的问题', 
    isnull(report.stopped_reason,last_data.stopped_reason) as '项目停工原因', 
    isnull(report.todo_reason,last_data.todo_reason) as '未开工项目原因', 
    report.closing_explain as '收尾项目情况说明', 
    isnull(report.main_node_target,last_data.main_node_target) as '主要节点目标', 
    report.project_progress as '整体工程进度', 
    report.progress_results as '进度结果判断', 
    report.construct_status as '项目状态',
    report.surplus_contract_amount,
    report.year_complete_amount, 
    report.month_complete_amount, 
    report.plan_opening_date as '计划开通日期',
    round(isnull(report.contract_src_price, project.contract_src_price/10000),0) as '签订合同金额',
    round(isnull(report.contract_amount, project.construction_master_plan/10000),0) as '有效合同金额',
    round(isnull(report.year_plan_amount, plan_detail.year_plan),0) as '年计划产值',
    round(isnull(report. month_plan_amount, case report.month when  1  then plan_detail.january_plan
                      when  2  then plan_detail.february_plan
                      when  3  then plan_detail.march_plan 
                      when  4  then plan_detail.april_plan 
                      when  5  then plan_detail.may_plan
                      when  6  then plan_detail.june_plan
                      when  7  then plan_detail.july_plan 
                      when  8  then plan_detail.august_plan 
                      when  9  then plan_detail.september_plan 
                      when  10 then plan_detail.october_plan 
                      when  11 then plan_detail.november_plan 
                      when  12 then plan_detail.december_plan
                            end),0) as '月计划产值',
    round(sum(if(record.record_date>=if(report.stat_start is null,record.record_date,isnull(last_year_report.stat_start,record.record_date)) && record.record_date<=report.stat_end,quantity.amount,0))/10000,0) as '本年完成产值',
    round(sum(if(record.record_date>=isnull(report.stat_start,record.record_date) && record.record_date<=report.stat_end,quantity.amount,0))/10000,0) as '本月完成产值',
    round(sum(if(record.record_date<=report.stat_end,quantity.amount,0))/10000,0) as '开累产值'
from gxlq_custom.gxlq_project_month_report report
    left  join (--取本年之前成功上报公司的那条数据的统计截止日期+1
                select org_id,
                    date_add(stat_end,1) as stat_start
                from gxlq_custom.gxlq_project_month_report 
                where is_removed=false 
                    and status=1 
                    and org_id=964108905705472
                    and year<2022
                order by year desc,month desc limit 1   
    )last_year_report on report.org_id=last_year_report.org_id
    left  join (--取上条的数据
                select stopped_reason,
                       todo_reason,
                       main_node_target
                from gxlq_custom.gxlq_project_month_report 
                where is_removed=false 
                    and org_id=964108905705472
                    and id<>1281161175669248
                order by year desc,month desc limit 1   
    )last_data on 1=1
    left  join global_platform.project on report.org_id=project.org_id
    left  join global_ipm.project_record record on report.org_id=record.org_id and record.is_removed=false 
    left  join global_ipm.project_record_progress_quantity quantity on record.id=quantity.project_record_id and quantity.is_removed=false
    left  join global_platform.organization org on report.org_id=org.id
    left  join gxlq_custom.gxlq_group_issued_quarter_plan plan on plan.org_id=cast(split_part(org.full_id,'|',1) as bigint) and plan.status=1 and report.year=plan.year and plan.quarter=ceiling(report.month/3) and plan.is_removed=false
    left  join gxlq_custom.gxlq_group_issued_quarter_plan_detail plan_detail on plan.id=plan_detail.quarter_plan_id and plan.org_id=plan_detail.org_id and report.org_id=plan_detail.project_id and plan_detail.is_removed=false
where report.is_removed=false
    and report.org_id=964108905705472
    and report.id=1281161175669248
group by 
    report.id,
    project.name,
    report.org_id,
    report.year,
    report.month,
    report.stat_start,
    report.stat_end,
    report.progress_devilate, 
    report.devilate_measures, 
    report.company_help, 
    isnull(report.stopped_reason,last_data.stopped_reason), 
    isnull(report.todo_reason,last_data.todo_reason), 
    report.closing_explain, 
    isnull(report.main_node_target,last_data.main_node_target), 
    report.project_progress, 
    report.progress_results, 
    report.construct_status,
    report.surplus_contract_amount,
    report.year_complete_amount, 
    report.month_complete_amount, 
    report.plan_opening_date,
    isnull(report.contract_src_price, project.contract_src_price/10000),
    isnull(report.contract_amount, project.construction_master_plan/10000),
    isnull(report.year_plan_amount, plan_detail.year_plan),
    isnull(report. month_plan_amount, case report.month when  1  then plan_detail.january_plan
                      when  2  then plan_detail.february_plan
                      when  3  then plan_detail.march_plan 
                      when  4  then plan_detail.april_plan 
                      when  5  then plan_detail.may_plan
                      when  6  then plan_detail.june_plan
                      when  7  then plan_detail.july_plan 
                      when  8  then plan_detail.august_plan 
                      when  9  then plan_detail.september_plan 
                      when  10 then plan_detail.october_plan 
                      when  11 then plan_detail.november_plan 
                      when  12 then plan_detail.december_plan
                            end)
/*& tenant:gxlq */
/*& $replace:tenant */
```

SQL 2.4.6:

同上

```mysql
select 
    closing_amount.id,
    closing_amount.org_id,
    closing_amount.year,
    closing_amount.month,
    closing_amount.source_explain, 
    closing_amount.surplus_contract_amount, 
    closing_amount.total_amount, 
    closing_amount.year_amount, 
    closing_amount.month_amount, 
    round(isnull(closing_amount.contract_src_price, project.contract_src_price/10000),0) as '签订合同金额',
    round(isnull(closing_amount.contract_amount,project.construction_master_plan/10000),0) as '有效合同金额',
    round(sum(if(record.record_date>=isnull(last_year.stat_start, this_year.stat_month_start) && record.record_date<=closing_amount.stat_end,quantity.amount,0))/10000,0) as '本年完成产值',
    round(sum(if(record.record_date>=isnull(closing_amount.stat_start, this_year.stat_month_start) && record.record_date<=closing_amount.stat_end,quantity.amount,0))/10000,0) as '本月完成产值',
    round(sum(if(record.record_date<=closing_amount.stat_end,quantity.amount,0))/10000,0) as '开累产值'
from gxlq_custom.gxlq_project_closing_amount closing_amount
    left  join (--取本年之前成功上报公司的那条数据的统计截止日期+1
                select org_id,
                    date_add(stat_end,1) as stat_start
                from gxlq_custom.gxlq_project_closing_amount 
                where is_removed=false 
                    and status=1 
                    and org_id=1119960007774208
                    and year<2022
                order by year desc,month desc limit 1   
                )last_year on closing_amount.org_id=last_year.org_id
    -- left  join (--取第一条已上报的项目收尾日期后一年所在统计月开始日期
    --             select 
    --               d.org_id,
    --                 d.stat_month_start
    --             from global_dwb.d_month d 
    --             where d.id=concat('pr-g-',cast(year((select closing_date from gxlq_custom.gxlq_project_closing_amount where org_id=1119960007774208 and status=1 and is_removed=false  order by year,month limit 1))+1 as string),'01-',cast(1119960007774208 as string))
    --             )closing_year on closing_year.org_id=closing_amount.org_id
    left  join (--取月报最后一条数据所在统计年的后一年所在统计月开始日期
                select 
                  d.org_id,
                    d.stat_month_start
                from global_dwb.d_month d 
                where d.id=concat('pr-g-',cast((select year from gxlq_custom.gxlq_project_month_report where org_id=1119960007774208 and is_removed=false and status = 1 order by year desc, month desc limit 1)+1 as string),'01-',cast(1119960007774208 as string))
                )this_year on this_year.org_id=closing_amount.org_id
    left  join global_platform.project on closing_amount.org_id=project.org_id
    left  join global_ipm.project_record record on closing_amount.org_id=record.org_id and record.is_removed=false 
    left  join global_ipm.project_record_progress_quantity quantity on record.id=quantity.project_record_id and quantity.is_removed=false
where closing_amount.is_removed=false
    and closing_amount.org_id=1119960007774208
    and closing_amount.id=1300485053133824
group by 
    closing_amount.id,
    closing_amount.org_id,
    closing_amount.year,
    closing_amount.month,
    closing_amount.source_explain, 
    closing_amount.surplus_contract_amount, 
    closing_amount.total_amount, 
    closing_amount.year_amount, 
    closing_amount.month_amount,
    isnull(closing_amount.contract_src_price, project.contract_src_price/10000),
    isnull(closing_amount.contract_amount,project.construction_master_plan/10000)
/*& tenant:gxlq */
/*& $replace:tenant */
```



## 2.5 表别名不存在

SQL 2.5.1

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

SQL 2.5.2:

q.reviser=901982285443072 没有别名q

```sql
/* from:'iquantity-template-service', addr:'10.180.38.79' */
update cq_command_bill_quantity_split
      set is_removed = true,
        updated_at = NOW(),
        version = 1274852384125928,
        q.reviser=901982285443072
      WHERE is_removed = false
        AND project_bill_quantity_id = 1274840797721088
        AND org_id IN (779552988099584)
```

## 2.6 非空字段插入了空值

impala的处理很宽松，违反空值约束不会报错，只会有一句提示，程序中没处理

但是违反空值约束是插不进去的，属于比较严重的bug，不知道为啥业务上没发现

SQL 2.6.1：

code是非空字段

```sql
/* from:'node-mb-basedata-service', addr:'10.180.137.12' */
INSERT INTO `material` (`org_id`,`id`,`material_category_id`,`code`,`full_code`,`name`,`model`,`spec`,`unit`,`is_expired`,`is_removed`,`created_at`,`updated_at`,`version`,`is_approve`,`category_approve`,`integration_id`,`match_state`,`approve_state`) VALUES (737647887905281,1275397092061696,1026568914284598,NULL,NULL,NULL,NULL,NULL,NULL,false,false,'2022-05-19 05:22:16.698000','2022-05-19 05:22:16.697000',1275397092061696,false,false,'8a8a8ae57ca2f848017ca6a124a430a9',1,0),(737647887905281,1275397092061697,1026568914284598,NULL,NULL,NULL,NULL,NULL,NULL,false,false,'2022-05-19 05:22:16.699000','2022-05-19 05:22:16.697000',1275397092061697,false,false,'8a8a8ae57ca2f848017ca6a124a430ab',1,0),(737647887905281,1275397092061698,1026568914284598,NULL,NULL,NULL,NULL,NULL,NULL,false,false,'2022-05-19 05:22:16.699000','2022-05-19 05:22:16.697000',1275397092061698,false,false,'8a8a8ae57ca2f848017ca6a124a430ae',1,0);
```

SQL 2.6.2:

name是非空字段

```sql
INSERT INTO gdcd_custom.gdcd_year_node_detail (org_id,id,year_node_id,node_id,name,owner_date,cnt_date,major_constraints,creator,created_at,reviser,updated_at,version,order_no) VALUES (1270093449631744,1275203318258176,1275192571491977,null,null,null,null,null,1270105399096832,'2022-05-18 22:48:02.666',1270105399096832,'2022-05-18 22:48:02.666',1275203318274048,1)
/*& tenant:gdcd */
/*& $replace:tenant */
```

SQL 2.6.3:

period_plan_detail_id 是非空字段

```mysql
INSERT INTO cr9g_custom.cr9g_project_period_progress_detail (org_id,id,period_progress_id,period_plan_detail_id,actual_start_date,actual_end_date,actual_progress,actual_effect_target,complete_quantity,surplus_quantity,is_complete,remark,created_at,updated_at,creator,reviser,version) VALUES (1259690140572136,1294622101566464,1294109876786688,1294099193641483,'2022-01-01 00:00:00.0',null,null,null,null,null,null,null,'2022-06-15 09:15:39.614','2022-06-15 09:15:39.614',1259721658088936,1259721658088936,1294622101575168)
/*& tenant:cr9g */
/*& $replace:tenant */ ,(1259690140572136,1294622101740520,1294109876786688,1294099193641487,'2022-01-01 00:00:00.0',null,null,null,null,null,null,null,'2022-06-15 09:15:39.635','2022-06-15 09:15:39.635',1259721658088936,1259721658088936,1294622101756881) ,(1259690140572136,1294622102526976,1294109876786688,null,null,null,null,null,null,null,null,null,'2022-06-15 09:15:39.731','2022-06-15 09:15:39.731',1259721658088936,1259721658088936,1294622102534632) ,
......
```

SQL 2.6.4:

is_removed 是非空字段

```mysql
/* from:'node-mq2-rds-service', addr:'10.180.184.138' */
INSERT INTO `q_receive_more_material` (`id`,`org_id`,`order_id`,`service_type`,`order_type`,`is_red`,`item_data_id`,`submit_date`,`material_id`,`material_code`,`material_name`,`material_model`,`material_unit`,`class_id`,`class_full_id`,`receive_price`,`auxiliary_unit`,`net_quantity`,`rough_quantity`,`conversion_rate`,`deduct_rate`,`deduct_quantity`,`ori_net_quantity`,`auxiliary_net_quantity`,`main_net_quantity`,`waybill_weight`,`item_bar_code`,`manufacturer`,`batch_no`,`test_report_no`,`storage_place`,`stockbin_id`,`stockbin_full_name`,`ori_stockbin_id`,`skill_card_no`,`quality_certificate`,`remark`,`sort_code`,`is_accounted`,`accountor`,`account_date`,`check_state`,`account_order_id`,`acceptance_record`,`ori_material_id`,`ori_class_id`,`ori_common_id`,`ori_item_id`,`ori_plan_id`,`ori_order_id`,`ori_org_id`,`is_removed`,`tax_rate`,`tax_free_price`,`tax_free_sum`,`tax_included_price`,`tax_included_sum`,`tax_amount`,`freight`,`is_void`,`is_accounted_eg`,`creator_id`,`type_mark`,`creator_name`,`created_at`,`modifier_id`,`modifier_name`,`updated_at`,`version`) VALUES (1294637429944832,1165089319505920,1294652334061568,10,4,false,NULL,'2022-06-15',651631265404135,'00090','外贴式橡胶止水带',' E2-3型350mm*10mm','米',NULL,NULL,0,NULL,1250,1250,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'P202206151017105wfx','','','','',0,NULL,NULL,'','',NULL,1,false,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,'',false,NULL,0,0,0,0,0,0,false,false,1174987211796480,'批次维度','','2022-06-15 09:46:50.756000',1174987211796480,'','2022-06-15 10:17:10.508000',1294637429944832),(1294652337370625,1165089319505920,1294652334061568,10,4,false,NULL,'2022-06-15',651631265404179,'00134','中埋式钢边橡胶止水带','350*10mm','米',NULL,NULL,NULL,NULL,550,550,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'P20220615101710aTvt','','','','',NULL,NULL,NULL,'','',NULL,2,false,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,'',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,false,1174987211796480,'批次维度','','2022-06-15 10:17:10.511000',1174987211796480,'','2022-06-15 10:17:10.509000',1294652337370625);

```

SQL 2.6.5:

project_id 是非空字段

```mysql
INSERT INTO gxlq_custom.gxlq_group_issued_quarter_plan_detail (org_id,id,project_id,quarter_plan_id,year_plan,year_after_next,next_year_plan,quarter_plan,january_plan,february_plan,march_plan,april_plan,may_plan,june_plan,july_plan,august_plan,september_plan,october_plan,november_plan,december_plan,is_removed,created_at,updated_at,creator,reviser,version,company_id) VALUES (784341625253889,1284356264317440,null,1284356109948928,0.0,null,null,0.0,null,null,null,null,null,null,null,null,null,null,null,null,false,'2022-05-31 21:09:45.653','2022-05-31 21:09:45.653',591141949961216,591141949961216,1284356264342528,784497769845760)
/*& tenant:gxlq */
/*& $replace:tenant */
```

SQL 2.6.6:

project_id 是非空字段

```mysql
INSERT INTO gxlq_custom.gxlq_company_reported_quarter_plan_detail (org_id,id,project_id,quarter_plan_id,year_plan,next_year_plan,quarter_plan,january_plan,february_plan,march_plan,april_plan,may_plan,june_plan,july_plan,august_plan,september_plan,october_plan,november_plan,december_plan,is_removed,created_at,updated_at,creator,reviser,version,contract_valid_price,total_amount,year_amount,first_quarter_amount,second_quarter_amount,third_quarter_amount,fourth_quarter_amount,remark,is_finished) VALUES (784497769845760,1284356899404288,null,1284355654775808,0.0,null,0.0,null,null,null,null,null,null,null,null,null,null,null,null,false,'2022-05-31 21:11:03.179','2022-05-31 21:11:03.179',591141949961216,591141949961216,1284356899420672,null,null,0.0,null,null,null,null,null,null)
/*& tenant:gxlq */
/*& $replace:tenant */
```

SQL 2.6.7:

project_id是非空字段

```mysql
INSERT INTO gdcd_custom.gdcd_company_quarter_plan_detail (org_id,id,quarter_plan_id,project_id,year,quarter,month,company_amount,is_removed,created_at,updated_at,creator,reviser,version) VALUES (1269830794885120,1301133418486784,1301133309393408,1270093982044672,2022,2,5,200.0,false,'2022-06-24 14:02:58.106','2022-06-24 14:02:58.106',1122123959676928,1122123959676928,1301133418494976)
/*& tenant:gdcd */
/*& $replace:tenant */ ,(1269830794885120,1301133418469888,1301133309393408,1270093873017856,2022,2,5,150.0,false,'2022-06-24 14:02:58.102','2022-06-24 14:02:58.102',1122123959676928,1122123959676928,1301133418478080) ,(1269830794885120,1301133418525696,1301133309393408,1270094114659328,2022,2,5,133.7,false,'2022-06-24 14:02:58.109','2022-06-24 14:02:58.109',1122123959676928,1122123959676928,1301133418543104) ,(1269830794885120,1301133418853888,1301133309393408,1270095292462592,2022,2,5,90.0,false,'2022-06-24 14:02:58.15','2022-06-24 14:02:58.15',1122123959676928,1122123959676928,1301133418869248) ,(1269830794885120,1301133419181568,1301133309393408,1270096616395264,2022,2,5,120.0,false,'2022-06-24 14:02:58.19','2022-06-24 14:02:58.19',1122123959676928,1122123959676928,1301133419197712) ,(1269830794885120,1301133418942976,1301133309393408,1270095601381888,2022,2,5,60.0,false,'2022-06-24 14:02:58.16','2022-06-24 14:02:58.16',1122123959676928,1122123959676928,1301133418953216) ,(1269830794885120,1301133419215360,1301133309393408,1270096843611136,2022,2,5,21.0,false,'2022-06-24 14:02:58.193','2022-06-24 14:02:58.193',1122123959676928,1122123959676928,1301133419230184) ,(1269830794885120,1301133418805760,1301133309393408,1270095199228928,2022,2,5,60.0,false,'2022-06-24 14:02:58.144','2022-06-24 14:02:58.144',1122123959676928,1122123959676928,1301133418830867) ,(1269830794885120,1301133419043304,1301133309393408,1270096113769472,2022,2,5,75.0,false,'2022-06-24 14:02:58.172','2022-06-24 14:02:58.172',1122123959676928,1122123959676928,1301133419059688) ,(1269830794885120,1301133419123640,1301133309393408,1270096401422848,2022,2,5,120.0,false,'2022-06-24 14:02:58.182','2022-06-24 14:02:58.182',1122123959676928,1122123959676928,1301133419140096) ,(1269830794885120,1301133419264512,1301133309393408,null,2022,2,5,null,false,'2022-06-24 14:02:58.2','2022-06-24 14:02:58.2',1122123959676928,1122123959676928,1301133419273216) ,(1269830794885120,1301133418617856,1301133309393408,1270094470206976,2022,2,5,130.0,false,'2022-06-24 14:02:58.122','2022-06-24 14:02:58.122',1122123959676928,1122123959676928,1301133418650088) ,(1269830794885120,1301133418977256,1301133309393408,1270095701684736,2022,2,5,84.0,false,'2022-06-24 14:02:58.164','2022-06-24 14:02:58.164',1122123959676928,1122123959676928,1301133418985984) ,(1269830794885120,1301133419002368,1301133309393408,1270095948493824,2022,2,5,150.0,false,'2022-06-24 14:02:58.167','2022-06-24 14:02:58.167',1122123959676928,1122123959676928,1301133419017192) ,(1269830794885120,1301133419075560,1301133309393408,1270096208313344,2022,2,5,125.5,false,'2022-06-24 14:02:58.176','2022-06-24 14:02:58.176',1122123959676928,1122123959676928,1301133419084288) ,(1269830794885120,1301133418886144,1301133309393408,1270095381158400,2022,2,5,80.0,false,'2022-06-24 14:02:58.153','2022-06-24 14:02:58.153',1122123959676928,1122123959676928,1301133418895872) ,(1269830794885120,1301133418592768,1301133309393408,1270094365432320,2022,2,5,193.0,false,'2022-06-24 14:02:58.117','2022-06-24 14:02:58.117',1122123959676928,1122123959676928,1301133418616272) ,(1269830794885120,1301133418436096,1301133309393408,1270093449631744,2022,2,5,300.0,false,'2022-06-24 14:02:58.099','2022-06-24 14:02:58.099',1122123959676928,1122123959676928,1301133418445824) ,(1269830794885120,1301133418657280,1301133309393408,1270094568413184,2022,2,5,43.0,false,'2022-06-24 14:02:58.126','2022-06-24 14:02:58.126',1122123959676928,1122123959676928,1301133418673152) ,(1269830794885120,1301133419296256,1301133309393408,null,2022,2,5,null,false,'2022-06-24 14:02:58.204','2022-06-24 14:02:58.204',1122123959676928,1122123959676928,1301133419313128) ,(1269830794885120,1301133418779648,1301133309393408,1270095012166144,2022,2,5,121.0,false,'2022-06-24 14:02:58.14','2022-06-24 14:02:58.14',1122123959676928,1122123959676928,1301133418781696) ,(1269830794885120,1301133418707968,1301133309393408,1270094748793856,2022,2,5,70.0,false,'2022-06-24 14:02:58.133','2022-06-24 14:02:58.133',1122123959676928,1122123959676928,1301133418732520) ,(1269830794885120,1301133418557952,1301133309393408,1270094221858816,2022,2,5,68.0,false,'2022-06-24 14:02:58.113','2022-06-24 14:02:58.113',1122123959676928,1122123959676928,1301133418575802) ,(1269830794885120,1301133418689512,1301133309393408,1270094665658880,2022,2,5,107.58,false,'2022-06-24 14:02:58.129','2022-06-24 14:02:58.129',1122123959676928,1122123959676928,1301133418706432) ,(1269830794885120,1301133419337324,1301133309393408,null,2022,2,5,null,false,'2022-06-24 14:02:58.209','2022-06-24 14:02:58.209',1122123959676928,1122123959676928,1301133419353600) ,(1269830794885120,1301133419232256,1301133309393408,1270096961131008,2022,2,5,830.0,false,'2022-06-24 14:02:58.197','2022-06-24 14:02:58.197',1122123959676928,1122123959676928,1301133419254784) ,(1269830794885120,1301133418912256,1301133309393408,1270095501236224,2022,2,5,22.4,false,'2022-06-24 14:02:58.156','2022-06-24 14:02:58.156',1122123959676928,1122123959676928,1301133418919424) ,(1269830794885120,1301133418747368,1301133309393408,1270094842746368,2022,2,5,150.0,false,'2022-06-24 14:02:58.137','2022-06-24 14:02:58.137',1122123959676928,1122123959676928,1301133418763264) ,(1269830794885120,1301133419156456,1301133309393408,1270096514783744,2022,2,5,null,false,'2022-06-24 14:02:58.186','2022-06-24 14:02:58.186',1122123959676928,1122123959676928,1301133419174400)
```

SQL 2.6.8:

id居然也插空的

```mysql
INSERT INTO cscrc_custom.cscrc_command_week_report_construction (org_id,id,command_week_report_id,project_id,section_id,section_name,reward_punishment,progress_id,progress_name,progress_unit,design_quantity,month_plan_quantity,month_quantity,week_plan_quantity,week_quantity,total_quantity,week_plan_amount,week_amount,incomplete_reason,rectification_measures,created_at,updated_at,creator,reviser,version) VALUES (779372762624000,null,1239640054235112,null,null,null,0.0,null,null,null,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,null,null,'2022-06-23 20:54:56.182','2022-06-23 20:54:56.182',591141949961216,591141949961216,1300628120204800)
/*& tenant:cscrc */
/*& $replace:tenant */
```

SQL 2.6.9：

org_id非空

```mysql
/* from:'pr-service', addr:'10.180.21.130' */
INSERT INTO  global_platform.org_relation_data(org_id, id, relation_org_id, zone_id, creator, reviser, created_at, updated_at, version) VALUES (null, 1300712188721664, 997377536790528, null, 1001595262999040, 1001595262999040, NOW(), NOW(), 1300712188721664)
```



## 2.7 错误的日期格式

impala里，无论什么样的字符串，都能插入到日期字段，只不过不合规范的会插入为null

tidb里不合规范的格式会报错

SQL 2.7.1:

出现了这样的日期字符串：'133406-03-22 00:00:00'，还发现过这样的：'NaN-aN-aN aN:aN:aN'

顺道说一句，用case when实现这个目的真别扭，为啥不用values

```sql
/* from:'itask-service-v2', addr:'10.180.38.101' */
UPDATE project_plan_detail SET stat_actual_start_date=CASE id WHEN 1179989213982721 THEN '2021-11-20 00:00:00' WHEN 1179993240368640 THEN null WHEN 1179993456979968 THEN null WHEN 1179989213982843 THEN null WHEN 1179989213982845 THEN null WHEN 1179989213982846 THEN null WHEN 1179989213982847 THEN null WHEN 1179989213982849 THEN null WHEN 1179989213982850 THEN null WHEN 1179989213982851 THEN null WHEN 
......
1179989213982934 THEN null WHEN 1179989213982910 THEN null WHEN 1179989213982908 THEN null WHEN 1179989213982906 THEN null WHEN 1179989213982904 THEN null WHEN 1179989213982918 THEN null ELSE stat_actual_end_date END, stat_est_end_date=CASE id WHEN 1179989213982721 THEN '133406-03-22 00:00:00' WHEN 1179993240368640 THEN '2022-05-19 00:00:00' WHEN 1179993456979968 THEN '2022-05-19 00:00:00' WHEN 
......
1179989213982908 THEN '2022-05-19 00:00:00' WHEN 1179989213982906 THEN '2022-05-19 00:00:00' WHEN 1179989213982904 THEN '2022-05-19 00:00:00' WHEN 1179989213982918 THEN '2022-05-19 00:00:00' ELSE stat_est_start_date END, version=1275306243586536, reviser=1155342276554752, updated_at=NOW() WHERE  id IN (1179989213982721, 1179993240368640, 1179993456979968, 1179989213982843, 1179989213982845, 1179989213982846, 
......
1179989213982906, 1179989213982904, 1179989213982918)
```

SQL 2.7.2:

出现了这样的日期字符串：'133406-03-21 00:00:00' 或 'NaN-aN-aN aN:aN:aN'

```sql
/* from:'itask-service-v2', addr:'10.180.88.164' */
UPDATE project_plan SET stat_actual_start_date=CASE id WHEN 1165272956449792 THEN '2021-11-20 00:00:00' WHEN 1179989213982720 THEN '2021-11-20 00:00:00' ELSE stat_actual_start_date END, stat_actual_end_date=CASE id WHEN 1165272956449792 THEN null WHEN 1179989213982720 THEN null ELSE stat_actual_end_date END, stat_est_end_date=CASE id WHEN 1165272956449792 THEN '133406-03-21 00:00:00' WHEN 1179989213982720 THEN '133406-03-21 00:00:00' ELSE stat_est_end_date END, version=1274858129683456, reviser=1155342276554752, updated_at=NOW() WHERE  id IN (1165272956449792, 1179989213982720)
```

SQL 2.7.3:

这种不是标准的默认时间格式 '8-5-20'，impala里也插不进去，只不过不报错

```sql
/* from:'excel-service', addr:'10.180.21.135' */
UPSERT INTO manage_person_roster (excel_task_id, org_id, id, version, creator, reviser, created_at, updated_at, name, sex, post, phone_number, in_date, out_date, remark) VALUES('excel:iquantity:import:10021:1270106435376128:1652841182473', 1270094665658880, 1274842050731496, 1274842050780160, 1270106435376128, 1270106435376128, NOW(), NOW(), '彭良辉', '男', '项目总工', '{crypto}kOZjZjWCy3ZW3b+xBemI3Q==', '8-5-20', null, null),('excel:iquantity:import:10021:1270106435376128:1652841182473', 1270094665658880, 1274842050731497, 1274842050780161, 1270106435376128, 1270106435376128, NOW(), NOW(), '麦俊辉', '男', '机材主管', '{crypto}hL5rAd7Q9pE73cGVV/s3bA==', '5-1-15', null, null),('excel:iquantity:import:10021:1270106435376128:1652841182473', 1270094665658880, 1274842050731498, 1274842050780162, 1270106435376128, 1270106435376128, NOW(), NOW(), '赖水源', '男', '中队长/兼后勤', '{crypto}T2X5X3ClbMQtZsZ0HbhUcg==', '3-6-16', null, null),('excel:iquantity:import:10021:1270106435376128:1652841182473', 1270094665658880, 1274842050731499, 1274842050780163, 1270106435376128, 1270106435376128, NOW(), NOW(), '李赞莲', '女', '经营主管', '{crypto}ld2N1Of8fE5qNaIlEwk9hQ==', '4-21-18', null, null),('excel:iquantity:import:10021:1270106435376128:1652841182473', 1270094665658880, 1274842050731500, 1274842050780164, 1270106435376128, 1270106435376128, NOW(), NOW(), '王小辉', '男', '施工员', '{crypto}Il2UJX+2hCwkBk+7tkK/Qw==', '3-10-19', null, null),('excel:iquantity:import:10021:1270106435376128:1652841182473', 1270094665658880, 1274842050731501, 1274842050780165, 1270106435376128, 1270106435376128, NOW(), NOW(), '蒋诚', '男', '施工员', '{crypto}Xwniv1d7fgkMC0Abo+iivQ==', '8-3-21', null, null),('excel:iquantity:import:10021:1270106435376128:1652841182473', 1270094665658880, 1274842050731502, 1274842050780166, 1270106435376128, 1270106435376128, NOW(), NOW(), '唐文正', '男', '机材管理员', '{crypto}5jnbNj+Go0DVsTiIGIQD1g==', '8-3-21', null, null),('excel:iquantity:import:10021:1270106435376128:1652841182473', 1270094665658880, 1274842050731503, 1274842050780167, 1270106435376128, 1270106435376128, NOW(), NOW(), '陈加耀', '男', '经营管理员', '{crypto}GjjjQgLOcH8o/JJn1BY++w==', '8-3-21', null, null)
```

SQL 2.7.4:

出现了这样的日期字符串：'Invalid date'

```sql
/* from:'node-mq-weight-consumer', addr:'10.180.137.125' */
INSERT INTO `q_delivery_more_material` (`id`,`org_id`,`order_id`,`service_type`,`order_type`,`material_id`,`material_code`,`material_name`,`material_model`,`material_unit`,`item_bar_code`,`auxiliary_unit`,`net_quantity`,`conversion_rate`,`is_red`,`sort_code`,`is_accounted`,`ori_material_id`,`ori_item_id`,`ori_order_id`,`is_removed`,`creator_name`,`created_at`,`updated_at`,`version`,`submit_date`,`receive_price`) VALUES (3748,998971519127552,1274801050163200,10,4,1027455049306194,'00001294087','混凝土','C15','立方米','','立方米',6.36,1,false,0,false,'1099000000001294087','L122-00360-A0006','L122-00360-A0006',false,'靳艾萍','Invalid date','2022-05-18 09:09:37.711000',3748,'2022-05-18',0);
```

SQL 2.7.5:

这种不是标准的默认时间格式'10-1-81'，impala里也插不进去，只不过不报错

```sql
/* from:'excel-service', addr:'10.180.180.216' */
UPSERT INTO hr_employee_attendance (excel_task_id, org_id, id, version, creator, reviser, created_at, updated_at, year, month, employment_nature, department, person_name, person_id, post, person_class, overtime_subsidy_grade, join_work_time, job_title, job_title_level, job_title_start_time, date_one, date_two, date_three, date_four, date_five, date_six, date_seven, date_eight, date_nine, date_ten, date_eleven, date_twelve, date_thirteen, date_fourteen, date_fifteen, date_sixteen, date_seventeen, date_eighteen, date_nineteen, date_twenty, date_twenty_one, date_twenty_two, date_twenty_three, date_twenty_four, date_twenty_five, date_twenty_six, date_twenty_seven, date_twenty_eight, date_twenty_nine, date_thirty, date_thirty_one, attendance, business_trip, public_rest_overtime, holiday_overtime, thing_leave, marriage_leave, funeral_leave, family_leave, year_rest_leave, maternity_leave, nursing_leave, sick_leave_one, sick_leave_two, work_injury, study_one, study_two, absenteeism, parenting, retire, wait_post_train, exclude_attendance) VALUES('excel:iquantity:import:10051:816870672258536:1652867883569', 1175208257327104, 1275060820497408, 1275060820497408, 816870672258536, 816870672258536, NOW(), NOW(), 2022, 1, null, null, null, null, null, null, null, null, null, null, null, '出勤', '出差', '法定节假日加班', '事假', '婚假', '丧假', '探亲假', '年休假', '工伤假', '培训', '机关轮训', '机关助勤', '产假', '陪产假', '旷工', '丧假', '探亲假', '年休假', '事假', '产假', '婚假', '事假', '培训', '工伤假', '机关轮训', '旷工', '病假', '培训', '产假', '出勤', '出差', 1, 4, 6, 8, 9, 10, null, null, null, 11, 12, 13, 14, 15, 17, 18, 20, 21, 22, 23, 24),('excel:iquantity:import:10051:816870672258536:1652867883569', 1175208257327104, 1275060820497409, 1275060820497409, 816870672258536, 816870672258536, NOW(), NOW(), 2022, 1, '实力员工', '项目领导', '陆国平', '511622198403151013', '项目经理', '管理人员', null, '2009-7-1', null, null, null, '法定加班', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '年休', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),('excel:iquantity:import:10051:816870672258536:1652867883569', 1175208257327104, 1275060820497410, 1275060820497410, 816870672258536, 816870672258536, NOW(), NOW(), 2022, 1, '实力员工', '项目领导', '高云良', '420621196311190076', '党工委书记', '管理人员', null, '10-1-81', null, null, null, '法定加班', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '法定加班', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
......
('excel:iquantity:import:10051:816870672258536:1652867883569', 1175208257327104, 1275060820497441, 1275060820497441, 816870672258536, 816870672258536, NOW(), NOW(), 2022, 1, '临时用工', '机电部', '王志辉', '411202197404284015', '电工', '其他人员', null, '2021-12-7', null, null, null, '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', '出勤', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
```

SQL 2.7.6:

出现了这样的日期字符串：'Invalid date'

```sql
/* from:'node-ca-concrete-service', addr:'10.180.38.176' */
INSERT INTO `q_mapping_materials_detail` (`org_id`,`id`,`org_name`,`material`,`pro_line`,`material_id`,`material_code`,`material_name`,`material_model`,`material_unit`,`auxiliary_unit`,`conversion_rate`,`is_matching`,`begin_date`,`end_date`,`first_extract_time`,`ori_material_id`,`sort_code`,`creator_id`,`creator_name`,`created_at`,`modifier_id`,`modifier_name`,`updated_at`,`version`) VALUES (1012251829238272,1275095292318648,'预制构件项目部','水泥1','Z1',1027455049379903,'00001293881','普通硅酸盐水泥','P · O 42.5 散装','吨','吨',1,false,'2016-05-01 19:08:04','2022-05-18 19:08:04','Invalid date','1099000000001293881',2,961627853945344,'李志成','2022-05-18 19:08:15.938000',961627853945344,'李志成','2022-05-18 19:08:15.939000',1275095292318648);
```

SQL 2.7.7:

出现了这样的日期字符串：'2013-7-1O'

```mysql
/* from:'excel-service', addr:'10.180.21.135' */
INSERT INTO hr_project_month_job (excel_task_id, org_id, id, version, creator, reviser, created_at, updated_at, stat_year, stat_month, staff_name, id_card, department, station, sex, nation, native_place, birth, age, work_date, work_age, on_unit_date, political_face, education, graduate_university, major, technical_position, mobile, employment_form, station_type, part_time_project, remark) VALUES('excel:iquantity:import:10051:933042408036328:1653911896306', 759009464701952, 1283613358175232, 1283613358175232, 933042408036328, 933042408036328, NOW(), NOW(), 2022, 5, '郝建伟', '420683198309100375', '项目领导', '常务副经理兼党支部副书记', '男', '汉族', '湖北襄阳', '1983-9-10', 38, '2007-7-1', 14, '2010-7-1', '中共党员', '本科', '武汉工程大学', '土木工程', '工程师', 18382383340, '实力员工', '生产管理岗位', null, null),
......
('excel:iquantity:import:10051:933042408036328:1653911896306', 759009464701952, 1283613358175292, 1283613358175292, 933042408036328, 933042408036328, NOW(), NOW(), 2022, 5, '陈曦', '421302199110250895', '计划部', '部长', '男', '汉族', '湖北随州', '1991-10-25', 30, '2013-7-10', 8, '2013-7-1O', '群众', '本科', '大连民族学院', '土木工程', '工程师', 13554021693, '实力员工', '生产管理岗位', null, null),
......
('excel:iquantity:import:10051:933042408036328:1653911896306', 759009464701952, 1283613358175303, 1283613358175303, 933042408036328, 933042408036328, NOW(), NOW(), 2022, 5, '刘浩', '610523199309035110', '协调部', '协调员', '男', '汉族', '陕西渭南', '1993-9-3', 28, '2013-4-1', 9, '2013-4-1', '共青团员', '专科', '石家庄铁道大学', '工程测量', '未聘', 15877649575, '自主用工', '生产管理岗位', null, null)
    
```

SQL 2.7.8:

出现了这样的日期字符串：'6-30-07'

```mysql
/* from:'excel-service', addr:'10.180.59.254' */
UPSERT INTO hr_strength_employee (excel_task_id, org_id, id, version, creator, reviser, created_at, updated_at, year, month, employment_nature, department, person_name, person_id, post, person_class, job_title, job_title_level, job_title_start_time, sal_level, actual_send_total, should_send_total, deduction_total, project_stimulate, endowment_insurance, unemployment_insurance, medical_insurance, housing_provident_fund, employment_injury_insurance, month_bonus, remark) VALUES('excel:iquantity:import:10051:768982698176512:1653881155615', 759005196121088, 1283361557517312, 1283361557517312, 768982698176512, 768982698176512, NOW(), NOW(), 2022, 4, '实力员工', '项目领导', '吴罡令', '500237198403028939', '常务副经理（主持工作）兼任党工委副书记', '管理人员', '工程师', '中级', '6-30-07', '16-3', 8455.45, 11747, 3291.55, 2132, 1103.2, 41.37, 275.8, 1654.8, 172.38, 0, null),('excel:iquantity:import:10051:768982698176512:1653881155615', 759005196121088, 1283361557517313, 1283361557517313, 768982698176512, 768982698176512, NOW(), NOW(), 2022, 4, '实力员工', '财务部', '刘艳琴', '130103198011130020', '部长', '管理人员', '助理会计师', '助理级', '#REF!', '18-5', 7890.46, 10633, 2742.54, 1540, 918.4, 34.44, 229.6, 1377.6, 143.5, 0, null),('excel:iquantity:import:10051:768982698176512:1653881155615', 759005196121088, 1283361557517314, 1283361557517314, 768982698176512, 768982698176512, NOW(), NOW(), 2022, 4, null, null, null, null, null, null, null, null, null, null, null, null, null, 0, null, null, null, null, null, 0, null)
```

SQL 2.7.9:

出现了这样的日期字符串：'Invalid date'

```mysql
/* from:'itask-service-v2', addr:'10.180.88.164' */
UPDATE `project_work_deadline` SET `parent_id`=1066046439330816,`name`='武家塬隧道1#斜井大里程',`node_type`='controlDate',`plan_start_date`=null,`plan_end_date`='2023-12-23 00:00:00.000000',`adjustment_start_date`='Invalid date',`adjustment_end_date`='2023-12-23 00:00:00.000000',`adjustment_version`=0,`duration`=null,`actual_start_date`=null,`actual_end_date`=null,`full_id`='1066046439330816|1069066441298432',`full_id_ex`='|1066046439330816|1069066441298432|',`is_leaf`=true,`level`=2,`order_no`=1066400559394819,`remark`=null,`reviser`=1062672453710848,`updated_at`='2022-06-01 14:38:49.334000',`version`=1284871898723840 WHERE `id` = 1069066441298432 AND `org_id` = 1025872933156864
```

SQL 2.7.10:

Incorrect datetime value: 'NaN-aN-aN aN:aN:aN'

```mysql
/* from:'itask-service-v2', addr:'10.180.88.164' */
UPDATE project_plan_detail SET stat_actual_start_date=CASE id WHEN 892144663319040 THEN '2020-09-16 00:00:00' WHEN 892144789894144 THEN '2020-09-16 00:00:00' WHEN 892144982520320 THEN null WHEN 892145065136128 THEN null WHEN 892145569263616 THEN null WHEN 892146581688320 THEN null WHEN 892145274647040 THEN null WHEN 892145135735808 THEN null WHEN 892146196115456 THEN null WHEN 892146309968896 THEN null WHEN 892146390610944 THEN null ELSE stat_actual_start_date END, stat_actual_end_date=CASE id WHEN 892144663319040 THEN null WHEN 892144789894144 THEN null WHEN 892144982520320 THEN null WHEN 892145065136128 THEN null WHEN 892145569263616 THEN null WHEN 892146581688320 THEN null WHEN 892145274647040 THEN null WHEN 892145135735808 THEN null WHEN 892146196115456 THEN null WHEN 892146309968896 THEN null WHEN 892146390610944 THEN null ELSE stat_actual_end_date END, stat_est_end_date=CASE id WHEN 892144663319040 THEN '2022-07-27 00:00:00' WHEN 892144789894144 THEN '2022-06-01 00:00:00' WHEN 892144982520320 THEN '2022-06-25 00:00:00' WHEN 892145065136128 THEN '2022-07-16 00:00:00' WHEN 892145569263616 THEN '2022-07-27 00:00:00' WHEN 892146581688320 THEN '2022-06-01 00:00:00' WHEN 892145274647040 THEN '2022-06-10 00:00:00' WHEN 892145135735808 THEN '2022-07-27 00:00:00' WHEN 892146196115456 THEN '2022-06-16 00:00:00' WHEN 892146309968896 THEN '2022-06-11 00:00:00' WHEN 892146390610944 THEN 'NaN-aN-aN aN:aN:aN' ELSE stat_est_end_date END, stat_est_start_date=CASE id WHEN 892144663319040 THEN '2020-09-16 00:00:00' WHEN 892144789894144 THEN '2020-09-16 00:00:00' WHEN 892144982520320 THEN '2022-06-01 00:00:00' WHEN 892145065136128 THEN '2022-06-01 00:00:00' WHEN 892145569263616 THEN '2022-06-01 00:00:00' WHEN 892146581688320 THEN '2022-06-01 00:00:00' WHEN 892145274647040 THEN '2022-06-01 00:00:00' WHEN 892145135735808 THEN '2022-06-01 00:00:00' WHEN 892146196115456 THEN '2022-06-01 00:00:00' WHEN 892146309968896 THEN '2022-06-01 00:00:00' WHEN 892146390610944 THEN 'NaN-aN-aN aN:aN:aN' ELSE stat_est_start_date END, version=1284513946449408, reviser=882002296738304, updated_at=NOW() WHERE  id IN (892144663319040, 892144789894144, 892144982520320, 892145065136128, 892145569263616, 892146581688320, 892145274647040, 892145135735808, 892146196115456, 892146309968896, 892146390610944)
```

SQL 2.7.11:

'2022-05-30-2020-12-30' for column 'cert_valid_date'

```mysql
/* from:'excel-service', addr:'10.180.180.216' */
UPSERT INTO contractor (excel_task_id, org_id, id, version, creator, reviser, created_at, updated_at, original_org_id, code, company_name, credit_code, credit_code_valid_date, is_credit_code_perpetual, cert_level_range, cert_number, cert_valid_date, is_cert_perpetual, type, business_scope, registration_city, registration_address, bank_name, bank_account, registered_capital, taxpayer_type, corporation_name, corporation_identity_number, corporation_phone_number, license_number, license_valid_date, is_license_perpetual, is_external, contractor_evaluation_id, excel_spared, remark) VALUES(
...
)
```



# 3. 拼接的SQL太长

太长的SQL一般是拼起来的插入语句，在翻译的时候性能很低，而且也不是很合理，所以控制下每次插入的批次数量

目前设置的阈值是长度超过3M的SQL直接抛出错误

SQL 3.1：

发现拼出来的SQL有18M之多，几万行。改为按批次拼接和执行，最多1000条一个批次。

```sql
/* from:'pr-timer-service', addr:'10.180.21.148' */ 
UPSERT INTO crssg_custom.crssg_bi_finance_index_analysis  (org_id,id,year,month,org_code,org_name,parent_code,flag,index_type,index_name,value,is_removed,creator,created_at,reviser,updated_at,version) 
VALUES (552383621616640, 'T02050010010330000000_2022_4_营业收入_本月值', 2022, 4, 'T02050010010330000000', '中铁十四局集团水利水电分公司清欠中心', 'C02050160000000000009', '季报', '营业收入', '本月值', -723.49, false, 10001, '2022-05-19 03:57:44.1000', 10001, '2022-05-19 03:57:44.1000', 1275355537397224), ...
```

SQL 3.2:

user:  crssg_internal_write / ys2_internal_write / ……

拼出的SQL有2500多条插入记录，由于project表有大段文字描述，所以每次插入的条数控制的小一些

```mysql
/* from:'h-data005', addr:'10.200.6.20' */ UPSERT INTO project (`id`, `org_id`, `name`, `short_name`, `manager`, `manager_mobile`, `overview`, `construct_status`, `construct_type`, `construct_purpose`, `struct_type`, `plan_start`, `plan_end`, `plan_finish`, `actual_start`, `area`, `contract_price`, `address`, `longitude`, `latitude`, `bidding_unit`, `design_unit`, `construct_unit`, `superviser_unit`, `thumbnail`, `sync_code`, `version`, `is_removed`, `created_at`, `updated_at`, `deleted_at`, `province`, `contract_sign_date`, `contract_src_price`, `contract_change_price`, `is_foreign_country`, `risk_level`, `manage_type`, `qualification`, `engineering_length`, `spot_shining_construction`, `actual_finish`, `ensuring_opening_liable_unit`, `liable_leader`, `liable_leader_phone`, `plan_opening_date`, `actual_opening_date`, `opening_remark`, `main_engineering_quantity`, `control_engineering_and_briefing`, `belong_railways_bureau`, `contract_type`, `construction_master_plan`, `application_mode`, `city`, `closing_date`, `closed_date`, `provisional_sums`, `third_party_inspection`, `bid_section`, `bid_section_length`, `design_speed`, `transport_type`, `altitude_min`, `altitude_max`, `internal_risk_level`, `chief_engineer_name`, `chief_engineer_phone`, `handover_acceptance_date`, `completed_acceptance_date`, `construction_method_resume`, `unit_task_partitioning`, `progress_works_plan`, `main_items`, `construction_method`, `line`, `bid_section_begin_end`, `is_control_project`, `editor`, `editor_phone`, `postal_address`, `postcode`, `fax`, `remark`, `spare_field`, `bid_unit`, `bid_date`, `project_leader_name`, `project_leader_phone`, `bid_section_short`, `investment_type`, `investment_price`, `investment_after_change_price`, `bidding_unit_name`, `bidding_unit_phone`, `design_unit_name`, `design_unit_phone`, `superviser_unit_name`, `superviser_unit_phone`, `survey_unit`, `contract_manager`, `contract_chief_engineer`, `quality_assessment`, `opening_reason`, `second_construct_type`, `field01`, `field02`, `field03`, `field04`, `field05`, `field06`, `field07`, `field08`, `field09`, `field10`, `field11`, `field12`, `field13`, `field14`, `field15`, `field16`, `field17`, `field18`, `field19`, `field20`, `field21`, `field22`, `field23`, `field24`, `field25`, `field26`, `field27`, `field28`, `field29`, `field30`, `field31`, `field32`, `field33`, `field34`, `field35`, `field36`, `field37`, `field38`, `field39`, `field40`, `field_text01`, `field_text02`, `field_text03`, `field_text04`, `field_text05`) 
VALUES (498347181863936, 498347181863936, '翔安机场高速', '翔安机场高速', '朱喊东', '{crypto}3+Tc5Rdeendju7RsFpYYrQ==', '时速100公里/小时，合同额8.06亿。线路起点设内厝互通与沈海高速连接，主线上跨国道324，下穿在建福厦客专，然后穿越面前山、鸿渐山，在鸿渐村附近设收费站，经内头大桥与C2标连接。主要工程内容：内厝互通（分为A、B、C、D四条匝道）、前垵大桥（427m）、前垵隧道（左洞490m/右洞470m）、收费站一处、内头大桥（307.5m）、主线路基2.08km', 'building', '公路工程', null, null, null, null, null, null, null, 805935100, '福建省厦门市翔安区', 118.310132, 24.610206, '厦门路桥建设集团有限公司', '中设设计集团股份有限公司', '中铁十四局集团第三工程有限公司', '福建省交通建设工程监理咨询有限公司', null, null, 1225930330182656, false, '2019-05-17 08:50:11.000000', '2022-03-10 08:01:53.000000', null, '福建省', null, 805935100, 0, false, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 0, '标准应用', '厦门市', null, null, 0, '福州建通工程试验检测有限公司', null, null, null, null, null, null, -1, null, null, null, null, null, null, null, null, null, null, null, false, null, null, null, null, null, null, null, '中铁十四局集团第三工程有限公司', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null), 
(501584939471360, 501584939471360, '济南绕城高速二环线东环段项目', '济南绕城高速二环线东环段项目', '许召军', null, '济南绕城高速公路二环线东环段项目起点起自唐王枢纽立交，经城角巷北，设唐王南互通立交与X051连接，下穿济青高铁和在建邯济铁路至胶济铁路联络线后，设龙山互通立交与S102省道连接并上跨胶济客专和胶济铁路，向南跨越世纪大道，于曹范枢纽互通，到达终点。项目全长23.594公里，主线采用双向6车道高速公路标准建设，设计速度120Km/h，路基宽度34.5m，桥涵设计荷载等级为公路-Ⅰ级。', 'finished', '公路工程', null, null, '2018-11-01 08:00:00.000000', '2020-12-31 08:00:00.000000', '2020-12-31 00:00:00.000000', '2018-09-01 00:00:00.000000', null, 1135839900, '山东省济南市历城区董家镇甄家村', 117.06526568028156, 36.69236943626191, '齐鲁交通发展集团', '山东省交通规划设计院', '中铁十四局集团第四工程有限公司', '山东省交通工程监理咨询有限公司', null, null, 1261435892669928, false, '2019-05-21 22:37:25.000000', '2022-04-29 11:58:08.000000', null, '山东省', '2018-12-14 00:00:00.000000', 1135839900, 0, false, null, null, null, 13.055, null, '2020-12-31 00:00:00.000000', null, null, null, null, null, null, null, null, null, null, 0, '标准应用', '济南市', null, null, 0, null, null, null, null, null, null, null, -1, null, null, null, null, null, null, null, null, null, null, null, false, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null), 
......
```

SQL 3.3:

```mysql
/* from:'sm-integration-crssg-service', addr:'10.180.184.137' */ 
upsert into subcontract_procedure ( org_id , id , package_id , procedure_category_id , code , name , unit , job_content , cost_composition , measurement_rule , order_no , full_code , creator , created_at , reviser , updated_at , version , is_removed , integration_id ) 
select 194004748551552 as org_id , ifnull(sp_old.id, __fn.sequence()) as id , 1182392418807808 as package_id , 1182392418807809 as procedure_category_id , sp_new.code as code , sp_new.name as name , sp_new.unit as unit , sp_new.job_content as job_content , sp_new.cost_composition as cost_composition , sp_new.measurement_rule as measurement_rule , ifnull(sp_old.order_no, __fn.sequence()) as order_no , sp_new.full_code as full_code , 10016 as creator , ifnull(sp_old.created_at, now()) as created_at , 10016 as reviser , now() as updated_at , __fn.sequence() as version , false as is_removed , sp_new.code as integration_id from (select * from (VALUES 
(194004748551552 as org_id,1182392418807808 as package_id,1182392418807809 as procedure_category_id,'1.1.1.1.1-C08.09.02.005.059' as code,'JCXT01-1.1.1.1.1-C08.09.02.005.059' as full_code,'配电箱、控制箱、插座箱 综合控制箱（含电源模块、控制模块、防雷、支架等）' as name,'台' as unit,'开箱检查、设备组装、画线定位、安装' as job_content,'单价包含除配电箱、控制箱、插座箱 综合控制箱（含电源模块、控制模块、防雷、支架等以外的全部费用' as cost_composition,'按设计图所示，以现场实际完成的合格数量计算' as measurement_rule,'1.1.1.1.1-C08.09.02.005.059' as integration_id),
(194004748551552 as org_id,1182392418807808 as package_id,1182392418807809 as procedure_category_id,'1.1.1.1.10-C08.08.05.007.008' as code,'JCXT01-1.1.1.1.10-C08.08.05.007.008' as full_code,'SC50内暗敷' as name,'m' as unit,'1、打眼、埋螺栓、锯管、套丝、煨管、路敷管设；2、支架制作、安装；3、接线盒（箱）安装；4、防腐油漆；5、接地；6、刨沟槽及墙面恢复；7、二次运输及保管等全部工作内容' as job_content,'单价包含除钢管以外的全部费用' as cost_composition,'按工程量计算规则计算施工完成数量' as measurement_rule,'1.1.1.1.10-C08.08.05.007.008' as integration_id),
(194004748551552 as org_id,1182392418807808 as package_id,1182392418807809 as procedure_category_id,'1.1.1.1.11-C08.08.05.007.009' as code,'JCXT01-1.1.1.1.11-C08.08.05.007.009' as full_code,'SC80内暗敷' as name,'m' as unit,'1、打眼、埋螺栓、锯管、套丝、煨管、路敷管设；2、支架制作、安装；3、接线盒（箱）安装；4、防腐油漆；5、接地；6、刨沟槽及墙面恢复；7、二次运输及保管等全部工作内容' as job_content,'单价包含除钢管以外的全部费用' as cost_composition,'按工程量计算规则计算施工完成数量' as measurement_rule,'1.1.1.1.11-C08.08.05.007.009' as integration_id),
......
```

