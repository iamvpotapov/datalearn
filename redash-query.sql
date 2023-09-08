with prep as (
select
    distinct p.employee_id,
    -- employeeName,
    case 
        when p.employee_id = 164448 and h.contractTypeName is null then 'Робот Макс'
        when h.contractTypeName = 'Трудовой договор' then 'Штат'
        when h.contractTypeName = 'Договор подряда (выполнение работ, оказание услуг)' then 'ГПХ'
        else h.contractTypeName end as contractTypeName,
    sumIf(is_final, prev_final = 0) as final,
    count(distinct mass_answer_com_id) as mass_dstnct,
    ifnull(sum(case when parent_theme_id = 1 and is_final = 1 then is_final end),0) as "Приемка",
    count(mass_answer_com_id) as mass_all_msg,
    final - mass_all_msg as not_mass,
    count(case when employee_id is not null and is_final = 0 then _id end) as clarify,
    clarify + final as all_clarify_and_final,
    count(distinct old_supplier_id) as suppliers
from 
    (select 
        id,
        _id,
        lagInFrame(is_final) over( partition by com_id order by id ) prev_final, 
        lagInFrame(employee_id) over( partition by com_id order by id ) prev_empl, 
        com_id,
        is_final,
        parent_theme_id,
        parent_theme_name,
        theme_id,
        case when employee_id is null then dt end as ask_dt,
        case when employee_id is not null then dt end as answer_dt,
        employee_id,
        create_dt,
        old_supplier_id,
        mass_answer_com_id
    from cc_dwh2.sup_messages) p
left join
    (with prep as(
    select
        employeeId,
        contractTypeName,
        row_number() over (partition by employeeId order by _dt desc) as cnt
    from
        cc_dwh2.hr_employee
    )
    select * from prep where cnt = 1) h
    on h.employeeId = p.employee_id
where
    answer_dt >= toDateTime('{{ Время.start }}')
    and answer_dt < toDateTime('{{ Время.end }}')
group by
    p.employee_id,
    h.contractTypeName
order by
    final desc
)

select
    p.contractTypeName as "Тип контракта",
    count(distinct p.employee_id) as "Операторов",
    sum(p.final) as "Окончательных",
    sum(p.mass_dstnct) as "Массово в зачет",
    sum(p.mass_all_msg) as "Массово всего",
    sum(p.not_mass) as "Не массово",
    sum("Приемка") as "Приемка",
    sum(p.clarify) as "Уточнений",
    sum(p.all_clarify_and_final) as "Окончательных + Уточнений",
    sum(suppliers) as "Продавцов"
from
    prep p
group by
    contractTypeName
union all
select
    'Итого' as "Тип контракта",
    count(distinct p.employee_id) as "Операторов",
    sum(p.final) as "Окончательных",
    sum(p.mass_dstnct) as "Массово в зачет",
    sum(p.mass_all_msg) as "Массово всего",
    sum(p.not_mass) as "Не массово",
    sum("Приемка") as "Приемка",
    sum(p.clarify) as "Уточнений",
    sum(p.all_clarify_and_final) as "Окончательных + Уточнений",
    sum(suppliers) as "Продавцов"
from
    prep p

-- ==================================================

select 
    -- "Обработано из поступивших в период" + "Обработано из поступивших до периода" as "Обработано в период",
    sumIf(is_final,
        answer_dt >= '{{ Период.start }}'::datetime
        and answer_dt < '{{ Период.end }}'::datetime
        and prev_final = 0) as "Обработано в период",
    sumIf(is_final,
        answer_dt >= '{{ Период.start }}'::datetime
        and answer_dt < '{{ Период.end }}'::datetime
        and create_dt >= '{{ Период.start }}'::datetime
        and create_dt < '{{ Период.end }}'::datetime
        and prev_final = 0) as "Обработано из поступивших в период",
    sumIf(is_final,
        answer_dt >= '{{ Период.start }}'::datetime
        and answer_dt < '{{ Период.end }}'::datetime
        and create_dt < '{{ Период.start }}'::datetime
        and prev_final = 0
        ) as "Обработано из поступивших до периода",
    -- uniqExactIf(com_id,
    --     is_final = 1
    --     and answer_dt >= '{{ Период.start }}'::datetime
    --     and answer_dt < '{{ Период.end }}'::datetime
    --     and create_dt >= '{{ Период.start }}'::datetime
    --     and create_dt < '{{ Период.end }}'::datetime) as "Обработано из поступивших в период",
    -- uniqExactIf(com_id,
    --     is_final = 1
    --     and answer_dt >= '{{ Период.start }}'::datetime
    --     and answer_dt < '{{ Период.end }}'::datetime
    --     and create_dt < '{{ Период.start }}'::datetime
    --     ) as "Обработано из поступивших до периода"
    "Обработано из поступивших в период тем ПП" + "Обработано из поступивших до периода тем ПП" as "Обработано в период тем ПП",
    sumIf(is_final,
        answer_dt >= '{{ Период.start }}'::datetime
        and answer_dt < '{{ Период.end }}'::datetime
        and create_dt >= '{{ Период.start }}'::datetime
        and create_dt < '{{ Период.end }}'::datetime
        and prev_final = 0
        and parent_theme_id in (2, 4, 5, 8, 9, 10, 11, 12, 13, 14, 68, 103, 108, 115, 123, 187, 229, 251, 256, 270, 276, 280, 296, 301, 305)) as "Обработано из поступивших в период тем ПП",
    sumIf(is_final,
        answer_dt >= '{{ Период.start }}'::datetime
        and answer_dt < '{{ Период.end }}'::datetime
        and create_dt < '{{ Период.start }}'::datetime
        and prev_final = 0 
        and parent_theme_id in (2, 4, 5, 8, 9, 10, 11, 12, 13, 14, 68, 103, 108, 115, 123, 187, 229, 251, 256, 270, 276, 280, 296, 301, 305)) as "Обработано из поступивших до периода тем ПП"
    
from
    (
    select 
        id,
        lagInFrame(is_final) over( partition by com_id order by id ) prev_final, 
        lagInFrame(employee_id) over( partition by com_id order by id ) prev_empl, 
        com_id,
        is_final,
        parent_theme_id,
        parent_theme_name,
        theme_id,
        case when employee_id is null then dt end as ask_dt,
        case when employee_id is not null then dt end as answer_dt,
        employee_id,
        create_dt
    from cc_dwh2.sup_messages sd
    ) q